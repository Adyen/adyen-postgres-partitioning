/*
This function dba.find_matching_index_by_definition compares an incoming CREATE INDEX statement
with existing indexes using PostgreSQL canonical representation (pg_get_indexdef).
It detects whether a logically equivalent index already exists, regardless of formatting differences.

Problem:
We need to determine whether an index with the same logical definition already exists.
Direct SQL comparison is unreliable because PostgreSQL rewrites index definitions internally
after creation.

Why direct SQL comparison does not work:
    - PostgreSQL converts input DDL into canonical form
    - Adds explicit casts (e.g. ::text[])
    - Rewrites expressions and predicates
    - Output from pg_get_indexdef() differs from input SQL

Approach:
1. Take incoming CREATE INDEX statement
2. Create temp table in pg_temp with same structure (0 rows)
3. Replace table reference with temp table
4. Remove CONCURRENTLY (because its used for runtime-only not stored in index definitions)
5. Execute index creation on temp table
6. Fetch temp index canonical definition via pg_get_indexdef()
7. Fetch existing index canonical definition via pg_get_indexdef()
8. Normalize both: remove index name, remove table/schema, remove ONLY, normalize whitespace
9. Compare using exact equality

Returns:
    0 → index does not exist
    1 → index exists and is valid
    2 → index exists but is invalid

    PARAMETER            TYPE    DESCRIPTION
    p_schema_name        TEXT    The schema location for the table
    p_table_name         TEXT    The name of the table
    p_create_index_sql   TEXT    The CREATE INDEX statement to check

Example:
    SELECT dba.find_matching_index_by_definition('someSchema',
                                                 'someTable',
                                                 'CREATE INDEX statement....');
*/

CREATE OR REPLACE FUNCTION dba.find_matching_index_by_definition(p_schema_name TEXT, p_table_name TEXT, p_create_index_sql TEXT)
RETURNS integer
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_temp_table_name   text := format('tmp_idx_cmp_tbl_%s', pg_backend_pid());
    v_temp_index_name   text := format('tmp_idx_cmp_idx_%s', pg_backend_pid());
    v_real_table_reg    regclass;
    v_rewritten_sql     text;
    v_temp_index_oid    oid;
    v_temp_index_def    text;
    v_matching_index    text;
    v_matching_index_isinvalid boolean;
BEGIN
    v_real_table_reg := format('%I.%I', p_schema_name, p_table_name)::regclass;

    -- cleanup in case something is left from earlier in same session
    EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I CASCADE', v_temp_table_name);

    -- clone structure only, no rows
    EXECUTE format(
        'CREATE TEMP TABLE %I (LIKE %s INCLUDING ALL) ON COMMIT DROP',
        v_temp_table_name,
        v_real_table_reg
    );

    /*
      Rewrite input SQL:
      1. remove CONCURRENTLY
      2. replace original index name with temp index name
      3. replace ON <schema>.<table> (or ON <table>) with temp table
    */
    v_rewritten_sql := p_create_index_sql;

    -- remove CONCURRENTLY because temp tables doesn't allow and doesn't need as they are session bound objects
    v_rewritten_sql := regexp_replace(
        v_rewritten_sql,
        '\mCONCURRENTLY\M',
        '',
        'gi'
    );

    -- remove ONLY to handle if explicitly written
    v_rewritten_sql := regexp_replace(v_rewritten_sql, '\mON\s+ONLY\s+', 'ON ', 'gi');

    -- replace index name after CREATE [UNIQUE] INDEX [IF NOT EXISTS]
    v_rewritten_sql := regexp_replace(
        v_rewritten_sql,
        '^\s*(CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?)(".*?"|\S+)',
        '\1' || quote_ident(v_temp_index_name),
        'i'
    );

    -- replace ON real_table with ON pg_temp.temp_table
    v_rewritten_sql := regexp_replace(
        v_rewritten_sql,
        '(\mON\M\s+)(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?',
        '\1pg_temp.' || quote_ident(v_temp_table_name),
        'i'
    );

    -- create candidate index on temp table
    EXECUTE v_rewritten_sql;

    -- find temp index oid
    SELECT c.oid
      INTO v_temp_index_oid
      FROM pg_class c
     WHERE c.relnamespace = pg_my_temp_schema()
       AND c.relname = v_temp_index_name
       AND c.relkind = 'i';

    IF v_temp_index_oid IS NULL THEN
        RAISE EXCEPTION 'Temp index was not created from statement: %', v_rewritten_sql;
    END IF;

    -- canonical PostgreSQL definition for temp-created candidate
    v_temp_index_def := pg_get_indexdef(v_temp_index_oid);

    -- normalize temp definition:
    -- remove index name
    -- remove table/schema after ON
    -- collapse spaces
    v_temp_index_def := trim(
        regexp_replace(
            regexp_replace(
                v_temp_index_def,
                '(INDEX\s+)(".*?"|\S+)(\s+ON\s+)((?:"[^"]+"|\w+)\.)?(?:"[^"]+"|\w+)',
                '\1\3',
                'i'
            ),
            '\s+',
            ' ',
            'g'
        )
    );

    -- compare against existing valid indexes on real table
    SELECT x.index_name
      INTO v_matching_index
      FROM (
            SELECT
                c.relname AS index_name,
                trim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(pg_get_indexdef(i.indexrelid),
                            '\mON\s+ONLY\s+',
                            'ON ',
                            'gi'),
                            '(INDEX\s+)(".*?"|\S+)(\s+ON\s+)((?:"[^"]+"|\w+)\.)?(?:"[^"]+"|\w+)',
                            '\1\3',
                            'i'
                        ),
                        '\s+',
                        ' ',
                        'g'
                    )
                ) AS normalized_index_def
            FROM pg_index i
            JOIN pg_class c
              ON c.oid = i.indexrelid
           WHERE i.indrelid = v_real_table_reg
             AND i.indisready
             AND i.indislive
      ) x
     WHERE x.normalized_index_def = v_temp_index_def
     LIMIT 1;


     IF coalesce(v_matching_index, '') <> '' THEN

        SELECT NOT indisvalid
        INTO v_matching_index_isinvalid
        FROM pg_index
        WHERE indexrelid = format('%I.%I', p_schema_name, v_matching_index)::regclass::oid;

        IF v_matching_index_isinvalid THEN
            RETURN 2; -- if index exist and invalid
        END IF;

        RETURN 1; -- if index exist

    END IF;

    RETURN 0; -- if index not exist

EXCEPTION
    WHEN OTHERS THEN
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I CASCADE', v_temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
        RAISE;
END;
$func$;
