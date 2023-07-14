/*
This function generates a set of statements to create concurrent indexes on all partitions of a partitioned tables and a
create index statement for the parent table. In case the table is partitioned on a column which is not part of the
primary key, also a create index statement for the <table>_template is returned as wel. It does not execute these
statements.

The function tries to find a unique index name in the form of <table_name>_<columns>[1-9]_idx. If no unique name
can be found, the name with a nine in it will we returned. Executing this statement will fail with a duplicate index error.

When a unique index has to be created, we only add this index on the  parent table when the partition column is included
in the index. Otherwise we would get an error. 

    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the parent table name
    v_columns       TEXT[]  the columns to create the index on including the names the operator class parameters such as desc, nulls first, nulls distinct
    v_method        TEXT    the name of the index method, default btree. Possible other values: hash, gist, spgist, gin, brin
    v_is_unique     BOOLEAN default false. Indicate the index has to be unique

Example:
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition', ARRAY['column_1', 'column_2']);
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition', ARRAY['lower(column_1)', 'column_2 desc nulls first'], 'gin');
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition', ARRAY['lower(column_1)', 'column_2 desc nulls first'], 'gin', true);

Returns:
    A table containing the following statements in order
     - A create index concurrently statement for every child table
     - A create index statement for the parent table
     - A create index statement for the parent_template table when this table exists
*/

CREATE OR REPLACE FUNCTION dba.partition_add_concurrent_index_on_partitioned_table(v_schema TEXT, v_table TEXT, v_columns TEXT[], v_method TEXT default 'btree', v_is_unique boolean DEFAULT FALSE)
RETURNS table(stmt text) LANGUAGE plpgsql AS $func$

DECLARE
    v_indexname            TEXT;
    v_row                  RECORD;
    v_row_count            INT;
    v_column_names         TEXT[];
    v_total_indexes        INT;
    v_column_name          TEXT;

BEGIN
    v_schema:=LOWER(v_schema);
    v_table:=LOWER(v_table);

    -- When creating a unique index we need to know the partition column
    IF (v_is_unique) THEN
        EXECUTE format($sel$
            SELECT
                LOWER(col.column_name)
            FROM
                (SELECT
                    partrelid,
                    unnest(partattrs) column_index
                 FROM
                     pg_partitioned_table) pt
            JOIN pg_class c on c.oid = pt.partrelid
            JOIN information_schema.columns col ON
                col.table_schema = c.relnamespace::regnamespace::text
                AND col.table_name = c.relname
                AND ordinal_position = pt.column_index
            WHERE
                c.relname = '%I'
                and relnamespace::regnamespace::text='%I'
        $sel$, v_table, v_schema)
        INTO v_column_name;
    END IF;

    -- separate the column names from the rest of the arguments like functions and operators like 'desc', 'nulls first', etc
    SELECT ARRAY (SELECT regexp_replace(split_part(UNNEST(v_columns), ' ', 1), '.*\((.*)\)', '\1'))
    INTO v_column_names;

    IF (SELECT LOWER(v_method) NOT IN ('btree', 'hash', 'gist', 'spgist', 'gin', 'brin') ) THEN
        RAISE EXCEPTION 'Index method % is not supported', v_method;
    END IF;

    -- Create a temporary table to store the results.
    CREATE TEMP TABLE IF NOT EXISTS temp_partition_concurrent_indexes (order_number int, table_name text, index_name text)
    ON COMMIT DELETE ROWS;

    -- List all the child partitions
    EXECUTE FORMAT ($sql$
        INSERT INTO temp_partition_concurrent_indexes (
            SELECT
                1 AS order_number,
                child.relname as table_name,
                substring(LOWER(child.relname) || '_%I' , 1, 59) || '_idx' AS index_name
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
            WHERE
                LOWER(parent.relnamespace::regnamespace::text) = LOWER('%I')
                AND LOWER(parent.relname)=LOWER('%I'))
        $sql$,
        LOWER(array_to_string(v_column_names, '_')), v_schema, v_table);

    -- Add the parent table if a non-unique index OR it is a unique index, but the index contains the partition column
    IF ( ( NOT v_is_unique) OR ( v_is_unique AND v_column_name=ANY(lower(v_columns::text)::text[])     ) )THEN
        EXECUTE FORMAT ($sql$
            INSERT INTO temp_partition_concurrent_indexes values (2, '%I', substring(LOWER('%I') || '_%I', 1, 59) || '_idx')
            $sql$, v_table, v_table, LOWER(array_to_string(v_column_names, '_')));
    END IF;

    -- Check if _template table exists
    perform 1
    FROM pg_class c
    WHERE LOWER(c.relname) = LOWER(v_table || '_template') AND LOWER(c.relnamespace::regnamespace::text) = LOWER(v_schema);

    IF FOUND THEN
         EXECUTE FORMAT ($sql$
              INSERT INTO temp_partition_concurrent_indexes values (3, '%I_template', substring(LOWER('%I_template') || '_%I', 1, 59) || '_idx')
              $sql$, v_table, v_table, LOWER(array_to_string(v_column_names, '_')));
    END IF;

    FOR v_row IN SELECT * FROM temp_partition_concurrent_indexes LOOP
        EXECUTE FORMAT ($sql$
            SELECT '1' FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid
            AND relname = '%I' AND nspname = '%I' AND lower(relkind) = 'i'
        $sql$, v_row.index_name, v_schema) ;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;

        IF v_row_count = 0 THEN
            -- Index name is unique. We are done.
            continue;
        ELSE
            -- An index with this name already exists. Remove the suffix and add a number at the end.
            -- After number 9 we give up and executing the create index statement will fail.
            FOR counter in 1..9 LOOP
                IF LENGTH(v_row.index_name) = 64 THEN
                    -- We should not cross the 64 characters when adding a number. Remove the suffix and one character.
                    v_indexname := left(v_row.index_name , -5) || counter || '_idx';
                ELSE
                    v_indexname := left(v_row.index_name , -4) || counter || '_idx';
                END IF;

                RAISE DEBUG 'Testing index name % for uniqueness', v_indexname;

                EXECUTE FORMAT ($sql$
                    SELECT '1' FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid
                    AND relname = '%I' AND nspname = '%I' AND lower(relkind) = 'i'
                $sql$, v_indexname, v_schema) ;

                GET DIAGNOSTICS v_row_count = ROW_COUNT;

                IF v_row_count = 0 THEN
                    -- We have found a unique index name. Update the temp table with this name.
                    EXECUTE FORMAT($sql$ UPDATE temp_partition_concurrent_indexes SET index_name = '%I' WHERE table_name = '%I'
                    $sql$, v_indexname, v_row.table_name);

                    exit;
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    SELECT COUNT(*) FROM temp_partition_concurrent_indexes
    INTO v_total_indexes;

    RETURN QUERY
    EXECUTE format($sql$
        SELECT '/* Creating index ' || row_number() over (order by order_number, index_name) || ' of %s */ create ' ||
            CASE WHEN '%I'::boolean THEN 'unique ' ELSE '' END ||
            'index ' ||
            CASE WHEN order_number = 1 THEN 'concurrently ' ELSE '' END ||
            index_name || ' on %I.' ||
            table_name || ' using %I (%s);'
        FROM temp_partition_concurrent_indexes
        ORDER BY order_number, index_name
    $sql$, v_total_indexes, v_is_unique, v_schema, v_method, LOWER(array_to_string(v_columns, ', ')));
END
$func$;
