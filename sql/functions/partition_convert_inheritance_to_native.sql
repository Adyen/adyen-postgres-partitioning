/*
Converts a table already partitioned with inheritance to native (declarative) partitioning
on the same key column. Also creates one additional partition for the given interval.

Steps:
  1. Rename the parent table to <table>_old
  2. Create a new partitioned table based on the original
  3. Copy column options to the new table
  4. Save and recreate foreign keys
  5. Convert each child table to a partition (remove inheritance, rename, validate constraint, attach)
  6. Recreate foreign keys on the new partitioned table
  7. Create one additional partition

Note: Only integer partition key types are currently supported. Foreign keys on the original
table are NOT validated automatically — run VALIDATE CONSTRAINT manually after this function.

    PARAMETER       TYPE    DESCRIPTION
    v_schema_name   TEXT    schema of the table
    v_table_name    TEXT    table name
    v_key_column    TEXT    the partition key column
    v_interval      TEXT    interval for the additional new partition (e.g. '10000')

Example:
    SELECT dba.partition_convert_inheritance_to_native(
        v_schema_name => 'public',
        v_table_name  => 'orders',
        v_key_column  => 'orderId',
        v_interval    => '10000');
*/
CREATE OR REPLACE FUNCTION dba.partition_convert_inheritance_to_native(v_schema_name text, v_table_name text, v_key_column text, v_interval text)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_suffix         TEXT := 'old';
    v_rows           RECORD;
    v_indexes        RECORD;
    v_col_type       TEXT;
    v_min            TEXT;
    v_max            TEXT;
    v_new_start      TEXT;
    v_new_end        TEXT;
    v_new_index_name TEXT;
BEGIN

    SELECT LOWER(typname::text) AS type
    INTO v_col_type
    FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
    JOIN pg_catalog.pg_class c     ON a.attrelid = c.oid
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = LOWER(v_schema_name::name)
      AND c.relname = LOWER(v_table_name::name)
      AND a.attname = LOWER(v_key_column::name);

    IF NOT (v_col_type ~ 'int') THEN
        RAISE EXCEPTION 'Table %.% is not partitioned on an integer column type', v_schema_name, v_table_name;
    END IF;

    RAISE DEBUG 'Original table name: %, new table name: %',
        v_schema_name || '.' || v_table_name, v_schema_name || '.' || v_table_name || '_' || v_suffix;
    EXECUTE format($fmt$ALTER TABLE %I.%I RENAME TO %I$fmt$, v_schema_name, v_table_name, v_table_name || '_' || v_suffix);

    RAISE DEBUG 'Creating new partitioned table % based on %',
        v_schema_name || '.' || v_table_name, v_schema_name || '.' || v_table_name || '_' || v_suffix;
    EXECUTE format($fmt$CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) PARTITION BY RANGE (%I)$fmt$,
        v_schema_name, v_table_name, v_schema_name, v_table_name || '_' || v_suffix, v_key_column);

    RAISE DEBUG 'Copy column options from % to %',
        v_schema_name || '.' || v_table_name || '_' || v_suffix, v_schema_name || '.' || v_table_name;
    FOR v_rows IN (
        SELECT attname, unnest(attoptions) AS setting
        FROM pg_attribute a
        WHERE attrelid = format('%I.%I', lower(v_schema_name), lower(v_table_name || '_' || v_suffix))::regclass
          AND attoptions IS NOT NULL
    )
    LOOP
        EXECUTE format($fmt$ALTER TABLE ONLY %I.%I ALTER COLUMN %I SET (%s)$fmt$,
            v_schema_name, v_table_name, v_rows.attname, v_rows.setting);
    END LOOP;

    RAISE DEBUG 'Saving existing FKs referencing to child tables of %', v_schema_name || '.' || v_table_name;
    CREATE TEMP TABLE tmp_ref_fks AS
    SELECT tcc.table_schema, tcc.table_name, tcc.constraint_name,
           format('ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I.%I (%I) NOT VALID;',
                  tcc.table_schema, tcc.table_name, rc.constraint_name, v_key_column,
                  v_schema_name, v_table_name, v_key_column) AS conn_definition
    FROM information_schema.table_constraints tcp
    JOIN information_schema.referential_constraints rc ON rc.unique_constraint_name = tcp.constraint_name
    JOIN information_schema.table_constraints tcc ON tcc.constraint_name = rc.constraint_name
    WHERE format('%I.%I', tcp.table_schema, tcp.table_name)::regclass IN (
        SELECT pi.inhrelid
        FROM pg_inherits pi
        JOIN pg_class AS p ON inhparent = p.oid
        JOIN pg_namespace pn ON pn.oid = p.relnamespace
        WHERE p.relname = LOWER(v_table_name || '_' || v_suffix)
          AND pn.nspname = LOWER(v_schema_name)
    );

    RAISE DEBUG 'Saving existing FKs on child tables of %', v_schema_name || '.' || v_table_name;
    CREATE TEMP TABLE tmp_fks AS
    SELECT cn.nspname AS table_schema, c.relname AS table_name, conname AS constraint_name,
           replace(conname, c.relname, v_table_name) AS new_constraint_name,
           pg_get_constraintdef(pc.oid) AS conn_definition
    FROM pg_inherits
    JOIN pg_class AS c ON inhrelid = c.oid
    JOIN pg_class AS p ON inhparent = p.oid
    JOIN pg_namespace pn ON pn.oid = p.relnamespace AND p.relname = LOWER(v_table_name || '_' || v_suffix) AND pn.nspname = LOWER(v_schema_name)
    JOIN pg_namespace cn ON cn.oid = c.relnamespace
    JOIN pg_constraint pc ON pc.conrelid = c.oid AND pc.contype = 'f' AND pc.conparentid = 0;

    RAISE DEBUG 'Recreating existing FKs from child tables on %', v_schema_name || '.' || v_table_name;
    FOR v_rows IN (SELECT DISTINCT new_constraint_name, conn_definition FROM tmp_fks)
    LOOP
        RAISE DEBUG 'ALTER TABLE %.% ADD CONSTRAINT % %', v_schema_name, v_table_name, v_rows.new_constraint_name, v_rows.conn_definition;
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s;', v_schema_name, v_table_name, v_rows.new_constraint_name, v_rows.conn_definition);
    END LOOP;

    RAISE DEBUG 'Locking tables with FKs referencing to child tables of %', v_schema_name || '.' || v_table_name;
    FOR v_rows IN (SELECT DISTINCT table_schema, table_name FROM tmp_ref_fks)
    LOOP
        EXECUTE format($fmt$LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE$fmt$, v_rows.table_schema, v_rows.table_name);
    END LOOP;

    RAISE DEBUG 'Dropping existing FKs referencing to child tables of %', v_schema_name || '.' || v_table_name;
    FOR v_rows IN (SELECT table_schema, table_name, constraint_name FROM tmp_ref_fks)
    LOOP
        EXECUTE format($fmt$ALTER TABLE %I.%I DROP CONSTRAINT %I$fmt$, v_rows.table_schema, v_rows.table_name, v_rows.constraint_name);
    END LOOP;

    RAISE DEBUG 'Converting existing child tables to new partitions of %', v_schema_name || '.' || v_table_name;
    FOR v_rows IN (
        SELECT pc.nspname, c.relname
        FROM pg_inherits pi
        JOIN pg_class AS p  ON inhparent = p.oid
        JOIN pg_namespace pn ON pn.oid = p.relnamespace
        JOIN pg_class AS c  ON inhrelid = c.oid
        JOIN pg_namespace pc ON c.relnamespace = pc.oid
        WHERE p.relname = LOWER(v_table_name || '_' || v_suffix)
          AND pn.nspname = LOWER(v_schema_name)
    )
    LOOP
        EXECUTE format($fmt$LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE$fmt$, v_rows.nspname, v_rows.relname);

        EXECUTE format($fmt$SELECT MIN(%I), MAX(%I) + 1 FROM %I.%I$fmt$,
            v_key_column, v_key_column, v_rows.nspname, v_rows.relname)
        INTO v_min, v_max;

        EXECUTE format($fmt$ALTER TABLE %I.%I NO INHERIT %I.%I;$fmt$,
            v_rows.nspname, v_rows.relname, v_schema_name, v_table_name || '_' || v_suffix);

        EXECUTE format($fmt$ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK ((%I >= %L::bigint) AND (%I < %L::bigint)) NOT VALID;$fmt$,
            v_rows.nspname, v_rows.relname, v_table_name || '_' || v_min || '_' || v_max || '_' || v_key_column || '_check',
            v_key_column, v_min, v_key_column, v_max);

        EXECUTE format($fmt$ALTER TABLE %I.%I RENAME TO %I;$fmt$,
            v_rows.nspname, v_rows.relname, v_table_name || '_' || v_min || '_' || v_max);

        FOR v_indexes IN (
            SELECT indexname FROM pg_indexes
            WHERE schemaname = LOWER(v_rows.nspname)
              AND tablename  = LOWER(v_table_name || '_' || v_min || '_' || v_max)
        )
        LOOP
            v_new_index_name := regexp_replace(v_indexes.indexname, LOWER(v_rows.relname), LOWER(v_table_name || '_' || v_min || '_' || v_max));
            IF length(v_new_index_name) > 64 THEN
                v_new_index_name := substring(LOWER(v_new_index_name), 1, 57) || '_idx' || trunc(random() * 99 + 1);
            END IF;
            EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', LOWER(v_rows.nspname), v_indexes.indexname, v_new_index_name);
        END LOOP;

        EXECUTE format($fmt$UPDATE pg_constraint SET convalidated = true WHERE conrelid = %L::regclass AND convalidated IS false;$fmt$,
            format('%I.%I', lower(v_rows.nspname), lower(v_table_name || '_' || v_min || '_' || v_max)));

        EXECUTE format($fmt$ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L);$fmt$,
            v_schema_name, v_table_name, v_rows.nspname, v_table_name || '_' || v_min || '_' || v_max, v_min, v_max);

        EXECUTE format($fmt$ALTER TABLE %I.%I DROP CONSTRAINT %I;$fmt$,
            v_rows.nspname, v_table_name || '_' || v_min || '_' || v_max,
            v_table_name || '_' || v_min || '_' || v_max || '_' || v_key_column || '_check');
    END LOOP;

    RAISE DEBUG 'Recreating FKs which were referencing to child tables of %', v_schema_name || '.' || v_table_name;
    FOR v_rows IN (SELECT table_schema, table_name, constraint_name, conn_definition FROM tmp_ref_fks)
    LOOP
        EXECUTE format($fmt$%s;$fmt$, v_rows.conn_definition);
        EXECUTE format($fmt$UPDATE pg_constraint SET convalidated = true WHERE conrelid = %L::regclass AND conname = %L AND convalidated IS false AND contype = 'f';$fmt$,
            format('%I.%I', v_rows.table_schema, v_rows.table_name), v_rows.constraint_name);
    END LOOP;

    IF v_col_type ~ 'int' THEN
        EXECUTE format($fmt$SELECT MAX(%I) + 1 FROM %I.%I$fmt$, v_key_column, v_schema_name, v_table_name)
        INTO v_new_start;
        v_new_end := v_new_start::bigint + v_interval::bigint;
    END IF;

    EXECUTE format($fmt$CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L);$fmt$,
        v_schema_name, v_table_name || '_' || v_new_start || '_' || v_new_end,
        v_schema_name, v_table_name, v_new_start, v_new_end);

    FOR v_rows IN (
        SELECT attname, unnest(attoptions) AS setting
        FROM pg_attribute a
        WHERE attrelid = format('%I.%I', lower(v_schema_name), lower(v_table_name || '_' || v_suffix))::regclass
          AND attoptions IS NOT NULL
    )
    LOOP
        EXECUTE format($fmt$ALTER TABLE ONLY %I.%I ALTER COLUMN %I SET (%s)$fmt$,
            v_schema_name, v_table_name || '_' || v_new_start || '_' || v_new_end, v_rows.attname, v_rows.setting);
    END LOOP;

    DROP TABLE IF EXISTS tmp_ref_fks;
    DROP TABLE IF EXISTS tmp_fks;

    RETURN TRUE;

END
$func$;
