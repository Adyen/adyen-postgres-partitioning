/*
This function detaches the default partition from the parent table. After detaching, the
default partition is dropped if it is empty.

    PARAMETER                   TYPE    DESCRIPTION
    v_schema                    TEXT    schema location for the table
    v_relname_parent            TEXT    the table name of the parent table
    v_detach_lock_timeout       INT     maximum time in ms to wait for a lock (default: 1000)
    v_detach_retry_sleep        INT     seconds between lock retry attempts (default: 10)

Example:
    SELECT dba.partition_drop_default_partition('public', 'orders');
    SELECT dba.partition_drop_default_partition('public', 'orders', 2000, 30);
*/
CREATE OR REPLACE FUNCTION dba.partition_drop_default_partition(v_schema text, v_relname_parent text, v_detach_lock_timeout int DEFAULT 1000, v_detach_retry_sleep int DEFAULT 10)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_partition_name TEXT;
    row_count        INTEGER;
BEGIN

v_schema := LOWER(v_schema);
v_relname_parent := LOWER(v_relname_parent);

RAISE NOTICE 'running with pid %', pg_backend_pid();

EXECUTE FORMAT('SET local lock_timeout TO %L', v_detach_lock_timeout);

EXECUTE format($sel$
    SELECT child.relname
    FROM pg_partitioned_table pt
    JOIN pg_class parent ON pt.partrelid = parent.oid
    JOIN pg_inherits i ON pt.partrelid = i.inhparent
    JOIN pg_class child ON i.inhrelid = child.oid
    WHERE parent.relname = %L
      AND parent.relnamespace::regnamespace::text = %L
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
$sel$, v_relname_parent, v_schema)
INTO v_partition_name;

IF v_partition_name IS NULL THEN
    RAISE NOTICE 'Table % has no default partition', v_schema || '.' || v_relname_parent;
    RETURN TRUE;
END IF;

RAISE NOTICE 'Detaching partition % from %', v_partition_name, v_schema || '.' || v_relname_parent;

WHILE TRUE LOOP
    BEGIN
        EXECUTE format('SELECT count(*) FROM %I.%I LIMIT 1', v_schema, v_partition_name)
        INTO row_count;

        IF row_count = 1 THEN
            RAISE EXCEPTION 'The default partition % is not empty', v_schema || '.' || v_partition_name;
        END IF;

        EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',
                v_schema, v_relname_parent, v_schema, v_partition_name);

        EXIT;

        EXCEPTION
            WHEN lock_not_available THEN
                RAISE NOTICE 'Lock not available';
                PERFORM pg_sleep(v_detach_retry_sleep);
    END;
END LOOP;

EXECUTE format('SELECT count(*) FROM %I.%I LIMIT 1', v_schema, v_partition_name)
INTO row_count;

IF row_count = 1 THEN
    RAISE NOTICE 'The default partition % is not empty after detaching from parent table', v_schema || '.' || v_partition_name;
    RETURN FALSE;
END IF;

EXECUTE format('DROP TABLE %I.%I', v_schema, v_partition_name);

RETURN TRUE;

END
$func$;
