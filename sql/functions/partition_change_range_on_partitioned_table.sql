/*
When a table is partitioned based on an integer or date/timestamp column this function can be
used to change partition ranges on the empty (future) partitions.

Steps:
  - Detach all empty partitions from the partitioned table
  - Attach the same number of partitions with the new range (v_new_interval)
  - Optionally drop the detached empty partitions

    PARAMETER                                   TYPE        DESCRIPTION
    v_schema_name                               TEXT        schema of the table
    v_table_name                                TEXT        the parent table name
    v_new_interval                              TEXT        the new partition interval
    v_number_of_additional_partitions_to_create INT         number of new partitions to create
    v_change_active_partition                   BOOLEAN     include active partition (default: FALSE)
    v_drop_detached_partitions                  BOOLEAN     drop the detached empty partitions (default: FALSE)

Example:
    SELECT dba.partition_change_range_on_partitioned_table(
        v_schema_name => 'public',
        v_table_name  => 'orders',
        v_new_interval => '10000',
        v_number_of_additional_partitions_to_create => 5,
        v_drop_detached_partitions => true);
*/
CREATE OR REPLACE FUNCTION dba.partition_change_range_on_partitioned_table(v_schema_name text, v_table_name text, v_new_interval text, v_number_of_additional_partitions_to_create INT, v_change_active_partition BOOLEAN DEFAULT FALSE, v_drop_detached_partitions BOOLEAN DEFAULT FALSE)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $function$
DECLARE
    v_empty_partitions            RECORD;
    v_partition_column_name       TEXT;
    v_col_type                    TEXT;
    v_last_range                  TEXT ARRAY;
    v_new_start                   TEXT;
    v_new_end                     TEXT;
    v_partition_suffix            TEXT;
    v_new_partition_name          TEXT;
    v_active_partition_name       TEXT;
    v_attach_lock_timeout         CONSTANT INT := 1000;
    v_attach_retries              CONSTANT INT := 3;
    v_attach_retry_sleep          CONSTANT INT := 10;
    v_loop_cnt                    INT;
    v_table_owner                 TEXT;
BEGIN

    -- Normalize the identifiers so names with uppercase letters are matched case-insensitively.
    v_schema_name := LOWER(v_schema_name);
    v_table_name  := LOWER(v_table_name);

    EXECUTE FORMAT('SET LOCAL lock_timeout TO %L', v_attach_lock_timeout);

    SELECT pci.v_column_name, pci.v_column_type
    INTO v_partition_column_name, v_col_type
    FROM dba.partition_get_partition_column_info(v_schema_name, v_table_name) AS pci;

    IF NOT (v_col_type ~ 'int' OR v_col_type ~ 'date' OR v_col_type ~ 'timestamp') OR v_col_type IS NULL THEN
        RAISE EXCEPTION 'Table %.% is not partitioned on an integer column type', v_schema_name, v_table_name;
    END IF;

    CREATE TEMP TABLE tmp_empty_partitions AS
    SELECT v_schema_name AS schema_name, child.relname AS partition_name
    FROM pg_inherits
    JOIN pg_class parent         ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child          ON pg_inherits.inhrelid  = child.oid
    JOIN pg_namespace nmsp_child ON nmsp_child.oid        = child.relnamespace
    WHERE LOWER(nmsp_child.nspname) = v_schema_name
      AND LOWER(parent.relname)     = v_table_name
      AND child.reltuples = 0;

    FOR v_empty_partitions IN (SELECT partition_name FROM tmp_empty_partitions)
    LOOP
        RAISE DEBUG 'Detaching empty partition: %', v_empty_partitions.partition_name;
        PERFORM dba.partition_detach_partition(
            v_schema      => v_schema_name,
            v_relname_parent => v_table_name,
            v_partition_name => v_empty_partitions.partition_name,
            v_detach_last    => FALSE);
    END LOOP;

    -- The last remaining partition is now the active one; the new partitions start at its upper bound.
    SELECT v_childrelname, v_range
    INTO v_active_partition_name, v_last_range
    FROM dba.partition_get_last_partition_details(
        v_schema  => v_schema_name,
        v_relname => v_table_name);

    CASE
        WHEN v_col_type ~ 'int' THEN
            SELECT v_last_range[2] INTO v_new_start;
            SELECT v_last_range[2]::bigint + v_new_interval::bigint INTO v_new_end;
        WHEN v_col_type ~ 'date' THEN
            SELECT v_last_range[2] INTO v_new_start;
            SELECT v_last_range[2]::date + v_new_interval::interval INTO v_new_end;
        WHEN v_col_type ~ 'timestamp' THEN
            SELECT v_last_range[2] INTO v_new_start;
            SELECT v_last_range[2]::timestamp + v_new_interval::interval INTO v_new_end;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_col_type;
    END CASE;

    RAISE DEBUG 'New lower bound: %, new upper bound: %', v_new_start, v_new_end;

    v_partition_suffix   := replace(regexp_replace(v_new_start::TEXT, '\ .*', ''), '-', '') || '_'
                         || replace(regexp_replace(v_new_end::TEXT, '\ .*', ''), '-', '');
    v_new_partition_name := v_table_name || '_' || v_partition_suffix;

    RAISE DEBUG 'New partition: %, boundaries % and %', v_new_partition_name, v_new_start, v_new_end;

    -- The active partition is the template: the copy gets its optimized column order, indexes, foreign keys,
    -- check constraints, storage parameters, statistics, triggers and owner.
    PERFORM dba.create_optimized_table_copy(
        v_schema_name, v_active_partition_name, v_schema_name, v_new_partition_name);

    -- When the constraint on the to be attached partition doesn't overlap with the constraint on a possible
    -- default partition we don't require an ACCESS EXCLUSIVE lock on the table.
    EXECUTE format(
        'ALTER TABLE %I.%I ADD CONSTRAINT partition_constraint CHECK ((%I IS NOT NULL) AND (%I >= %L::%I) AND (%I < %L::%I))',
        v_schema_name, v_new_partition_name,
        v_partition_column_name,
        v_partition_column_name, v_new_start, v_col_type,
        v_partition_column_name, v_new_end,   v_col_type);

    FOR v_loop_cnt IN 1..v_attach_retries LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
                v_schema_name, v_table_name, v_schema_name, v_new_partition_name, v_new_start, v_new_end);

            SELECT tableowner FROM pg_tables
            WHERE schemaname = v_schema_name AND tablename = v_table_name
            INTO v_table_owner;

            EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', v_schema_name, v_new_partition_name, v_table_owner);

            EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT partition_constraint',
                v_schema_name, v_new_partition_name);

            EXIT;

            EXCEPTION WHEN lock_not_available THEN
                RAISE NOTICE 'Lock not available %', v_loop_cnt;
                IF v_loop_cnt = v_attach_retries THEN
                    RAISE NOTICE 'Attaching table failed';
                    EXECUTE format('DROP TABLE %I.%I', v_schema_name, v_new_partition_name);
                    RETURN FALSE;
                END IF;
                PERFORM pg_sleep(v_attach_retry_sleep);
        END;
    END LOOP;

    PERFORM dba.partition_add_up_to_nr_of_free_partitions(
        v_schema  => v_schema_name,
        v_relname => v_table_name,
        v_number_of_additional_partitions => v_number_of_additional_partitions_to_create);

    IF v_drop_detached_partitions THEN
        FOR v_empty_partitions IN (SELECT partition_name FROM tmp_empty_partitions)
        LOOP
            RAISE DEBUG 'Dropping empty partition: %', v_empty_partitions.partition_name;
            EXECUTE format('DROP TABLE %I.%I', v_schema_name, v_empty_partitions.partition_name);
        END LOOP;
    END IF;

    DROP TABLE IF EXISTS tmp_empty_partitions;

    RETURN TRUE;

END
$function$;
