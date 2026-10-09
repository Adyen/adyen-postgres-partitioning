/*
Detach misaligned free (empty) partitions from an integer-range table and replace them
with up to 3 new correctly grid-aligned partitions, without touching the data partition.

The data partition (the one currently containing writes) is never detached or modified.
Only the empty partitions ahead of it are removed and replaced. This corrects misaligned
future boundaries while keeping all existing data accessible through the parent table.

A bridge partition absorbs the gap between the data partition upper bound and the first
grid boundary, using the same P/3 skip rule as partition_table_native_aligned_wrapper.
After the bridge, partition_add_up_to_nr_of_free_partitions fills up to 3 free partitions
using the correct aligned size.

Always run with v_dry_run = TRUE (default) first to inspect the plan in the server logs.

    PARAMETER                   TYPE                    DESCRIPTION
    v_schemaname                TEXT                    schema of the table
    v_tablename                 TEXT                    table to realign
    v_partition_size            BIGINT                  desired grid width going forward
    v_grid_anchor               BIGINT DEFAULT 0        a known grid-aligned boundary
    v_dry_run                   BOOLEAN DEFAULT TRUE    when TRUE, logs the plan but makes no changes
    v_detach_lock_timeout_ms    INT DEFAULT 1000        maximum time in ms to wait for a lock
    v_detach_retry_sleep_sec    INT DEFAULT 20          time in seconds between lock attempts
    v_max_retries               INT DEFAULT 10          maximum number of lock attempts

Example:
    SELECT dba.partition_realign_boundaries('public', 'orders', 10000);
    SELECT dba.partition_realign_boundaries('public', 'orders', 10000, v_dry_run => FALSE);
*/
CREATE OR REPLACE FUNCTION dba.partition_realign_boundaries(v_schemaname TEXT, v_tablename TEXT, v_partition_size BIGINT, v_grid_anchor BIGINT DEFAULT 0, v_dry_run BOOLEAN DEFAULT TRUE, v_detach_lock_timeout_ms INT DEFAULT 1000, v_detach_retry_sleep_sec INT DEFAULT 20, v_max_retries INT DEFAULT 10)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_col_name          text;
    v_col_type          text;
    v_free_count        int;
    v_free_count_locked int;
    v_data_partition    text;
    v_data_lower        bigint;
    v_data_upper        bigint;
    v_bridge_upper      bigint;
    v_bridge_name       text;
    v_aligned_end       bigint;
    v_aligned_name      text;
    v_loop_partition    text;
    i                   int;
BEGIN
    SET LOCAL client_min_messages = 'log';

    v_schemaname := lower(v_schemaname);
    v_tablename  := lower(v_tablename);

    SELECT pci.v_column_name, pci.v_column_type
    INTO v_col_name, v_col_type
    FROM dba.partition_get_partition_column_info(v_schemaname, v_tablename) AS pci;

    IF v_col_type !~ 'int' THEN
        RAISE EXCEPTION '%.% is partitioned on type %, but only integer types are supported',
            v_schemaname, v_tablename, v_col_type;
    END IF;

    IF v_partition_size <= 0 THEN
        RAISE EXCEPTION 'v_partition_size must be positive, got %', v_partition_size;
    END IF;

    SELECT v_partition_name, v_lower_bound::bigint, v_upper_bound::bigint
    INTO v_data_partition, v_data_lower, v_data_upper
    FROM dba.partition_get_current_partition_boundaries(v_schemaname, v_tablename);

    SELECT dba.partition_calculate_free_partitions(v_schemaname, v_tablename, v_col_name, v_col_type)
    INTO v_free_count;

    v_bridge_upper := dba.partition_compute_bridge_upper(v_data_upper, v_grid_anchor, v_partition_size);
    v_bridge_name  := v_tablename || '_' || v_data_upper || '_' || v_bridge_upper;
    v_aligned_end  := v_bridge_upper + v_partition_size;
    v_aligned_name := v_tablename || '_' || v_bridge_upper || '_' || v_aligned_end;

    RAISE LOG 'Plan for %.%:', v_schemaname, v_tablename;
    RAISE LOG '  data partition : % [%, % (%)) — stays in place',
        v_data_partition, v_data_lower, v_data_upper, dba.fmt_readable_number(v_data_upper);
    RAISE LOG '  free partitions to detach: %', v_free_count;
    RAISE LOG '  grid           : size=% (%), anchor=% (%)',
        v_partition_size, dba.fmt_readable_number(v_partition_size),
        v_grid_anchor,    dba.fmt_readable_number(v_grid_anchor);
    RAISE LOG '  bridge         : [% (%), % (%)) — absorbs gap to grid',
        v_data_upper,    dba.fmt_readable_number(v_data_upper),
        v_bridge_upper,  dba.fmt_readable_number(v_bridge_upper);
    RAISE LOG '  first aligned  : [% (%), % (%))',
        v_bridge_upper, dba.fmt_readable_number(v_bridge_upper),
        v_aligned_end,  dba.fmt_readable_number(v_aligned_end);
    RAISE LOG '  (additional aligned partitions filled by partition_add_up_to_nr_of_free_partitions)';

    IF v_dry_run THEN
        RAISE LOG 'dry_run=true — no changes made';
        RETURN;
    END IF;

    BEGIN
        EXECUTE format(
            $sql$ CALL dba.lock_safe_execute(%L, null, %s, %s, %s) $sql$,
            format('lock table %I.%I in access exclusive mode', v_schemaname, v_tablename),
            v_detach_lock_timeout_ms, v_detach_retry_sleep_sec, v_max_retries
        );
        EXCEPTION WHEN OTHERS THEN
            RAISE LOG 'Failed to get a lock on %.%: % (Code: %)', v_schemaname, v_tablename, SQLERRM, SQLSTATE;
            RETURN;
    END;

    SELECT dba.partition_calculate_free_partitions(v_schemaname, v_tablename, v_col_name, v_col_type)
    INTO v_free_count_locked;

    IF v_free_count_locked IS DISTINCT FROM v_free_count THEN
        RAISE EXCEPTION
            'Free partition count changed between planning (%) and lock (%): '
            'partition layout of %.% was modified concurrently. Re-run to get a fresh plan.',
            v_free_count, v_free_count_locked, v_schemaname, v_tablename;
    END IF;

    FOR i IN 1..v_free_count LOOP
        SELECT v_childrelname INTO v_loop_partition
        FROM dba.partition_get_last_partition_details(v_schemaname, v_tablename);

        RAISE LOG 'Detaching free partition %', v_loop_partition;
        PERFORM dba.partition_detach_partition(v_schemaname, v_tablename, v_loop_partition, FALSE);
    END LOOP;

    RAISE LOG 'Creating bridge partition %.%', v_schemaname, v_bridge_name;
    PERFORM dba.create_optimized_table_copy(v_schemaname, v_data_partition, v_schemaname, v_bridge_name);

    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT partition_constraint CHECK ((%I IS NOT NULL) AND (%I >= %L::bigint) AND (%I < %L::bigint))',
        v_schemaname, v_bridge_name, v_col_name, v_col_name, v_data_upper, v_col_name, v_bridge_upper);
    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
        v_schemaname, v_tablename, v_schemaname, v_bridge_name, v_data_upper, v_bridge_upper);
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT partition_constraint', v_schemaname, v_bridge_name);

    RAISE LOG 'Creating first aligned partition %.%', v_schemaname, v_aligned_name;
    PERFORM dba.create_optimized_table_copy(v_schemaname, v_bridge_name, v_schemaname, v_aligned_name);

    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT partition_constraint CHECK ((%I IS NOT NULL) AND (%I >= %L::bigint) AND (%I < %L::bigint))',
        v_schemaname, v_aligned_name, v_col_name, v_col_name, v_bridge_upper, v_col_name, v_aligned_end);
    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
        v_schemaname, v_tablename, v_schemaname, v_aligned_name, v_bridge_upper, v_aligned_end);
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT partition_constraint', v_schemaname, v_aligned_name);

    RAISE LOG 'Adding free partitions to %.%', v_schemaname, v_tablename;
    PERFORM dba.partition_add_up_to_nr_of_free_partitions(v_schemaname, v_tablename, 3);

    RAISE LOG '';
    RAISE LOG 'Table %.% partition boundaries realigned successfully', v_schemaname, v_tablename;
END
$func$;
