/*
This is a wrapper function to partition a table and immediately align its partitions with an
already-partitioned leader table. After the initial bridge partition, all subsequent partitions
land on grid-aligned boundaries shared with the leader.

The function executes the following steps:
  - Validate the leader table is partitioned on an integer column
  - Derive the grid width and anchor from the leader's last non-mammoth partition
  - Compute the bridge upper bound (snapping to the alignment grid, with the P/3 skip rule)
  - Log the full partitioning plan
  - Return early when v_dry_run = TRUE (no structural changes made)
  - Verify no data exists at or beyond v_switch_boundary (before and after acquiring the lock)
  - Lock the target table
  - Partition the table natively: mammoth covering all existing data, plus a bridge partition
    that snaps the boundary up to the nearest grid-aligned value
  - Create one grid-aligned partition to anchor subsequent maintenance to the correct grid width
  - Run regular statistics
  - Add a row to dba.partition_configuration for automatic maintenance

Partition layout after execution:
  <table>_mammoth                 : all existing data [startkey, v_switch_boundary)
  <table>_<sb>_<br>               : bridge partition  [v_switch_boundary, bridge_upper)
  <table>_<br>_<br+P>             : first aligned     [bridge_upper, bridge_upper+grid_width)
  ... (additional partitions added by partition_add_up_to_nr_of_free_partitions)

    PARAMETER                   TYPE                    DESCRIPTION
    v_schemaname                TEXT                    schema for both the target and leader table
    v_tablename                 TEXT                    the table to partition
    v_leader_tablename          TEXT                    an already integer-range partitioned table whose grid to align to
    v_switch_boundary           TEXT                    exclusive upper boundary of the mammoth partition; no row with a value >= v_switch_boundary may exist in the table
    v_dry_run                   BOOLEAN DEFAULT TRUE    when TRUE, logs the plan but makes no structural changes
    v_column_name               TEXT DEFAULT NULL       partition column on the target table; defaults to the leader's partition column
    v_move_trg                  BOOLEAN DEFAULT TRUE    set to false to skip moving triggers to the partitioned table
    v_detach_lock_timeout_ms    INT DEFAULT 1000        maximum time in ms to wait for a lock
    v_detach_retry_sleep_sec    INT DEFAULT 20          time in seconds between lock attempts
    v_max_retries               INT DEFAULT 10          maximum number of lock attempts
    v_skip_statistics           BOOLEAN DEFAULT FALSE   skip ANALYZE after partitioning (not recommended)
    p_allow_skipping_unique_indexes
                                BOOLEAN DEFAULT FALSE   allow unique indexes that do not include the partition key to be silently
                                                        skipped on the parent table. When FALSE (the default), an exception is raised
                                                        if such indexes are detected, because uniqueness will only be enforced per
                                                        individual partition, not across the entire table. Set to TRUE only if you
                                                        accept that cross-partition uniqueness is not guaranteed.
    p_copy_statistics_to_children
                                BOOLEAN DEFAULT FALSE   when TRUE, extended statistics objects and per-column statistics targets
                                                        (attstattarget) are kept on the first new partition so they propagate to
                                                        future partitions. When FALSE (default), those objects are dropped from
                                                        the new partition; the mammoth always retains its own statistics.

Example:
    -- Dry run first (default), then apply:
    SELECT dba.partition_table_native_aligned_wrapper('public', 'order_items', 'orders', '1234');
    SELECT dba.partition_table_native_aligned_wrapper('public', 'order_items', 'orders', '1234', FALSE);
    SELECT dba.partition_table_native_aligned_wrapper('public', 'order_items', 'orders', '1234', FALSE, 'orderId');

    -- Partition order_items aligned with orders, using 1234 as boundary (dry run):
    SELECT dba.partition_table_native_aligned_wrapper('public', 'order_items', 'orders', '1234');
*/

CREATE OR REPLACE FUNCTION dba.partition_table_native_aligned_wrapper(
    v_schemaname TEXT,
    v_tablename TEXT,
    v_leader_tablename TEXT,
    v_switch_boundary TEXT,
    v_dry_run BOOLEAN DEFAULT TRUE,
    v_column_name TEXT DEFAULT NULL,
    v_move_trg BOOLEAN DEFAULT TRUE,
    v_detach_lock_timeout_ms int default 1000,
    v_detach_retry_sleep_sec int default 20,
    v_max_retries int default 10,
    v_skip_statistics boolean default false,
    p_allow_skipping_unique_indexes boolean default false,
    p_copy_statistics_to_children boolean default false
)
     RETURNS VOID
     LANGUAGE plpgsql
     SET search_path = pg_catalog, dba, pg_temp
     AS $func$
     DECLARE
         v_leader_col_name           text;
         v_leader_col_type           text;
         v_target_col_name           text;
         v_target_col_type           text;
         v_grid_width                bigint;
         v_grid_anchor               bigint;
         v_leader_last_range         text[];
         v_startkey_val              bigint;
         v_bridge_upper              bigint;
         v_bridge_interval           bigint;
         v_bridge_partition_name     text;
         v_aligned_end               bigint;
         v_aligned_partition_name    text;
         v_has_violations            boolean;
     BEGIN
         SET LOCAL client_min_messages = 'log';

         -- Normalize the identifiers so names with uppercase letters are matched
         -- case-insensitively, used consistently in the generated DDL, and stored
         -- lowercase in dba.partition_configuration.
         v_schemaname := LOWER(v_schemaname);
         v_tablename := LOWER(v_tablename);
         v_leader_tablename := LOWER(v_leader_tablename);

         -- 1. Validate the leader table is partitioned on an integer column
         SELECT pci.v_column_name, pci.v_column_type
         INTO v_leader_col_name, v_leader_col_type
         FROM dba.partition_get_partition_column_info(v_schemaname, v_leader_tablename) AS pci;

         IF v_leader_col_type IS NULL THEN
             RAISE EXCEPTION '%.% is not a partitioned table or does not exist', v_schemaname, v_leader_tablename;
         END IF;

         IF v_leader_col_type !~ 'int' THEN
             RAISE EXCEPTION '%.% is partitioned on type %, but only integer types are supported as leader',
                 v_schemaname, v_leader_tablename, v_leader_col_type;
         END IF;

         -- 2. Determine the partition column for the target table
         IF v_column_name IS NULL THEN
             v_target_col_name := v_leader_col_name;
         ELSE
             v_target_col_name := LOWER(v_column_name);
         END IF;

         -- 3. Validate the target column exists and is an integer type
         SELECT LOWER(t.typname)
         INTO v_target_col_type
         FROM pg_catalog.pg_type t
         JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
         JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
         JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
         WHERE n.nspname = LOWER(v_schemaname)
           AND c.relname = LOWER(v_tablename)
           AND a.attname = v_target_col_name
           AND a.attnum > 0;

         IF v_target_col_type IS NULL THEN
             RAISE EXCEPTION 'Column % does not exist on %.%', v_target_col_name, v_schemaname, v_tablename;
         END IF;

         IF v_target_col_type !~ 'int' THEN
             RAISE EXCEPTION 'Column % on %.% has type %, but only integer types are supported',
                 v_target_col_name, v_schemaname, v_tablename, v_target_col_type;
         END IF;

         -- 4. Derive grid width and anchor from the leader's last non-mammoth partition
         SELECT v_range
         INTO v_leader_last_range
         FROM dba.partition_get_last_partition_details(v_schemaname, v_leader_tablename);

         IF v_leader_last_range IS NULL THEN
             RAISE EXCEPTION '%.% has no non-mammoth partitions; cannot derive grid info',
                 v_schemaname, v_leader_tablename;
         END IF;

         v_grid_width  := v_leader_last_range[2]::bigint - v_leader_last_range[1]::bigint;
         v_grid_anchor := v_leader_last_range[2]::bigint;

         RAISE LOG 'Leader grid: width=% (%), anchor=% (%)',
             v_grid_width,  dba.fmt_readable_number(v_grid_width),
             v_grid_anchor, dba.fmt_readable_number(v_grid_anchor);

         -- 5. Determine startkey from the table's minimum value; default to 0 for empty tables
         EXECUTE format('SELECT min(%I) FROM %I.%I', v_target_col_name, v_schemaname, v_tablename)
         INTO v_startkey_val;

         IF v_startkey_val IS NULL THEN
             v_startkey_val := 0;
         END IF;

         -- 6. Compute bridge partition bounds
         v_bridge_upper    := dba.partition_compute_bridge_upper(v_switch_boundary::bigint, v_grid_anchor, v_grid_width);
         v_bridge_interval := v_bridge_upper - v_switch_boundary::bigint;

         RAISE LOG 'Plan for %.%:', v_schemaname, v_tablename;
         RAISE LOG '  mammoth  : [MINVALUE, % (%)) ',
             v_switch_boundary, dba.fmt_readable_number(v_switch_boundary::bigint);
         RAISE LOG '  bridge   : [% (%), % (%)) ',
             v_switch_boundary, dba.fmt_readable_number(v_switch_boundary::bigint),
             v_bridge_upper,    dba.fmt_readable_number(v_bridge_upper);
         RAISE LOG '  aligned  : [% (%), % (%)) + up to 3 more via partition_add_up_to_nr_of_free_partitions',
             v_bridge_upper,                dba.fmt_readable_number(v_bridge_upper),
             v_bridge_upper + v_grid_width, dba.fmt_readable_number(v_bridge_upper + v_grid_width);
         RAISE LOG '  grid_width=% (%), grid_anchor=% (%)',
             v_grid_width,  dba.fmt_readable_number(v_grid_width),
             v_grid_anchor, dba.fmt_readable_number(v_grid_anchor);

         IF v_dry_run THEN
             RAISE LOG 'dry_run=true — no changes made';
             RETURN;
         END IF;

         -- 7. Pre-lock boundary check: ensure no data exists at or beyond v_switch_boundary
         EXECUTE format('SELECT count(1) > 0 FROM %I.%I WHERE %I >= %L::%s',
                        v_schemaname, v_tablename, v_target_col_name, v_switch_boundary, v_target_col_type)
         INTO v_has_violations;

         IF v_has_violations THEN
             RAISE EXCEPTION 'Table %.% contains data at or beyond the requested switch_boundary %',
                 v_schemaname, v_tablename, v_switch_boundary;
         END IF;

         -- 8. Lock the table
         BEGIN
             EXECUTE format($sql$ CALL dba.lock_safe_execute(%L, null, %s, %s, %s) $sql$,
                            format('lock table %I.%I in access exclusive mode', v_schemaname, v_tablename),
                            v_detach_lock_timeout_ms, v_detach_retry_sleep_sec, v_max_retries);
             EXCEPTION
                 WHEN OTHERS THEN
                     RAISE LOG 'Failed to get a lock on %.%: % (Code: %)', v_schemaname, v_tablename, SQLERRM, SQLSTATE;
                     RETURN;
         END;

         -- 9. Post-lock boundary check (authoritative, under the lock)
         EXECUTE format('SELECT count(1) > 0 FROM %I.%I WHERE %I >= %L::%s',
                        v_schemaname, v_tablename, v_target_col_name, v_switch_boundary, v_target_col_type)
         INTO v_has_violations;

         IF v_has_violations THEN
             RAISE EXCEPTION 'Table %.% contains data at or beyond the requested switch_boundary % (verified under lock)',
                 v_schemaname, v_tablename, v_switch_boundary;
         END IF;

         -- 10. Partition the table: creates mammoth [startkey, v_switch_boundary) and bridge [v_switch_boundary, bridge_upper)
         --     partition_native adds +1 to its endkey argument internally for integer types, so we pass
         --     (v_switch_boundary - 1) to make the exclusive upper of the mammoth land exactly at v_switch_boundary.
         RAISE LOG 'Partitioning %.%: mammoth up to %, bridge to %',
             v_schemaname, v_tablename, v_switch_boundary, v_bridge_upper;
         PERFORM dba.partition_native(v_schemaname, v_tablename, v_target_col_name,
                                      v_startkey_val::text, (v_switch_boundary::bigint - 1)::text, v_bridge_interval::text, FALSE, v_move_trg, p_allow_skipping_unique_indexes, p_copy_statistics_to_children);

         -- 11. Create and attach the first grid-aligned partition
         --     Naming follows the same convention as partition_native: <table>_<lower>_<upper>
         v_bridge_partition_name  := v_tablename || '_' || v_switch_boundary::text || '_' || v_bridge_upper::text;
         v_aligned_end            := v_bridge_upper + v_grid_width;
         v_aligned_partition_name := v_tablename || '_' || v_bridge_upper::text || '_' || v_aligned_end::text;

         RAISE LOG 'Creating first aligned partition %.%', v_schemaname, v_aligned_partition_name;

         PERFORM dba.create_optimized_table_copy(v_schemaname, v_bridge_partition_name, v_schemaname, v_aligned_partition_name);

         EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%s) TO (%s)',
                        v_schemaname, v_tablename, v_schemaname, v_aligned_partition_name,
                        v_bridge_upper, v_aligned_end);

         -- 12. Ensure 4 free partitions exist: the bridge counts as 1 free partition (its lower bound
         --     exceeds the current max), so passing 4 results in bridge + 3 aligned empty partitions.
         --     partition_add_up_to_nr_of_free_partitions derives partition size from the last partition,
         --     which is now the aligned partition of grid_width.
         RAISE LOG 'Adding free partitions to %.%', v_schemaname, v_tablename;
         PERFORM dba.partition_add_up_to_nr_of_free_partitions(v_schemaname, v_tablename, 4);

         -- 13. Gather statistics
         IF v_skip_statistics THEN
             RAISE LOG '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
             RAISE LOG '!!!';
             RAISE LOG '!!! Skipping statistics for table %.%. This reduces the lock time, but increases performance risks', v_schemaname, v_tablename;
             RAISE LOG '!!! Please run the command "ANALYZE (VERBOSE) %.%" as quickly as possible', v_schemaname, v_tablename;
             RAISE LOG '!!!';
             RAISE LOG '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
         ELSE
             RAISE LOG 'Collecting statistics for table %.%', v_schemaname, v_tablename;
             EXECUTE format('ANALYZE (VERBOSE) %I.%I', v_schemaname, v_tablename);
         END IF;

         -- 14. Register for automatic partition maintenance
         INSERT INTO dba.partition_configuration VALUES (v_schemaname, v_tablename, '{"auto-maintenance": true}');

         RAISE LOG '';
         RAISE LOG 'Table %.% successfully partitioned and aligned with leader %.%',
             v_schemaname, v_tablename, v_schemaname, v_leader_tablename;
         RAISE LOG '';
         RAISE LOG 'A line has been added to table dba.partition_configuration';
         RAISE LOG 'The partitioning framework will keep up to 3 empty partitions available at all times';
         RAISE LOG 'If you need any additional configuration, please update the configuration table manually';
     END
$func$;
