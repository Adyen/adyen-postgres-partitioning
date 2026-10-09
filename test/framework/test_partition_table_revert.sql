/*
Test: test_partition_table_revert (and partition_table_revert_cleanup)
Functions under test: dba.partition_table_revert, dba.partition_table_revert_cleanup
Run: ./test/framework/run_partition_tests.sh test_partition_table_revert
Purpose: Verify the revert procedure restores a partitioned table to its original state,
         and that cleanup drops leftover tables and metadata.
Test coverage:
  - dry_run (default) makes no structural changes
  - apply: table is restored as a regular table; mammoth data preserved
  - apply: non-mammoth children are detached (not dropped), config row stays (for cleanup)
  - cleanup dry_run: no drops
  - cleanup apply: retired shell and detached partitions dropped; metadata removed
  - negative: revert raises when a non-mammoth partition has rows
  - negative: revert raises on a non-partitioned table
*/

CREATE OR REPLACE PROCEDURE dba_test.partition_table_revert_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count          int;
    v_is_partitioned boolean;
    v_exists         boolean;
    -- Setup: ref_table with grid [0,1000),[1000,2000) → P=1000
    -- switch_boundary=1500 → standard bridge [1500,2000), 3 aligned
    v_switch         text := '1500';
BEGIN
    -- ----------------------------------------------------------------
    -- Setup: reference alignment table + target table
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ref_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_target_partitioned_retired CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'revert_target';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test' AND parent_relname = 'revert_target';

    CREATE TABLE dba_test.ref_table (ref_id bigint NOT NULL) PARTITION BY RANGE (ref_id);
    CREATE TABLE dba_test.ref_table_0_1000    PARTITION OF dba_test.ref_table FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.ref_table_1000_2000 PARTITION OF dba_test.ref_table FOR VALUES FROM (1000) TO (2000);

    CREATE TABLE dba_test.revert_target (
        col_id bigint NOT NULL,
        data   text,
        PRIMARY KEY (col_id)
    );
    INSERT INTO dba_test.revert_target VALUES (500, 'seed row');

    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.revert_trigger_fn() CASCADE';
    EXECUTE $sql$
        CREATE FUNCTION dba_test.revert_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS
        $t$ BEGIN RETURN NEW; END; $t$
    $sql$;
    EXECUTE $sql$
        CREATE TRIGGER revert_test_trg BEFORE INSERT ON dba_test.revert_target
            FOR EACH ROW EXECUTE FUNCTION dba_test.revert_trigger_fn()
    $sql$;

    COMMIT;

    -- ----------------------------------------------------------------
    -- Step 1: Partition the target using the generic wrapper
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'revert_target', 'ref_table', v_switch,
        FALSE,
        'col_id'
    );
    COMMIT;

    -- Sanity check: must be partitioned before revert
    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'revert_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_setup_is_partitioned',
        CASE WHEN v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- ----------------------------------------------------------------
    -- Test: dry_run (default) makes no changes
    -- ----------------------------------------------------------------
    CALL dba.partition_table_revert('dba_test', 'revert_target', TRUE);

    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'revert_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_dry_run_still_partitioned',
        CASE WHEN v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- ----------------------------------------------------------------
    -- Test: apply (dry_run=false)
    -- ----------------------------------------------------------------
    CALL dba.partition_table_revert('dba_test', 'revert_target', FALSE);
    -- COMMIT happens inside partition_table_revert

    -- revert_target must now be a regular (non-partitioned) table
    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'revert_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_apply_not_partitioned',
        CASE WHEN NOT v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Seed row must still be accessible
    SELECT count(*) INTO v_count
    FROM dba_test.revert_target WHERE col_id = 500;

    PERFORM dba_test.record_result('revert_apply_data_preserved',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Retired shell must exist (not yet dropped — that's cleanup's job)
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = 'dba_test' AND lower(c.relname) = 'revert_target_partitioned_retired'
    ) INTO v_exists;

    PERFORM dba_test.record_result('revert_apply_retired_shell_exists',
        CASE WHEN v_exists THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- partition_configuration row must still exist (cleanup removes it)
    SELECT count(*) INTO v_count
    FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'revert_target';

    PERFORM dba_test.record_result('revert_apply_config_still_present',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- detached_partitions must have entries for the detached children (bridge + 3 aligned)
    SELECT count(*) INTO v_count
    FROM dba.detached_partitions
    WHERE schema = 'dba_test' AND parent_relname = 'revert_target';

    PERFORM dba_test.record_result('revert_apply_detached_partitions_recorded',
        CASE WHEN v_count = 4 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 4 (bridge + 3 aligned), got %s', v_count));

    -- No index on the restored table should contain '_mammoth' in its name
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test'
      AND tablename  = 'revert_target'
      AND lower(indexname) ~ 'mammoth';

    PERFORM dba_test.record_result('revert_apply_no_mammoth_indexes',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 0 indexes with _mammoth, got %s', v_count));

    -- No constraint on the restored table should contain '_mammoth' in its name
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conrelid = 'dba_test.revert_target'::regclass
      AND lower(conname) ~ 'mammoth';

    PERFORM dba_test.record_result('revert_apply_no_mammoth_constraints',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 0 constraints with _mammoth, got %s', v_count));

    -- Trigger must exist on the restored standalone table
    SELECT count(*) INTO v_count
    FROM pg_trigger
    WHERE tgrelid = 'dba_test.revert_target'::regclass
      AND tgname = 'revert_test_trg';

    PERFORM dba_test.record_result('revert_apply_trigger_restored_on_table',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 1 trigger revert_test_trg on revert_target, got %s', v_count));

    -- Trigger must NOT remain on the retired shell
    SELECT count(*) INTO v_count
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = 'dba_test'
      AND lower(c.relname) = 'revert_target_partitioned_retired'
      AND t.tgname = 'revert_test_trg';

    PERFORM dba_test.record_result('revert_apply_trigger_not_on_retired_shell',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 0 triggers revert_test_trg on retired shell, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: cleanup dry_run makes no drops
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_revert_cleanup('dba_test', 'revert_target', TRUE);

    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = 'dba_test' AND lower(c.relname) = 'revert_target_partitioned_retired'
    ) INTO v_exists;

    PERFORM dba_test.record_result('cleanup_dry_run_retired_still_exists',
        CASE WHEN v_exists THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- ----------------------------------------------------------------
    -- Test: cleanup apply — drops shell + detached children + removes metadata
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_revert_cleanup('dba_test', 'revert_target', FALSE);

    -- Retired shell must be gone
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = 'dba_test' AND lower(c.relname) = 'revert_target_partitioned_retired'
    ) INTO v_exists;

    PERFORM dba_test.record_result('cleanup_apply_retired_shell_gone',
        CASE WHEN NOT v_exists THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- partition_configuration row must be removed
    SELECT count(*) INTO v_count
    FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'revert_target';

    PERFORM dba_test.record_result('cleanup_apply_config_removed',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- detached_partitions entries must be removed
    SELECT count(*) INTO v_count
    FROM dba.detached_partitions
    WHERE schema = 'dba_test' AND parent_relname = 'revert_target';

    PERFORM dba_test.record_result('cleanup_apply_detached_partitions_cleared',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Live table must still exist with data intact
    SELECT count(*) INTO v_count FROM dba_test.revert_target WHERE col_id = 500;
    PERFORM dba_test.record_result('cleanup_apply_live_table_intact',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- ----------------------------------------------------------------
    -- Test: round-trip — partition again after revert must succeed without
    --       "already exists" errors for indexes or constraints
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'revert_target', 'ref_table', v_switch,
        FALSE,
        'col_id'
    );
    COMMIT;

    -- Must be partitioned again
    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'revert_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('round_trip_is_partitioned',
        CASE WHEN v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Seed row must still be accessible after the second partitioning
    SELECT count(*) INTO v_count FROM dba_test.revert_target WHERE col_id = 500;
    PERFORM dba_test.record_result('round_trip_data_preserved',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Revert again to prove a second revert cycle also works cleanly
    CALL dba.partition_table_revert('dba_test', 'revert_target', FALSE);
    PERFORM dba.partition_table_revert_cleanup('dba_test', 'revert_target', FALSE);

    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'revert_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('round_trip_second_revert_not_partitioned',
        CASE WHEN NOT v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- No index on the restored table should contain '_mammoth' after the second revert
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test'
      AND tablename  = 'revert_target'
      AND lower(indexname) ~ 'mammoth';

    PERFORM dba_test.record_result('round_trip_no_mammoth_indexes',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 0 indexes with _mammoth, got %s', v_count));

    -- No constraint on the restored table should contain '_mammoth' after the second revert
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conrelid = 'dba_test.revert_target'::regclass
      AND lower(conname) ~ 'mammoth';

    PERFORM dba_test.record_result('round_trip_no_mammoth_constraints',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 0 constraints with _mammoth, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: G1 index name length preservation through partition and revert
    --
    -- The original table has an index whose name is 58 chars:
    --   revert_trunc_col_name_for_idx_length_truncation_test_a_idx
    -- During the mammoth rename, the substituted name would be 66 chars (> 63),
    -- so it is truncated to exactly 63 chars (preserving as much column info as
    -- possible).  When the parent-shell index is derived from that mammoth name
    -- (by replacing the mammoth suffix back to the base table name), the result
    -- is 55 chars.  Appending '_retired' during revert gives exactly 63 chars.
    -- ----------------------------------------------------------------
    BEGIN
        EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_trunc CASCADE';
        EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_trunc_partitioned_retired CASCADE';
        DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'revert_trunc';
        DELETE FROM dba.detached_partitions       WHERE schema = 'dba_test'   AND parent_relname = 'revert_trunc';

        CREATE TABLE dba_test.revert_trunc (
            col_id                                    bigint NOT NULL,
            col_name_for_idx_length_truncation_test_a text,
            PRIMARY KEY (col_id)
        );
        -- Original index name: 'revert_trunc_col_name_for_idx_length_truncation_test_a_idx' (58 chars)
        -- After mammoth rename (66 chars > 63): truncated to 63 chars on mammoth
        -- After parent copy (replace tablename_mammoth -> tablename): 55 chars on parent shell
        -- After revert (55 + 8 = 63): exactly 63-char index ending with '_retired'
        CREATE INDEX ON dba_test.revert_trunc (col_name_for_idx_length_truncation_test_a);

        INSERT INTO dba_test.revert_trunc VALUES (500, 'seed row');
        COMMIT;

        PERFORM dba.partition_table_native_aligned_wrapper(
            'dba_test', 'revert_trunc', 'ref_table', v_switch,
            FALSE, 'col_id'
        );
        COMMIT;

        CALL dba.partition_table_revert('dba_test', 'revert_trunc', FALSE);

        -- The parent-shell index (55 chars) must result in exactly 63 chars ending with '_retired'
        SELECT count(*) INTO v_count
        FROM pg_indexes
        WHERE schemaname = 'dba_test'
          AND tablename  = 'revert_trunc_partitioned_retired'
          AND indexname  LIKE '%_retired'
          AND length(indexname) = 63;

        PERFORM dba_test.record_result('g1_long_idx_truncated_with_retired_suffix',
            CASE WHEN v_count >= 1 THEN 'PASS' ELSE 'FAIL' END,
            format('expected at least 1 index of length 63 ending in _retired, got %s', v_count));

        EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_trunc CASCADE';
        EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_trunc_partitioned_retired CASCADE';
        DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'revert_trunc';
        DELETE FROM dba.detached_partitions       WHERE schema = 'dba_test'   AND parent_relname = 'revert_trunc';
        COMMIT;
    END;

    -- ----------------------------------------------------------------
    -- Test: negative — revert raises on a non-partitioned table
    -- ----------------------------------------------------------------
    BEGIN
        CALL dba.partition_table_revert('dba_test', 'revert_target', FALSE);
        PERFORM dba_test.record_result('revert_raises_on_non_partitioned', 'FAIL',
            'expected exception, none raised');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('revert_raises_on_non_partitioned', 'PASS', NULL);
    END;

    -- ----------------------------------------------------------------
    -- Test: negative — revert raises when a non-mammoth partition has rows
    -- ----------------------------------------------------------------

    -- Re-partition the target for this test
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'revert_target', 'ref_table', v_switch,
        FALSE,
        'col_id'
    );
    COMMIT;

    -- Insert a row directly into the bridge partition (first non-mammoth child)
    DECLARE
        v_bridge_name  text;
        v_bridge_lower bigint;
    BEGIN
        SELECT lower(child.relname),
               (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint
        INTO v_bridge_name, v_bridge_lower
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        WHERE parent.relname = 'revert_target'
          AND parent.relnamespace = 'dba_test'::regnamespace
          AND NOT lower(child.relname) ~ 'mammoth'
        ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                  '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC
        LIMIT 1;

        EXECUTE format('INSERT INTO dba_test.%I VALUES (%s, %L)', v_bridge_name, v_bridge_lower, 'crossed');
    END;

    BEGIN
        CALL dba.partition_table_revert('dba_test', 'revert_target', FALSE);
        PERFORM dba_test.record_result('revert_raises_when_data_crossed', 'FAIL',
            'expected exception, none raised');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('revert_raises_when_data_crossed', 'PASS', NULL);
    END;

    -- ----------------------------------------------------------------
    -- Final cleanup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.revert_target_partitioned_retired CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ref_table CASCADE';
    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.revert_trigger_fn() CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'revert_target';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test' AND parent_relname = 'revert_target';

    DECLARE
        v_leftover text;
    BEGIN
        FOR v_leftover IN
            SELECT relname FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE lower(n.nspname) = 'dba_test'
              AND lower(c.relname) LIKE 'revert_target_%'
              AND c.relkind = 'r'
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS dba_test.%I CASCADE', v_leftover);
        END LOOP;
    END;

    COMMIT;
END;
$$;
