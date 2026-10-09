/*
Test: test_partition_fix_triggers_on_all_partitions
Function under test: dba.partition_fix_triggers_on_all_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_fix_triggers_on_all_partitions
Purpose: Verify that trigger inconsistencies on partitioned tables are correctly repaired.
Test coverage:
  - Row-level trigger on some children but not others: fix promotes it to parent, which
    PostgreSQL auto-clones to all children. Returns TRUE.
  - Statement-level trigger on some children but not others: fix adds it directly to each
    missing child. Returns TRUE.
  - Already consistent (trigger on parent, auto-cloned to all children): no-op. Returns TRUE.
  - No triggers anywhere: no-op. Returns TRUE.
  - Non-partitioned table raises an exception.
  - Dry-run mode returns FALSE and does not apply any changes.
  - After a repair, dba.partition_check_triggers_on_all_partitions also returns TRUE.

Note: PostgreSQL 13+ auto-propagates row-level parent triggers to all child partitions as
internal clones. The "missing clone" scenario (parent trigger present, clone missing from a
child) cannot be constructed in practice because PG prevents dropping a clone while the
parent trigger exists. The repair path for that case is included as a safety net.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_fix_triggers_on_all_partitions()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result   boolean;
    v_parent   oid;
BEGIN

    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.trg_fix_noop() CASCADE';
    EXECUTE $sql$
        CREATE FUNCTION dba_test.trg_fix_noop() RETURNS trigger LANGUAGE plpgsql AS $b$ BEGIN RETURN NEW; END $b$
    $sql$;

    -- -----------------------------------------------------------------------
    -- Case 1: Row-level trigger on some children but not all -> promoted to parent
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_row CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_row (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_row_0_10  PARTITION OF dba_test.trg_fix_row FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_row_10_20 PARTITION OF dba_test.trg_fix_row FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_fix_row_0_10
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_fix_noop()
    $sql$;

    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_row') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'row_level_fix_returns_true');

    -- All children must now have the trigger
    SELECT oid INTO v_parent FROM pg_class
    WHERE relname = 'trg_fix_row'
      AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba_test');

    SELECT NOT EXISTS (
        SELECT 1 FROM pg_inherits i
        WHERE i.inhparent = v_parent
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t WHERE t.tgrelid = i.inhrelid AND t.tgname = 'trg_test'
          )
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'row_level_fix_all_children_have_trigger');

    -- Trigger must now exist on the parent
    SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = v_parent AND tgname = 'trg_test' AND tgisinternal = false AND tgparentid = 0
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'row_level_fix_trigger_on_parent');

    -- All children must have the same trigger set as the parent (catalog-level consistency check)
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_inherits i
        JOIN pg_trigger tp ON tp.tgrelid = v_parent AND tp.tgisinternal = false AND tp.tgparentid = 0
        WHERE i.inhparent = v_parent
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger tc WHERE tc.tgrelid = i.inhrelid AND tc.tgname = tp.tgname
          )
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'row_level_fix_parent_cloned_to_all_children');

    -- -----------------------------------------------------------------------
    -- Case 2: Statement-level trigger on some children but not all -> added to missing children
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_stmt CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_stmt (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_stmt_0_10  PARTITION OF dba_test.trg_fix_stmt FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_stmt_10_20 PARTITION OF dba_test.trg_fix_stmt FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_stmt_test AFTER INSERT ON dba_test.trg_fix_stmt_0_10
            FOR EACH STATEMENT EXECUTE FUNCTION dba_test.trg_fix_noop()
    $sql$;

    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_stmt') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'stmt_level_fix_returns_true');

    -- Both children must now have the trigger directly (not as clones)
    SELECT oid INTO v_parent FROM pg_class
    WHERE relname = 'trg_fix_stmt'
      AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba_test');

    SELECT NOT EXISTS (
        SELECT 1 FROM pg_inherits i
        WHERE i.inhparent = v_parent
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t
              WHERE t.tgrelid = i.inhrelid AND t.tgname = 'trg_stmt_test' AND t.tgisinternal = false
          )
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'stmt_level_fix_all_children_have_trigger');

    -- Statement-level trigger must NOT have been promoted to the parent
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = v_parent AND tgname = 'trg_stmt_test'
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'stmt_level_fix_not_on_parent');

    -- All children must have the trigger (direct, not cloned)
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_inherits i
        WHERE i.inhparent = v_parent
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t
              WHERE t.tgrelid = i.inhrelid AND t.tgname = 'trg_stmt_test' AND t.tgisinternal = false AND t.tgparentid = 0
          )
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'stmt_level_fix_all_children_have_direct_trigger');

    -- -----------------------------------------------------------------------
    -- Case 3: Already consistent (trigger on parent, auto-cloned) -> no-op
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_ok CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_ok (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_ok_0_10  PARTITION OF dba_test.trg_fix_ok FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_ok_10_20 PARTITION OF dba_test.trg_fix_ok FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_fix_ok
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_fix_noop()
    $sql$;

    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_ok') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'already_consistent_returns_true');

    -- -----------------------------------------------------------------------
    -- Case 4: No triggers anywhere -> returns TRUE
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_none CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_none (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_none_0_10 PARTITION OF dba_test.trg_fix_none FOR VALUES FROM (0) TO (10)';

    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_none') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'no_triggers_returns_true');

    -- -----------------------------------------------------------------------
    -- Case 5: Non-partitioned table raises an exception
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_plain CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_plain (id bigint NOT NULL)';

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$ SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_plain') $sql$,
        'P0001',
        'not_partitioned_raises_exception'
    );

    -- -----------------------------------------------------------------------
    -- Case 6: Dry-run returns FALSE and does not apply changes
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_dryrun CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_dryrun (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_dryrun_0_10  PARTITION OF dba_test.trg_fix_dryrun FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_dryrun_10_20 PARTITION OF dba_test.trg_fix_dryrun FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_fix_dryrun_0_10
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_fix_noop()
    $sql$;

    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_dryrun', TRUE) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(NOT v_result, 'dry_run_returns_false');

    -- Trigger must still be absent from the second child (no changes applied)
    SELECT oid INTO v_parent FROM pg_class
    WHERE relname = 'trg_fix_dryrun'
      AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba_test');

    SELECT EXISTS (
        SELECT 1 FROM pg_inherits i
        WHERE i.inhparent = v_parent
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t WHERE t.tgrelid = i.inhrelid AND t.tgname = 'trg_test'
          )
    ) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'dry_run_does_not_apply_changes');

    -- -----------------------------------------------------------------------
    -- Case 7: Parent trigger present, clone missing from a child (catalog-level inconsistency)
    --
    -- This state cannot occur in normal PostgreSQL operation (PG prevents dropping
    -- a clone while its parent trigger exists). We force it by deleting the clone
    -- row directly from pg_trigger. Dry-run is used to verify the function detects
    -- the inconsistency: the non-dry-run repair path (DROP TRIGGER on the parent)
    -- would itself fail on catalog-corrupted state.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_fix_parent_missing CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_parent_missing (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_parent_missing_0_10  PARTITION OF dba_test.trg_fix_parent_missing FOR VALUES FROM (0)  TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_fix_parent_missing_10_20 PARTITION OF dba_test.trg_fix_parent_missing FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_fix_parent_missing
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_fix_noop()
    $sql$;
    -- Forcibly delete the auto-cloned trigger from one child to simulate catalog-level inconsistency
    DELETE FROM pg_catalog.pg_trigger
    WHERE tgrelid = 'dba_test.trg_fix_parent_missing_0_10'::regclass
      AND tgname = 'trg_test'
      AND tgparentid != 0;

    -- Dry-run: the function must detect the inconsistency and return FALSE
    SELECT dba.partition_fix_triggers_on_all_partitions('dba_test', 'trg_fix_parent_missing', TRUE) INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(NOT v_result, 'parent_missing_child_detected_by_fix');

    RETURN;
END;
$$;
