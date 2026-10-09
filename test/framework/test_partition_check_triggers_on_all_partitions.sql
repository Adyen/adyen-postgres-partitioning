/*
Test: test_partition_check_triggers_on_all_partitions
Function under test: dba.partition_check_triggers_on_all_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_check_triggers_on_all_partitions
Purpose: Verify trigger consistency checks across partitions work correctly.
Test coverage:
  - Returns TRUE when a trigger on the parent is auto-propagated to all children.
  - Returns FALSE when children have inconsistent triggers (one child has a directly-created
    trigger that the other does not -- a missing-trigger inconsistency).
  - Returns TRUE when multiple triggers on the parent are all consistently propagated.
  - Returns TRUE when no triggers exist anywhere.
  - Raises an exception for a non-partitioned table.
  - Returns TRUE when children are created via LIKE+ATTACH and the parent trigger is
    auto-propagated to all children at attach time.

Note: In PostgreSQL 13+, row-level triggers on a partitioned table are automatically cloned to all
existing and future child partitions, and PostgreSQL prevents dropping a cloned trigger from a child
while the parent trigger still exists. The "inconsistent children" scenario therefore arises when a
trigger is added directly to individual child partitions rather than through the parent.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_check_triggers_on_all_partitions()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result boolean;
BEGIN

    -- Shared trigger function used across all test cases
    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.trg_noop() CASCADE';
    EXECUTE $sql$
        CREATE FUNCTION dba_test.trg_noop() RETURNS trigger LANGUAGE plpgsql AS $b$ BEGIN RETURN NEW; END $b$
    $sql$;

    -- -----------------------------------------------------------------------
    -- Case 1: trigger on parent auto-propagates to all children -> TRUE
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_ok CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_ok (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_ok_0_10  PARTITION OF dba_test.trg_ok FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_ok_10_20 PARTITION OF dba_test.trg_ok FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_ok
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_ok') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'triggers_all_consistent');

    -- -----------------------------------------------------------------------
    -- Case 2: trigger present on one child but not the other -> FALSE
    --
    -- This simulates a missing-trigger inconsistency: a trigger was added directly to some
    -- child partitions (e.g., before the parent trigger existed) but not to others.
    -- The parent has no corresponding trigger in this scenario.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_inconsistent CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_inconsistent (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_inconsistent_0_10  PARTITION OF dba_test.trg_inconsistent FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_inconsistent_10_20 PARTITION OF dba_test.trg_inconsistent FOR VALUES FROM (10) TO (20)';
    -- Add trigger only to the first child; parent and second child have no trigger
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_inconsistent_0_10
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_inconsistent') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(NOT v_result, 'triggers_inconsistent_children');

    -- -----------------------------------------------------------------------
    -- Case 3: multiple triggers on parent, all propagated consistently -> TRUE
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_multi CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_multi (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_multi_0_10  PARTITION OF dba_test.trg_multi FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_multi_10_20 PARTITION OF dba_test.trg_multi FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_before BEFORE INSERT ON dba_test.trg_multi
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;
    EXECUTE $sql$
        CREATE TRIGGER trg_after AFTER INSERT ON dba_test.trg_multi
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_multi') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'triggers_multiple_consistent');

    -- -----------------------------------------------------------------------
    -- Case 4: no triggers anywhere -> TRUE
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_none CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_none (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_none_0_10 PARTITION OF dba_test.trg_none FOR VALUES FROM (0) TO (10)';

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_none') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'triggers_no_triggers');

    -- -----------------------------------------------------------------------
    -- Case 5: non-partitioned table raises an exception
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_plain CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_plain (id bigint NOT NULL)';

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$ SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_plain') $sql$,
        'P0001',
        'triggers_not_partitioned'
    );

    -- -----------------------------------------------------------------------
    -- Case 6: trigger on parent only; children created via LIKE+ATTACH -> TRUE
    --
    -- Children are created with CREATE TABLE ... (LIKE parent INCLUDING ALL),
    -- which does NOT copy triggers from the source table, then attached with
    -- ALTER TABLE parent ATTACH PARTITION.  PostgreSQL auto-propagates the
    -- parent trigger to each child at attach time as an internal clone
    -- (tgisinternal=true), so the function should see consistent coverage and
    -- return TRUE.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_like_attach CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_like_attach_0_10 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_like_attach_10_20 CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_like_attach (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_like_attach
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;
    -- Create children via LIKE (no triggers copied) then attach
    EXECUTE 'CREATE TABLE dba_test.trg_like_attach_0_10  (LIKE dba_test.trg_like_attach INCLUDING ALL)';
    EXECUTE 'CREATE TABLE dba_test.trg_like_attach_10_20 (LIKE dba_test.trg_like_attach INCLUDING ALL)';
    EXECUTE 'ALTER TABLE dba_test.trg_like_attach ATTACH PARTITION dba_test.trg_like_attach_0_10  FOR VALUES FROM (0)  TO (10)';
    EXECUTE 'ALTER TABLE dba_test.trg_like_attach ATTACH PARTITION dba_test.trg_like_attach_10_20 FOR VALUES FROM (10) TO (20)';

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_like_attach') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'trigger_on_parent_like_attach_propagated');

    -- -----------------------------------------------------------------------
    -- Case 7: Parent trigger present, clone missing from one child
    --
    -- This state cannot occur in normal PostgreSQL operation (PG prevents dropping
    -- a clone while its parent trigger exists). We force it by deleting the clone
    -- row directly from pg_trigger. The function should detect it and return FALSE.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.trg_parent_missing_child CASCADE';
    EXECUTE 'CREATE TABLE dba_test.trg_parent_missing_child (id bigint NOT NULL) PARTITION BY RANGE (id)';
    EXECUTE 'CREATE TABLE dba_test.trg_parent_missing_child_0_10  PARTITION OF dba_test.trg_parent_missing_child FOR VALUES FROM (0)  TO (10)';
    EXECUTE 'CREATE TABLE dba_test.trg_parent_missing_child_10_20 PARTITION OF dba_test.trg_parent_missing_child FOR VALUES FROM (10) TO (20)';
    EXECUTE $sql$
        CREATE TRIGGER trg_test BEFORE INSERT ON dba_test.trg_parent_missing_child
            FOR EACH ROW EXECUTE FUNCTION dba_test.trg_noop()
    $sql$;
    -- Forcibly delete the auto-cloned trigger from one child to simulate catalog-level inconsistency
    DELETE FROM pg_catalog.pg_trigger
    WHERE tgrelid = 'dba_test.trg_parent_missing_child_0_10'::regclass
      AND tgname = 'trg_test'
      AND tgparentid != 0;

    SELECT dba.partition_check_triggers_on_all_partitions('dba_test', 'trg_parent_missing_child') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(NOT v_result, 'trigger_on_parent_missing_from_child');

    RETURN;
END;
$$;
