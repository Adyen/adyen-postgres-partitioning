/*
Test: test_partition_table_revert_fk
Functions under test: dba.partition_table_revert, dba.partition_table_revert_cleanup
Run: ./test/framework/run_partition_tests.sh test_partition_table_revert_fk
Purpose: Verify that partition_table_revert correctly handles incoming FK constraints
         from other tables that reference the partitioned table.

Background: partition_native migrates incoming FKs from the original table to the new
partitioned parent during partitioning.  Without FK handling in the revert procedure,
those FKs would follow the OID and end up pointing to the empty retired shell after
the rename — breaking all subsequent DML on the referencing tables.

Test coverage:
  - FK from a non-partitioned referencing table is preserved through revert
  - FK from a partitioned referencing table is preserved through revert
  - dry_run does not drop or re-add any FK constraints
  - After revert, FK confrelid points to the restored table, not the retired shell
  - Valid DML on referencing tables succeeds after revert
  - Invalid DML on referencing tables fails after revert (FK enforced)
  - partition_table_revert_cleanup succeeds after an FK-aware revert
  - Round-trip: partition → revert → partition again preserves FK integrity
*/

CREATE OR REPLACE PROCEDURE dba_test.partition_table_revert_fk_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count          int;
    v_relname        name;
    v_is_partitioned boolean;
    v_exists         boolean;
    v_switch         text := '1500';
BEGIN
    -- ----------------------------------------------------------------
    -- Setup: drop any leftovers from previous interrupted runs
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_simple_ref   CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_part_ref     CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_target       CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_target_partitioned_retired CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_ref_leader   CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'fk_target';
    DELETE FROM dba.detached_partitions       WHERE schema     = 'dba_test' AND parent_relname = 'fk_target';

    -- ----------------------------------------------------------------
    -- Setup: alignment leader table (two partitions, grid width = 1000)
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.fk_ref_leader (ref_id bigint NOT NULL) PARTITION BY RANGE (ref_id);
    CREATE TABLE dba_test.fk_ref_leader_0_1000    PARTITION OF dba_test.fk_ref_leader FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.fk_ref_leader_1000_2000 PARTITION OF dba_test.fk_ref_leader FOR VALUES FROM (1000) TO (2000);

    -- ----------------------------------------------------------------
    -- Setup: target table (will be partitioned and then reverted)
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.fk_target (
        col_id  bigint NOT NULL,
        data    text,
        PRIMARY KEY (col_id)
    );
    INSERT INTO dba_test.fk_target VALUES (500, 'seed row');

    -- ----------------------------------------------------------------
    -- Setup: non-partitioned referencing table with FK → fk_target
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.fk_simple_ref (
        ref_id  bigserial PRIMARY KEY,
        link_id   bigint NOT NULL,
        CONSTRAINT fk_simple_ref_linkid_fkey FOREIGN KEY (link_id) REFERENCES dba_test.fk_target (col_id)
    );
    INSERT INTO dba_test.fk_simple_ref (link_id) VALUES (500);

    -- ----------------------------------------------------------------
    -- Setup: partitioned referencing table with FK → fk_target
    -- ----------------------------------------------------------------
    -- parent_link_id gets a second FK to the same target added after partitioning
    -- (see below) so the revert sees two distinct FKs from one partitioned
    -- referencing table.
    CREATE TABLE dba_test.fk_part_ref (
        part_id      bigint NOT NULL,
        link_id        bigint NOT NULL,
        parent_link_id bigint,
        CONSTRAINT fk_part_ref_linkid_fkey FOREIGN KEY (link_id) REFERENCES dba_test.fk_target (col_id)
    ) PARTITION BY RANGE (part_id);
    CREATE TABLE dba_test.fk_part_ref_0_1000    PARTITION OF dba_test.fk_part_ref FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.fk_part_ref_1000_2000 PARTITION OF dba_test.fk_part_ref FOR VALUES FROM (1000) TO (2000);
    INSERT INTO dba_test.fk_part_ref VALUES (1, 500, 500);

    COMMIT;

    -- ----------------------------------------------------------------
    -- Step 1: partition fk_target
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'fk_target', 'fk_ref_leader', v_switch,
        FALSE,
        'col_id'
    );
    COMMIT;

    -- Sanity: target is partitioned
    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'fk_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_fk_setup_partitioned',
        CASE WHEN v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Add a second, distinct FK from the partitioned referencing table to the same
    -- target.  Deriving per-partition constraint names from the referenced table name
    -- would map both FKs onto one identifier and fail on the second ADD CONSTRAINT
    -- during revert, so this covers the per-partition naming collision path.
    ALTER TABLE dba_test.fk_part_ref
        ADD CONSTRAINT fk_part_ref_parent_linkid_fkey
        FOREIGN KEY (parent_link_id) REFERENCES dba_test.fk_target (col_id);
    COMMIT;

    -- Sanity: FKs exist on both referencing tables after partitioning
    SELECT count(*) INTO v_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = 'dba_test'
      AND lower(ref_c.relname) = 'fk_target';

    PERFORM dba_test.record_result('revert_fk_setup_fks_present',
        CASE WHEN v_count = 3 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 3 incoming FKs on fk_target after partitioning, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: dry_run does not drop or re-add FKs
    -- ----------------------------------------------------------------
    CALL dba.partition_table_revert('dba_test', 'fk_target', TRUE);

    SELECT count(*) INTO v_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = 'dba_test'
      AND lower(ref_c.relname) = 'fk_target';

    PERFORM dba_test.record_result('revert_fk_dry_run_fks_unchanged',
        CASE WHEN v_count = 3 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 3 incoming FKs after dry-run, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: apply the revert
    -- ----------------------------------------------------------------
    CALL dba.partition_table_revert('dba_test', 'fk_target', FALSE);

    -- fk_target must be a plain (non-partitioned) table again
    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'fk_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_fk_apply_not_partitioned',
        CASE WHEN NOT v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- FK from simple (non-partitioned) referencing table must still exist
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conname = 'fk_simple_ref_linkid_fkey'
      AND conrelid = 'dba_test.fk_simple_ref'::regclass;

    PERFORM dba_test.record_result('revert_fk_simple_ref_preserved',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        format('expected fk_simple_ref_linkid_fkey on fk_simple_ref, count=%s', v_count));

    -- FK from partitioned referencing table must still exist on the parent
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conname = 'fk_part_ref_linkid_fkey'
      AND conrelid = 'dba_test.fk_part_ref'::regclass
      AND coninhcount = 0;

    PERFORM dba_test.record_result('revert_fk_part_ref_parent_preserved',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        format('expected fk_part_ref_linkid_fkey on fk_part_ref parent, count=%s', v_count));

    -- FK confrelid must point to the restored fk_target, NOT to the retired shell
    SELECT c.relname INTO v_relname
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.confrelid
    WHERE con.conname  = 'fk_simple_ref_linkid_fkey'
      AND con.conrelid = 'dba_test.fk_simple_ref'::regclass;

    PERFORM dba_test.record_result('revert_fk_confrelid_points_to_restored_table',
        CASE WHEN v_relname = 'fk_target'::name THEN 'PASS' ELSE 'FAIL' END,
        format('expected confrelid=fk_target, got %s', v_relname));

    -- FK must be marked as validated
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conname     = 'fk_simple_ref_linkid_fkey'
      AND conrelid    = 'dba_test.fk_simple_ref'::regclass
      AND convalidated = true;

    PERFORM dba_test.record_result('revert_fk_simple_ref_validated',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        format('expected fk_simple_ref_linkid_fkey to be convalidated=true, count=%s', v_count));

    -- FK from partitioned ref table must be validated on parent
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conname      = 'fk_part_ref_linkid_fkey'
      AND conrelid     = 'dba_test.fk_part_ref'::regclass
      AND convalidated = true;

    PERFORM dba_test.record_result('revert_fk_part_ref_parent_validated',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        format('expected fk_part_ref_linkid_fkey on fk_part_ref parent to be convalidated=true, count=%s', v_count));

    -- Both distinct FKs on the partitioned referencing table must survive, each with
    -- its own parent constraint.  A per-partition naming collision would have aborted
    -- the revert before this point.
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conrelid    = 'dba_test.fk_part_ref'::regclass
      AND contype     = 'f'
      AND coninhcount = 0
      AND conname IN ('fk_part_ref_linkid_fkey', 'fk_part_ref_parent_linkid_fkey');

    PERFORM dba_test.record_result('revert_fk_part_ref_both_distinct_fks_preserved',
        CASE WHEN v_count = 2 THEN 'PASS' ELSE 'FAIL' END,
        format('expected both distinct FKs on fk_part_ref parent, got %s', v_count));

    -- Each partition must carry two separate FK children (one per parent FK), proving
    -- the per-partition names did not collapse into a single identifier.
    SELECT count(*) INTO v_count
    FROM pg_constraint
    WHERE conrelid = 'dba_test.fk_part_ref_0_1000'::regclass
      AND contype  = 'f';

    PERFORM dba_test.record_result('revert_fk_part_ref_partition_has_both_fks',
        CASE WHEN v_count = 2 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 2 FK constraints on partition fk_part_ref_0_1000, got %s', v_count));

    -- Every partition must carry exactly one validated FK child per parent FK, each
    -- pointing at the restored fk_target.  The checks above only look at the parent and
    -- at fk_part_ref_0_1000, so a per-partition loop that skipped fk_part_ref_1000_2000,
    -- left a child NOT VALID, or pointed a child at the retired shell would pass them.
    -- Counting (partition x parent FK) pairs that have exactly one such child gives one
    -- combined result: 2 partitions x 2 parent FKs = 4.
    WITH parent_fks AS (
        SELECT oid
        FROM pg_constraint
        WHERE conrelid    = 'dba_test.fk_part_ref'::regclass
          AND contype     = 'f'
          AND coninhcount = 0
    ), partitions AS (
        SELECT inhrelid AS part_oid
        FROM pg_inherits
        WHERE inhparent = 'dba_test.fk_part_ref'::regclass
    )
    SELECT count(*)
    INTO v_count
    FROM partitions p
    CROSS JOIN parent_fks pf
    WHERE (
        SELECT count(*)
        FROM pg_constraint child
        WHERE child.conrelid    = p.part_oid
          AND child.conparentid = pf.oid
          AND child.contype     = 'f'
          AND child.confrelid   = 'dba_test.fk_target'::regclass
          AND child.convalidated
    ) = 1;

    PERFORM dba_test.record_result('revert_fk_part_ref_all_partitions_fks_validated',
        CASE WHEN v_count = 4 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 4 partition/parent-FK pairs with exactly one validated child referencing fk_target, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: DML on referencing tables works correctly after revert
    -- ----------------------------------------------------------------

    -- Valid insert into simple ref (link_id=500 exists in fk_target) must succeed
    BEGIN
        INSERT INTO dba_test.fk_simple_ref (link_id) VALUES (500);
        PERFORM dba_test.record_result('revert_fk_valid_insert_simple_ref',
            'PASS', NULL);
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('revert_fk_valid_insert_simple_ref',
            'FAIL', format('unexpected error: %s', SQLERRM));
    END;

    -- Invalid insert into simple ref (link_id=9999 does not exist) must fail
    BEGIN
        INSERT INTO dba_test.fk_simple_ref (link_id) VALUES (9999);
        PERFORM dba_test.record_result('revert_fk_invalid_insert_simple_ref',
            'FAIL', 'expected FK violation, none raised');
    EXCEPTION WHEN foreign_key_violation THEN
        PERFORM dba_test.record_result('revert_fk_invalid_insert_simple_ref',
            'PASS', NULL);
    END;

    -- Valid insert into partitioned ref (link_id=500 exists) must succeed
    BEGIN
        INSERT INTO dba_test.fk_part_ref VALUES (2, 500);
        PERFORM dba_test.record_result('revert_fk_valid_insert_part_ref',
            'PASS', NULL);
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('revert_fk_valid_insert_part_ref',
            'FAIL', format('unexpected error: %s', SQLERRM));
    END;

    -- Invalid insert into partitioned ref (link_id=9999 does not exist) must fail
    BEGIN
        INSERT INTO dba_test.fk_part_ref VALUES (3, 9999);
        PERFORM dba_test.record_result('revert_fk_invalid_insert_part_ref',
            'FAIL', 'expected FK violation, none raised');
    EXCEPTION WHEN foreign_key_violation THEN
        PERFORM dba_test.record_result('revert_fk_invalid_insert_part_ref',
            'PASS', NULL);
    END;

    -- ----------------------------------------------------------------
    -- Test: cleanup succeeds after FK-aware revert
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_revert_cleanup('dba_test', 'fk_target', FALSE);

    -- Retired shell must be gone
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = 'dba_test'
          AND lower(c.relname) = 'fk_target_partitioned_retired'
    ) INTO v_exists;

    PERFORM dba_test.record_result('revert_fk_cleanup_retired_shell_gone',
        CASE WHEN NOT v_exists THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Data in fk_target must still be intact after cleanup
    SELECT count(*) INTO v_count FROM dba_test.fk_target WHERE col_id = 500;
    PERFORM dba_test.record_result('revert_fk_cleanup_data_intact',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- FKs on referencing tables must still exist after cleanup
    SELECT count(*) INTO v_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = 'dba_test'
      AND lower(ref_c.relname) = 'fk_target';

    PERFORM dba_test.record_result('revert_fk_cleanup_fks_still_present',
        CASE WHEN v_count = 3 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 3 incoming FKs after cleanup, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Test: round-trip — partition again after FK-aware revert must work
    --
    -- The second FK is dropped first: dba.partition_native still derives its
    -- per-partition FK names from the referenced table name, so re-partitioning with
    -- two FKs from one partitioned referencing table collides there.  That limitation is
    -- in the forward path, so the round-trip below covers the single-FK case.
    -- ----------------------------------------------------------------
    ALTER TABLE dba_test.fk_part_ref DROP CONSTRAINT fk_part_ref_parent_linkid_fkey;
    COMMIT;

    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'fk_target', 'fk_ref_leader', v_switch,
        FALSE, 'col_id'
    );
    COMMIT;

    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'fk_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_fk_round_trip_partitioned',
        CASE WHEN v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT count(*) INTO v_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = 'dba_test'
      AND lower(ref_c.relname) = 'fk_target';

    PERFORM dba_test.record_result('revert_fk_round_trip_fks_present',
        CASE WHEN v_count = 2 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 2 incoming FKs after second partitioning, got %s', v_count));

    -- Revert the second partitioning to clean up
    CALL dba.partition_table_revert('dba_test', 'fk_target', FALSE);
    PERFORM dba.partition_table_revert_cleanup('dba_test', 'fk_target', FALSE);

    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'fk_target' AND c.relnamespace = 'dba_test'::regnamespace;

    PERFORM dba_test.record_result('revert_fk_round_trip_second_revert',
        CASE WHEN NOT v_is_partitioned THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT count(*) INTO v_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = 'dba_test'
      AND lower(ref_c.relname) = 'fk_target';

    PERFORM dba_test.record_result('revert_fk_round_trip_fks_after_second_revert',
        CASE WHEN v_count = 2 THEN 'PASS' ELSE 'FAIL' END,
        format('expected 2 incoming FKs after second revert, got %s', v_count));

    -- ----------------------------------------------------------------
    -- Final cleanup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_simple_ref   CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_part_ref     CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_target       CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_target_partitioned_retired CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_ref_leader   CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'fk_target';
    DELETE FROM dba.detached_partitions       WHERE schema     = 'dba_test' AND parent_relname = 'fk_target';
    COMMIT;
END;
$$;
