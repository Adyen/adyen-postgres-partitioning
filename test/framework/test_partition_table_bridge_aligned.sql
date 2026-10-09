/*
Test: test_partition_table_bridge_aligned
Function under test: dba.partition_table_native_aligned_wrapper
Run: ./test/framework/run_partition_tests.sh test_partition_table_bridge_aligned
Purpose: Verify the bridge-aligned wrapper partitions a table correctly using
         an arbitrary alignment reference table.
Test coverage:
  - dry_run=true (default) makes no structural changes
  - apply creates mammoth + bridge + 3 aligned partitions
  - bridge upper is on the alignment grid
  - aligned partitions each have width == grid_width
  - partition_configuration is populated
  - bridge-skip rule: when bridge_gap < P/3, bridge_upper is bumped by one extra grid slot
  - validation raises when data exceeds switch_boundary
  - validation raises when leader table is partitioned on a non-integer column
  - mixed-case arguments are normalized and register a lowercase configuration row
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_table_bridge_aligned()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count          int;
    v_is_partitioned boolean;
    v_grid_width     bigint := 1000;
    v_ref_anchor     bigint := 1000;   -- last ref partition starts here
    -- Standard bridge test: switch=1500, gap=500, P=1000 → 500 >= 333 → standard
    v_switch_std     text   := '1500';
    v_bridge_upper_std bigint;         -- expected: 2000
    -- Bridge-skip rule test: switch=1900, gap=100, P=1000 → 100 < 333 → skip
    v_switch_skip    text   := '1900';
    v_bridge_upper_skip bigint;        -- expected: 3000
    -- Partition boundary helpers
    v_children       text[];
    v_bounds         record;
    v_mammoth_upper  text;
    v_bridge_lower   text;
    v_bridge_upper_actual text;
    v_cur_lower      text;
    v_width          bigint;
    v_prev_upper     text;
    i                int;
BEGIN
    v_bridge_upper_std  := 2000;
    v_bridge_upper_skip := 3000;

    -- ----------------------------------------------------------------
    -- Setup: reference table (alignment grid) + target table
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ref_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_skip_target CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test'
      AND table_name IN ('bridge_target', 'bridge_skip_target');

    -- ref_table: partitioned on ref_id; last non-mammoth partition [1000, 2000) → P=1000
    CREATE TABLE dba_test.ref_table (
        ref_id bigint NOT NULL
    ) PARTITION BY RANGE (ref_id);
    CREATE TABLE dba_test.ref_table_0_1000    PARTITION OF dba_test.ref_table FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.ref_table_1000_2000 PARTITION OF dba_test.ref_table FOR VALUES FROM (1000) TO (2000);

    -- bridge_target: regular table with col_id bigint
    CREATE TABLE dba_test.bridge_target (
        col_id bigint NOT NULL,
        data   text,
        PRIMARY KEY (col_id)
    );
    ALTER TABLE dba_test.bridge_target ALTER COLUMN col_id SET STATISTICS 400;
    CREATE STATISTICS dba_test.bridge_target_ndist (ndistinct) ON col_id, data FROM dba_test.bridge_target;
    INSERT INTO dba_test.bridge_target VALUES (500, 'seed row');

    -- ----------------------------------------------------------------
    -- Test 1: dry_run=true (default) makes no changes
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'bridge_target', 'ref_table', v_switch_std,
        TRUE, 'col_id'
    );

    SELECT count(*) > 0 INTO v_is_partitioned
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'bridge_target' AND c.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(false, v_is_partitioned,
        'bridge_aligned_dry_run_no_partition');

    SELECT count(*) INTO v_count
    FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'bridge_target';

    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count,
        'bridge_aligned_dry_run_no_config');

    -- ----------------------------------------------------------------
    -- Test 2: apply — standard bridge (switch=1500, gap=500 >= P/3=333)
    -- ----------------------------------------------------------------
    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'bridge_target', 'ref_table', v_switch_std,
        FALSE, 'col_id', p_copy_statistics_to_children := TRUE
    );

    -- Must be partitioned
    SELECT count(*) INTO v_count
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    WHERE c.relname = 'bridge_target' AND c.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
        'bridge_aligned_apply_is_partitioned');

    -- Must have exactly 5 children (mammoth + bridge + 3 aligned)
    SELECT count(*) INTO v_count
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    WHERE parent.relname = 'bridge_target' AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(5, v_count,
        'bridge_aligned_apply_child_count');

    -- partition_configuration row must exist
    SELECT count(*) INTO v_count
    FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'bridge_target';

    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
        'bridge_aligned_apply_config_added');

    SELECT count(*)
    FROM pg_statistic_ext AS s
    JOIN pg_class AS child ON child.oid = s.stxrelid
    JOIN pg_inherits AS i ON i.inhrelid = child.oid
    JOIN pg_class AS parent ON parent.oid = i.inhparent
    WHERE parent.relnamespace = 'dba_test'::regnamespace
        AND parent.relname = 'bridge_target'
        AND child.relname <> 'bridge_target_mammoth'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count,
        'bridge_aligned_extended_statistics_copied_to_children');

    -- Seed row must still be accessible (lives in mammoth)
    SELECT count(*) INTO v_count FROM dba_test.bridge_target WHERE col_id = 500;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
        'bridge_aligned_seed_row_in_mammoth');

    -- Get non-mammoth partitions ordered by lower bound
    SELECT
        array_agg(lower(child.relname) ORDER BY
            (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
             '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC)
    INTO v_children
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'bridge_target'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND NOT lower(child.relname) ~ 'mammoth';

    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, array_length(v_children, 1),
        'bridge_aligned_non_mammoth_count');

    -- Mammoth upper bound
    SELECT
        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
         '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_mammoth_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'bridge_target'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND lower(child.relname) ~ 'mammoth';

    -- Mammoth upper must equal switch_boundary (exclusive)
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_switch_std::bigint, v_mammoth_upper::bigint,
        'bridge_aligned_mammoth_upper_eq_switch_boundary');

    -- Bridge lower must equal switch_boundary (contiguous with mammoth)
    SELECT
        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
         '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1],
        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
         '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_bridge_lower, v_bridge_upper_actual
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE lower(child.relname) = v_children[1]
      AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_switch_std::bigint, v_bridge_lower::bigint,
        'bridge_aligned_bridge_lower_eq_switch_boundary');

    -- Bridge upper must be on the alignment grid
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        0::bigint, (v_bridge_upper_actual::bigint - v_ref_anchor) % v_grid_width,
        'bridge_aligned_bridge_upper_on_grid');

    -- Standard bridge: bridge_upper must equal aligned_lower = 2000
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_bridge_upper_std, v_bridge_upper_actual::bigint,
        'bridge_aligned_standard_bridge_upper');

    -- Each aligned partition (children[2..4]) must have width == grid_width
    FOR i IN 2..4 LOOP
        EXECUTE format($sel$
            SELECT
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                '.*\(''''?(.*?)''''?\).*\(''''?(.*?)''''?\).*'))[2]::bigint
              - (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                '.*\(''''?(.*?)''''?\).*\(''''?(.*?)''''?\).*'))[1]::bigint
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
            WHERE lower(child.relname) = %L
              AND parent.relnamespace = 'dba_test'::regnamespace
        $sel$, v_children[i])
        INTO v_width;

        RETURN QUERY SELECT * FROM dba_test.assert_equals(
            v_grid_width, v_width,
            'bridge_aligned_aligned' || (i-1)::text || '_width');
    END LOOP;

    -- ----------------------------------------------------------------
    -- Test 3: bridge-skip rule (switch=1900, gap=100 < P/3=333 → skip)
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.bridge_skip_target (
        col_id bigint NOT NULL,
        PRIMARY KEY (col_id)
    );
    INSERT INTO dba_test.bridge_skip_target VALUES (100);

    PERFORM dba.partition_table_native_aligned_wrapper(
        'dba_test', 'bridge_skip_target', 'ref_table', v_switch_skip,
        FALSE, 'col_id'
    );

    -- Get bridge partition (first non-mammoth)
    SELECT
        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
         '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_bridge_upper_actual
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'bridge_skip_target'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND NOT lower(child.relname) ~ 'mammoth'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
              '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC
    LIMIT 1;

    -- With bridge-skip rule, bridge_upper must be aligned_lower + P = 3000
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_bridge_upper_skip, v_bridge_upper_actual::bigint,
        'bridge_aligned_skip_rule_bridge_upper');

    -- Skip-rule bridge must still be on the grid
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        0::bigint, (v_bridge_upper_actual::bigint - v_ref_anchor) % v_grid_width,
        'bridge_aligned_skip_rule_on_grid');

    -- ----------------------------------------------------------------
    -- Test 4: negative — data exceeds switch_boundary
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.over_boundary CASCADE';
    CREATE TABLE dba_test.over_boundary (col_id bigint NOT NULL PRIMARY KEY);
    INSERT INTO dba_test.over_boundary VALUES (2000);  -- >= switch_boundary of 1500

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_table_native_aligned_wrapper('dba_test', 'over_boundary', 'ref_table', '1500', false, 'col_id')$sql$,
        'P0001',
        'bridge_aligned_data_exceeds_boundary_raises'
    );

    -- ----------------------------------------------------------------
    -- Test 5: negative — leader table partitioned on a non-integer column
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.text_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.non_int_target CASCADE';

    CREATE TABLE dba_test.text_leader (
        key_col text NOT NULL
    ) PARTITION BY RANGE (key_col);
    CREATE TABLE dba_test.text_leader_a_m PARTITION OF dba_test.text_leader FOR VALUES FROM ('a') TO ('m');
    CREATE TABLE dba_test.text_leader_m_z PARTITION OF dba_test.text_leader FOR VALUES FROM ('m') TO ('z');

    CREATE TABLE dba_test.non_int_target (col_id bigint NOT NULL PRIMARY KEY);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_table_native_aligned_wrapper('dba_test', 'non_int_target', 'text_leader', '1500', false, 'col_id')$sql$,
        'P0001',
        'bridge_aligned_non_integer_leader_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.text_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.non_int_target CASCADE';

    -- ----------------------------------------------------------------
    -- Test 6: mixed-case arguments are normalized; config row is lowercase
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_case CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'bridge_case';

    CREATE TABLE dba_test.bridge_case (
        col_id bigint NOT NULL,
        PRIMARY KEY (col_id)
    );
    INSERT INTO dba_test.bridge_case VALUES (500);

    BEGIN
        PERFORM dba.partition_table_native_aligned_wrapper(
            'DBA_TEST', 'BRIDGE_CASE', 'REF_TABLE', v_switch_std,
            FALSE, 'COL_ID'
        );

        SELECT count(*) INTO v_count
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        WHERE c.relname = 'bridge_case' AND c.relnamespace = 'dba_test'::regnamespace;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
            'bridge_aligned_mixed_case_partitioned');

        SELECT count(*) INTO v_count
        FROM dba.partition_configuration
        WHERE schema_name = 'dba_test' AND table_name = 'bridge_case';
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
            'bridge_aligned_mixed_case_config_added_lowercase');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('bridge_aligned_mixed_case_partitioned', 'FAIL', SQLERRM);
        PERFORM dba_test.record_result('bridge_aligned_mixed_case_config_added_lowercase', 'FAIL', SQLERRM);
    END;

    -- ----------------------------------------------------------------
    -- Cleanup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_skip_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.bridge_case CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ref_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.no_col_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.over_boundary CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test'
      AND table_name IN ('bridge_target', 'bridge_skip_target', 'bridge_case');

    RETURN;
END;
$$;
