/*
Test: test_partition_realign_boundaries
Function under test: dba.partition_realign_boundaries
Run: ./test/framework/run_partition_tests.sh test_partition_realign_boundaries
Purpose: Verify that partition_realign_boundaries correctly detaches misaligned free
         partitions and replaces them with grid-aligned ones without touching the data partition.
Test coverage:
  - dry_run=true (default) makes no structural changes
  - apply: data partition untouched, free partitions detached and replaced
  - data is still accessible after realignment
  - bridge upper is on the alignment grid
  - aligned partitions have width == partition_size
  - exactly 3 free partitions after apply
  - bridge-skip rule: when bridge_gap < P/3, bridge_upper is extended by one grid slot
  - zero free partitions: just adds aligned partitions from the data partition's upper bound
  - negative: non-partitioned table raises
  - negative: non-integer partition key raises
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_realign_boundaries()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count           int;
    v_partition_size  bigint := 1000;
    v_grid_anchor     bigint := 0;
    v_data_lower      text;
    v_data_upper      text;
    v_bridge_upper    bigint;
    v_upper_bounds    bigint[];
    i                 int;
    v_width           bigint;
BEGIN
    -- ----------------------------------------------------------------
    -- Setup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_std CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_skip CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_zero_free CASCADE';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test';

    -- Standard-bridge table: data [0, 1500) with seed row; two misaligned free partitions.
    -- With P=1000, anchor=0: data_upper=1500, gap=500 >= P/3=333 → standard bridge_upper=2000
    CREATE TABLE dba_test.rb_std (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rb_std_0_1500    PARTITION OF dba_test.rb_std FOR VALUES FROM (0)    TO (1500);
    CREATE TABLE dba_test.rb_std_1500_2600 PARTITION OF dba_test.rb_std FOR VALUES FROM (1500) TO (2600);
    CREATE TABLE dba_test.rb_std_2600_3700 PARTITION OF dba_test.rb_std FOR VALUES FROM (2600) TO (3700);
    INSERT INTO dba_test.rb_std VALUES (1200);

    -- ----------------------------------------------------------------
    -- Test 1: dry_run=true (default) makes no structural changes
    -- ----------------------------------------------------------------
    PERFORM dba.partition_realign_boundaries('dba_test', 'rb_std', v_partition_size, v_grid_anchor);

    SELECT count(*) INTO v_count
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    WHERE parent.relname = 'rb_std' AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'rb_dry_run_child_count_unchanged');

    -- ----------------------------------------------------------------
    -- Test 2: apply — standard bridge
    -- ----------------------------------------------------------------
    PERFORM dba.partition_realign_boundaries('dba_test', 'rb_std', v_partition_size, v_grid_anchor, FALSE);

    -- Data is still accessible through the parent
    SELECT count(*) INTO v_count FROM dba_test.rb_std WHERE col_id = 1200;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'rb_data_accessible_after_realign');

    -- Data partition retains original bounds [0, 1500)
    SELECT v_lower_bound, v_upper_bound
    INTO v_data_lower, v_data_upper
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'rb_std');

    RETURN QUERY SELECT * FROM dba_test.assert_equals('0',    v_data_lower, 'rb_data_partition_lower_unchanged');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('1500', v_data_upper, 'rb_data_partition_upper_unchanged');

    -- Exactly 3 free partitions after realign
    SELECT dba.partition_calculate_free_partitions('dba_test', 'rb_std') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'rb_three_free_partitions');

    -- Total children: data(1) + bridge(1) + 2 aligned = 4
    SELECT count(*) INTO v_count
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    WHERE parent.relname = 'rb_std' AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count, 'rb_total_child_count');

    -- Collect upper bounds of all partitions with lower >= 1500, ordered by lower bound
    SELECT array_agg(
        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
         '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
        ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                  '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC
    )
    INTO v_upper_bounds
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'rb_std'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
           '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint >= 1500;

    -- Bridge: [1500, 2000) — upper on grid
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2000::bigint, v_upper_bounds[1], 'rb_bridge_upper_on_grid');

    -- First aligned: [2000, 3000) — width == partition_size
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3000::bigint, v_upper_bounds[2], 'rb_first_aligned_upper');

    -- Second aligned: [3000, 4000) — width == partition_size
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4000::bigint, v_upper_bounds[3], 'rb_second_aligned_upper');

    -- Bridge upper modulo partition_size == grid_anchor modulo partition_size
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_grid_anchor % v_partition_size, v_upper_bounds[1] % v_partition_size,
        'rb_bridge_upper_grid_aligned');

    -- ----------------------------------------------------------------
    -- Test 3: bridge-skip rule (data_upper=1900, gap=100 < P/3=333 → bridge_upper=3000)
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.rb_skip (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rb_skip_0_1900    PARTITION OF dba_test.rb_skip FOR VALUES FROM (0)    TO (1900);
    CREATE TABLE dba_test.rb_skip_1900_2600 PARTITION OF dba_test.rb_skip FOR VALUES FROM (1900) TO (2600);
    INSERT INTO dba_test.rb_skip VALUES (1800);

    PERFORM dba.partition_realign_boundaries('dba_test', 'rb_skip', v_partition_size, v_grid_anchor, FALSE);

    -- Bridge upper must be at 3000 (aligned_lower=2000, gap=100 < 333 → skip one slot)
    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
            '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_bridge_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'rb_skip'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
           '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint = 1900;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(3000::bigint, v_bridge_upper, 'rb_skip_rule_bridge_upper');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        0::bigint, v_bridge_upper % v_partition_size,
        'rb_skip_rule_bridge_on_grid');

    -- ----------------------------------------------------------------
    -- Test 4: zero free partitions — adds aligned partitions from data_upper
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.rb_zero_free (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rb_zero_free_0_1500 PARTITION OF dba_test.rb_zero_free FOR VALUES FROM (0) TO (1500);
    INSERT INTO dba_test.rb_zero_free VALUES (1200);

    PERFORM dba.partition_realign_boundaries('dba_test', 'rb_zero_free', v_partition_size, v_grid_anchor, FALSE);

    SELECT dba.partition_calculate_free_partitions('dba_test', 'rb_zero_free') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'rb_zero_free_adds_partitions');

    SELECT count(*) INTO v_count FROM dba_test.rb_zero_free WHERE col_id = 1200;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'rb_zero_free_data_accessible');

    -- ----------------------------------------------------------------
    -- Test 5: negative — non-partitioned table raises
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_plain CASCADE';
    CREATE TABLE dba_test.rb_plain (col_id bigint NOT NULL PRIMARY KEY);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_realign_boundaries('dba_test', 'rb_plain', 1000)$sql$,
        'P0001',
        'rb_non_partitioned_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_plain CASCADE';

    -- ----------------------------------------------------------------
    -- Test 6: negative — non-integer partition key raises
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_text_part CASCADE';
    CREATE TABLE dba_test.rb_text_part (key_col text NOT NULL) PARTITION BY RANGE (key_col);
    CREATE TABLE dba_test.rb_text_part_a_m PARTITION OF dba_test.rb_text_part FOR VALUES FROM ('a') TO ('m');
    CREATE TABLE dba_test.rb_text_part_m_z PARTITION OF dba_test.rb_text_part FOR VALUES FROM ('m') TO ('z');

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_realign_boundaries('dba_test', 'rb_text_part', 1000)$sql$,
        'P0001',
        'rb_non_integer_key_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_text_part CASCADE';

    -- ----------------------------------------------------------------
    -- Cleanup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_std CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_skip CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rb_zero_free CASCADE';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test';

    RETURN;
END;
$$;
