/*
Test: test_partition_realign_with_leader
Function under test: dba.partition_realign_with_leader
Run: ./test/framework/run_partition_tests.sh test_partition_realign_with_leader
Purpose: Verify that partition_realign_with_leader correctly detaches misaligned free
         partitions from a follower table and replaces them with partitions whose
         boundaries align with a leader table's partition grid.
Test coverage:
  - dry_run=true (default) makes no structural changes
  - apply: data partition untouched, free partitions detached and replaced
  - data is still accessible after realignment
  - bridge upper lands on the leader's grid
  - aligned partitions have width == leader's grid_width
  - exactly 3 free partitions after apply
  - bridge-skip rule: when bridge_gap < P/3, bridge_upper is extended by one grid slot
  - negative: leader not partitioned raises
  - negative: follower not partitioned raises
  - negative: leader has non-integer partition key raises
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_realign_with_leader()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count        int;
    v_grid_width   bigint := 1000;
    v_grid_anchor  bigint := 2000;  -- last leader partition [1000,2000) → anchor=2000
    v_data_lower   text;
    v_data_upper   text;
    v_bridge_upper bigint;
    v_upper_bounds bigint[];
BEGIN
    -- ----------------------------------------------------------------
    -- Setup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_follower CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_skip_follower CASCADE';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test';

    -- Leader table: grid of width=1000, last partition [1000, 2000) → anchor=2000
    CREATE TABLE dba_test.rwl_leader (ref_id bigint NOT NULL, PRIMARY KEY (ref_id))
        PARTITION BY RANGE (ref_id);
    CREATE TABLE dba_test.rwl_leader_0_1000    PARTITION OF dba_test.rwl_leader FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.rwl_leader_1000_2000 PARTITION OF dba_test.rwl_leader FOR VALUES FROM (1000) TO (2000);
    INSERT INTO dba_test.rwl_leader VALUES (500);

    -- Follower table: data [0, 1500) with seed row; one misaligned free partition [1500, 2600)
    -- With leader grid P=1000, anchor=2000: data_upper=1500, gap=500 >= P/3=333 → bridge_upper=2000
    CREATE TABLE dba_test.rwl_follower (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rwl_follower_0_1500    PARTITION OF dba_test.rwl_follower FOR VALUES FROM (0)    TO (1500);
    CREATE TABLE dba_test.rwl_follower_1500_2600 PARTITION OF dba_test.rwl_follower FOR VALUES FROM (1500) TO (2600);
    INSERT INTO dba_test.rwl_follower VALUES (1200);

    -- ----------------------------------------------------------------
    -- Test 1: dry_run=true (default) makes no structural changes
    -- ----------------------------------------------------------------
    PERFORM dba.partition_realign_with_leader('dba_test', 'rwl_follower', 'rwl_leader');

    SELECT count(*) INTO v_count
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    WHERE parent.relname = 'rwl_follower' AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'rwl_dry_run_child_count_unchanged');

    -- ----------------------------------------------------------------
    -- Test 2: apply — standard bridge (data_upper=1500, gap=500 >= P/3=333)
    -- ----------------------------------------------------------------
    PERFORM dba.partition_realign_with_leader('dba_test', 'rwl_follower', 'rwl_leader', FALSE);

    -- Data is still accessible through the parent
    SELECT count(*) INTO v_count FROM dba_test.rwl_follower WHERE col_id = 1200;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'rwl_data_accessible_after_realign');

    -- Data partition retains original bounds [0, 1500)
    SELECT v_lower_bound, v_upper_bound
    INTO v_data_lower, v_data_upper
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'rwl_follower');

    RETURN QUERY SELECT * FROM dba_test.assert_equals('0',    v_data_lower, 'rwl_data_partition_lower_unchanged');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('1500', v_data_upper, 'rwl_data_partition_upper_unchanged');

    -- Exactly 3 free partitions after realign
    SELECT dba.partition_calculate_free_partitions('dba_test', 'rwl_follower') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'rwl_three_free_partitions');

    -- Total children: data(1) + bridge(1) + 2 aligned = 4
    SELECT count(*) INTO v_count
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    WHERE parent.relname = 'rwl_follower' AND parent.relnamespace = 'dba_test'::regnamespace;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count, 'rwl_total_child_count');

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
    WHERE parent.relname = 'rwl_follower'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
           '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint >= 1500;

    -- Bridge: [1500, 2000) — upper must be on the leader's grid
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2000::bigint, v_upper_bounds[1], 'rwl_bridge_upper_on_leader_grid');

    -- Bridge upper is a grid-aligned boundary: (bridge_upper - anchor) % grid_width == 0
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        0::bigint, (v_upper_bounds[1] - v_grid_anchor) % v_grid_width,
        'rwl_bridge_upper_modulo_grid');

    -- First aligned: [2000, 3000) — width == leader grid_width
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3000::bigint, v_upper_bounds[2], 'rwl_first_aligned_upper');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_grid_width, v_upper_bounds[2] - v_upper_bounds[1],
        'rwl_first_aligned_width_eq_grid');

    -- Second aligned: [3000, 4000) — width == leader grid_width
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4000::bigint, v_upper_bounds[3], 'rwl_second_aligned_upper');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        v_grid_width, v_upper_bounds[3] - v_upper_bounds[2],
        'rwl_second_aligned_width_eq_grid');

    -- ----------------------------------------------------------------
    -- Test 3: bridge-skip rule (data_upper=1900, gap=100 < P/3=333 → bridge_upper=3000)
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.rwl_skip_follower (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rwl_skip_follower_0_1900    PARTITION OF dba_test.rwl_skip_follower FOR VALUES FROM (0)    TO (1900);
    CREATE TABLE dba_test.rwl_skip_follower_1900_2600 PARTITION OF dba_test.rwl_skip_follower FOR VALUES FROM (1900) TO (2600);
    INSERT INTO dba_test.rwl_skip_follower VALUES (1800);

    PERFORM dba.partition_realign_with_leader('dba_test', 'rwl_skip_follower', 'rwl_leader', FALSE);

    -- Bridge upper must be 3000 (aligned_lower=2000, gap=100 < 333 → skip one slot)
    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
            '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_bridge_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    WHERE parent.relname = 'rwl_skip_follower'
      AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid),
           '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint = 1900;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(3000::bigint, v_bridge_upper, 'rwl_skip_rule_bridge_upper');

    -- Skip-rule bridge is still grid-aligned
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        0::bigint, (v_bridge_upper - v_grid_anchor) % v_grid_width,
        'rwl_skip_rule_bridge_on_leader_grid');

    -- ----------------------------------------------------------------
    -- Test 4: negative — leader not partitioned raises
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_plain_leader CASCADE';
    CREATE TABLE dba_test.rwl_plain_leader (col_id bigint NOT NULL PRIMARY KEY);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_realign_with_leader('dba_test', 'rwl_follower', 'rwl_plain_leader')$sql$,
        'P0001',
        'rwl_leader_not_partitioned_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_plain_leader CASCADE';

    -- ----------------------------------------------------------------
    -- Test 5: negative — follower not partitioned raises
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_plain_follower CASCADE';
    CREATE TABLE dba_test.rwl_plain_follower (col_id bigint NOT NULL PRIMARY KEY);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_realign_with_leader('dba_test', 'rwl_plain_follower', 'rwl_leader')$sql$,
        'P0001',
        'rwl_follower_not_partitioned_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_plain_follower CASCADE';

    -- ----------------------------------------------------------------
    -- Test 6: negative — leader has non-integer partition key raises
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_text_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_int_follower CASCADE';

    CREATE TABLE dba_test.rwl_text_leader (key_col text NOT NULL) PARTITION BY RANGE (key_col);
    CREATE TABLE dba_test.rwl_text_leader_a_m PARTITION OF dba_test.rwl_text_leader FOR VALUES FROM ('a') TO ('m');
    CREATE TABLE dba_test.rwl_text_leader_m_z PARTITION OF dba_test.rwl_text_leader FOR VALUES FROM ('m') TO ('z');

    CREATE TABLE dba_test.rwl_int_follower (col_id bigint NOT NULL, PRIMARY KEY (col_id))
        PARTITION BY RANGE (col_id);
    CREATE TABLE dba_test.rwl_int_follower_0_1000 PARTITION OF dba_test.rwl_int_follower FOR VALUES FROM (0) TO (1000);
    INSERT INTO dba_test.rwl_int_follower VALUES (500);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_realign_with_leader('dba_test', 'rwl_int_follower', 'rwl_text_leader')$sql$,
        'P0001',
        'rwl_leader_non_integer_key_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_text_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_int_follower CASCADE';

    -- ----------------------------------------------------------------
    -- Cleanup
    -- ----------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_leader CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_follower CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rwl_skip_follower CASCADE';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test';

    RETURN;
END;
$$;
