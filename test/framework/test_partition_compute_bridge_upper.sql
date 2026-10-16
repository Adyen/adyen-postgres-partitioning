/*
Test: test_partition_compute_bridge_upper
Function under test: dba.partition_compute_bridge_upper
Run: ./test/framework/run_partition_tests.sh test_partition_compute_bridge_upper
Purpose: Verify bridge upper bound snapping and the bridge-skip rule (gap < P/3 → extend one slot).
Test coverage:
  - gap >= P/3: returns aligned_lower as-is
  - gap < P/3: extends by one grid slot (bridge-skip rule)
  - switch exactly on a grid boundary: gap = 0, always triggers bridge-skip
  - switch just above a grid boundary: gap near P, no skip
  - switch below anchor: snaps up to anchor (ceil of negative fraction = 0 multiplier)
  - raises when grid_width <= 0
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_compute_bridge_upper()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
BEGIN
    -- ----------------------------------------------------------------
    -- Test 1: gap 4300 >= P/3 3333 — return aligned_lower = 20000
    --   switch=15700, anchor=20000, width=10000
    --   ceil((15700-20000)/10000) = ceil(-0.43) = 0 → aligned = 20000
    --   gap = 20000 - 15700 = 4300 >= 3333 → no skip
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        20000::bigint,
        dba.partition_compute_bridge_upper(15700, 20000, 10000),
        'cbu_gap_above_threshold_no_skip'
    );

    -- ----------------------------------------------------------------
    -- Test 2: gap 500 < P/3 3333 — extend one slot → 30000
    --   switch=19500, anchor=20000, width=10000
    --   ceil((19500-20000)/10000) = ceil(-0.05) = 0 → aligned = 20000
    --   gap = 500 < 3333 → skip → return 30000
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        30000::bigint,
        dba.partition_compute_bridge_upper(19500, 20000, 10000),
        'cbu_gap_below_threshold_skip'
    );

    -- ----------------------------------------------------------------
    -- Test 3: switch exactly on grid boundary — gap = 0 → always skip
    --   switch=20000, anchor=20000, width=10000
    --   ceil(0/10000) = 0 → aligned = 20000
    --   gap = 0 < 3333 → skip → return 30000
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        30000::bigint,
        dba.partition_compute_bridge_upper(20000, 20000, 10000),
        'cbu_on_grid_boundary_always_skip'
    );

    -- ----------------------------------------------------------------
    -- Test 4: switch just above a boundary — large gap, no skip
    --   switch=20001, anchor=20000, width=10000
    --   ceil((20001-20000)/10000) = ceil(0.0001) = 1 → aligned = 30000
    --   gap = 30000 - 20001 = 9999 >= 3333 → no skip → return 30000
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        30000::bigint,
        dba.partition_compute_bridge_upper(20001, 20000, 10000),
        'cbu_just_above_boundary_no_skip'
    );

    -- ----------------------------------------------------------------
    -- Test 5: switch < anchor — snaps up to anchor (no skip)
    --   switch=5000, anchor=10000, width=10000
    --   ceil((5000-10000)/10000) = ceil(-0.5) = 0 → aligned = 10000
    --   gap = 10000 - 5000 = 5000 >= 3333 → no skip → return 10000
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        10000::bigint,
        dba.partition_compute_bridge_upper(5000, 10000, 10000),
        'cbu_switch_below_anchor_snaps_to_anchor'
    );

    -- ----------------------------------------------------------------
    -- Test 6: negative — grid_width = 0 → raises
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_compute_bridge_upper(1000, 0, 0)$sql$,
        'P0001',
        'cbu_zero_width_raises'
    );

    -- ----------------------------------------------------------------
    -- Test 7: negative — grid_width < 0 → raises
    -- ----------------------------------------------------------------
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_compute_bridge_upper(1000, 0, -500)$sql$,
        'P0001',
        'cbu_negative_width_raises'
    );

    RETURN;
END;
$$;
