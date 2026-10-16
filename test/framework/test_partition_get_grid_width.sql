/*
Test: test_partition_get_grid_width
Function under test: dba.partition_get_grid_width
Run: ./test/framework/run_partition_tests.sh test_partition_get_grid_width
Purpose: Verify that the grid width is computed as upper - lower of the last non-mammoth partition.
Test coverage:
  - single uniform partition width is returned correctly
  - last partition width is used when partitions have different sizes
  - raises when the table has no non-mammoth partitions
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_get_grid_width()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_uniform CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_unequal CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_empty CASCADE';

    -- Uniform partitions — width = 1000
    CREATE TABLE dba_test.ggw_uniform (id bigint NOT NULL) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.ggw_uniform_0_1000    PARTITION OF dba_test.ggw_uniform FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.ggw_uniform_1000_2000 PARTITION OF dba_test.ggw_uniform FOR VALUES FROM (1000) TO (2000);
    CREATE TABLE dba_test.ggw_uniform_2000_3000 PARTITION OF dba_test.ggw_uniform FOR VALUES FROM (2000) TO (3000);

    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        1000::bigint, dba.partition_get_grid_width('dba_test', 'ggw_uniform'),
        'ggw_uniform_width'
    );

    -- Unequal partitions — last one is [500,1500), width = 1000
    CREATE TABLE dba_test.ggw_unequal (id bigint NOT NULL) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.ggw_unequal_0_500   PARTITION OF dba_test.ggw_unequal FOR VALUES FROM (0)   TO (500);
    CREATE TABLE dba_test.ggw_unequal_500_1500 PARTITION OF dba_test.ggw_unequal FOR VALUES FROM (500) TO (1500);

    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        1000::bigint, dba.partition_get_grid_width('dba_test', 'ggw_unequal'),
        'ggw_last_partition_width_used'
    );

    -- No non-mammoth partitions — should raise
    CREATE TABLE dba_test.ggw_empty (id bigint NOT NULL) PARTITION BY RANGE (id);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_get_grid_width('dba_test', 'ggw_empty')$sql$,
        'P0001',
        'ggw_no_partitions_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_uniform CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_unequal CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ggw_empty CASCADE';

    RETURN;
END;
$$;
