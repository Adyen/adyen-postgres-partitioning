/*
Test: test_partition_drop_detached_partition
Function under test: dba.partition_drop_detached_partition
Run: ./test/framework/run_partition_tests.sh test_partition_drop_detached_partition
Purpose: Drop a detached partition and remove it from dba.detached_partitions.
Test coverage: Detaches a child, drops it, validates catalog cleanup, and asserts error paths.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_drop_detached_partition()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.drop_int CASCADE';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'drop_int' AND schema = 'dba_test';

    CREATE TABLE dba_test.drop_int (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.drop_int_0_10 PARTITION OF dba_test.drop_int FOR VALUES FROM (0) TO (10);

    PERFORM dba.partition_detach_partition('dba_test','drop_int','drop_int_0_10');
    PERFORM dba.partition_drop_detached_partition('dba_test','drop_int','drop_int_0_10');

    SELECT count(*) FROM pg_class WHERE relname = 'drop_int_0_10' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_exists, 'drop_detached_partition_dropped');

    INSERT INTO dba.detached_partitions VALUES ('dba_test','drop_int','missing_partition', ARRAY['0','10'], current_date);
    PERFORM dba.partition_drop_detached_partition('dba_test','drop_int','missing_partition');
    SELECT count(*) FROM dba.detached_partitions WHERE partition_relname = 'missing_partition' INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_exists, 'drop_detached_partition_missing_table');

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_drop_detached_partition(''dba_test'',''drop_int'',''drop_int'')',
        'P0001',
        'drop_detached_partition_not_found'
    );

    RETURN;
END;
$$;
