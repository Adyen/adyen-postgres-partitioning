/*
Test: test_partition_detach_partition
Function under test: dba.partition_detach_partition
Run: ./test/framework/run_partition_tests.sh test_partition_detach_partition
Purpose: Detach a single partition from its parent and record it in dba.detached_partitions.
Test coverage: Detaches oldest partition successfully, verifies bookkeeping, and checks non-oldest behavior with/without override.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_detach_partition()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int_0_10 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int_10_20 CASCADE';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_int' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_int (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.detach_int_0_10 PARTITION OF dba_test.detach_int FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.detach_int_10_20 PARTITION OF dba_test.detach_int FOR VALUES FROM (10) TO (20);

    INSERT INTO dba_test.detach_int VALUES (5, 'a');

    PERFORM dba.partition_detach_partition('dba_test','detach_int','detach_int_0_10');

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_int' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'detach_partition_oldest');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int_0_10 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_int_10_20 CASCADE';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_int' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_int (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.detach_int_0_10 PARTITION OF dba_test.detach_int FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.detach_int_10_20 PARTITION OF dba_test.detach_int FOR VALUES FROM (10) TO (20);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_detach_partition(''dba_test'',''detach_int'',''detach_int_10_20'', true)',
        'P0001',
        'detach_partition_non_oldest_raises'
    );

    PERFORM dba.partition_detach_partition('dba_test','detach_int','detach_int_10_20', false);
    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_int' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'detach_partition_non_oldest');

    RETURN;
END;
$$;
