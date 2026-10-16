/*
Test: test_partition_query_based_detach_partitions
Function under test: dba.partition_query_based_detach_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_query_based_detach_partitions
Purpose: Detach partitions based on the result of a custom query per partition.
Test coverage: Uses a schema-qualified query placeholder and verifies the partition is detached.
*/

CREATE OR REPLACE PROCEDURE dba_test.query_based_detach_partitions_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.qdetach CASCADE';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'qdetach' AND schema = 'dba_test';

    CREATE TABLE dba_test.qdetach (id bigint not null, trip_date date not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.qdetach_0_10 PARTITION OF dba_test.qdetach FOR VALUES FROM (0) TO (10);

    INSERT INTO dba_test.qdetach VALUES (1, current_date - 1000);

    CALL dba.partition_query_based_detach_partitions(
        'dba_test',
        'qdetach',
        'select min(trip_date) from dba_test.<<partition>>',
        '365 days'
    );

    SELECT count(*) FROM dba.detached_partitions
     WHERE parent_relname = 'qdetach' AND schema = 'dba_test' AND partition_relname = 'qdetach_0_10'
      INTO v_count;

    PERFORM dba_test.record_result(
        'query_based_detach_partitions',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        NULL
    );
END;
$$;
