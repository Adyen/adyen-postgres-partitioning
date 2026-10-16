/*
Test: test_partition_query_based_maintenance_detach_partitions
Function under test: dba.partition_query_based_maintenance_detach_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_query_based_maintenance_detach_partitions
Purpose: Maintenance wrapper that executes query-based detaches from configuration.
Test coverage: Creates configuration and runs the wrapper to detach a partition and validate bookkeeping.
*/

CREATE OR REPLACE PROCEDURE dba_test.query_based_maintenance_detach_partitions_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.qmaint CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.qmaint_case CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.qmaint_case_0_10 CASCADE';
    DELETE FROM dba.partition_configuration WHERE table_name = 'qmaint' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'qmaint_case' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'qmaint' AND schema = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'qmaint_case' AND schema = 'dba_test';

    CREATE TABLE dba_test.qmaint (id bigint not null, trip_date date not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.qmaint_0_10 PARTITION OF dba_test.qmaint FOR VALUES FROM (0) TO (10);

    INSERT INTO dba_test.qmaint VALUES (1, current_date - 1000);

    -- A mixed-case table_name in dba.partition_configuration must still match the lower-cased catalog name
    CREATE TABLE dba_test.qmaint_case (id bigint not null, trip_date date not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.qmaint_case_0_10 PARTITION OF dba_test.qmaint_case FOR VALUES FROM (0) TO (10);

    INSERT INTO dba_test.qmaint_case VALUES (1, current_date - 1000);

    INSERT INTO dba.partition_configuration VALUES (
        'dba_test',
        'qmaint',
        '{"detach_query":"select min(trip_date) from dba_test.<<partition>>","detach":"365 days"}'
    );

    INSERT INTO dba.partition_configuration VALUES (
        'dba_test',
        'QMAINT_CASE',
        '{"detach_query":"select min(trip_date) from dba_test.<<partition>>","detach":"365 days"}'
    );

    CALL dba.partition_query_based_maintenance_detach_partitions();

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'qmaint' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result(
        'query_based_maintenance_detach',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        NULL
    );

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'qmaint_case' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result(
        'query_based_maintenance_detach_mixed_case',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END,
        NULL
    );
END;
$$;
