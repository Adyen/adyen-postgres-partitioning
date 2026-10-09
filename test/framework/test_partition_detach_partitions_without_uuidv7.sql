/*
Test: test_partition_detach_partitions_without_uuidv7
Function under test: dba.partition_detach_partitions_without_uuidv7
Run: ./test/framework/run_partition_tests.sh test_partition_detach_partitions_without_uuidv7
Purpose: Detach eligible partitions for date/timestamp range tables without UUIDv7 support.
Test coverage: Creates date-partitioned fixtures with a detach interval and verifies that old partitions are detached and recorded.
*/

CREATE OR REPLACE PROCEDURE dba_test.detach_partitions_without_uuidv7_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_date2 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case2 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case2_20200101_20200201 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case2_20990101_20990201 CASCADE';
    DELETE FROM dba.partition_configuration WHERE table_name = 'detach_date2' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'detach_case2' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_date2' AND schema = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_case2' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_date2 (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_date2_20200101_20200201 PARTITION OF dba_test.detach_date2 FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_date2_20990101_20990201 PARTITION OF dba_test.detach_date2 FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    -- A mixed-case table_name in dba.partition_configuration must still match the lower-cased catalog name
    CREATE TABLE dba_test.detach_case2 (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_case2_20200101_20200201 PARTITION OF dba_test.detach_case2 FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_case2_20990101_20990201 PARTITION OF dba_test.detach_case2 FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','detach_date2','{"detach":"365 days"}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','DETACH_CASE2','{"detach":"365 days"}');

    CALL dba.partition_detach_partitions_without_uuidv7();

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_date2' AND schema = 'dba_test' AND partition_relname = 'detach_date2_20200101_20200201' INTO v_count;
    PERFORM dba_test.record_result('detach_partitions_without_uuidv7_date', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_case2' AND schema = 'dba_test' AND partition_relname = 'detach_case2_20200101_20200201' INTO v_count;
    PERFORM dba_test.record_result('detach_partitions_without_uuidv7_mixed_case', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);
END;
$$;
