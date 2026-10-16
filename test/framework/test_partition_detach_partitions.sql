/*
Test: test_partition_detach_partitions
Function under test: dba.partition_detach_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_detach_partitions
Purpose: Detach eligible partitions for date/timestamp/uuid range tables based on configuration.
Test coverage: A date-partitioned fixture (including a mixed-case table_name configuration entry) and a
UUIDv7-partitioned fixture, run from detach_partitions_date_exec because the procedure commits.
*/

CREATE OR REPLACE PROCEDURE dba_test.test_partition_detach_partitions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_uuid CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case_20200101_20200201 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case_20990101_20990201 CASCADE';
    DELETE FROM dba.partition_configuration WHERE table_name = 'detach_date' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE table_name = 'detach_uuid' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'detach_case' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_date' AND schema = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_case' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_date (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_date_20200101_20200201 PARTITION OF dba_test.detach_date FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_date_20990101_20990201 PARTITION OF dba_test.detach_date FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    -- A mixed-case table_name in dba.partition_configuration must still match the lower-cased catalog name
    CREATE TABLE dba_test.detach_case (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_case_20200101_20200201 PARTITION OF dba_test.detach_case FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_case_20990101_20990201 PARTITION OF dba_test.detach_case FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','detach_date','{"detach":"365 days"}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','DETACH_CASE','{"detach":"365 days"}');
    -- partition_detach_partitions commits, which is not allowed when the test runs through
    -- run_test/run_all_tests (EXECUTE 'CALL ...' is atomic). The runner calls detach_partitions_date_exec
    -- at the top level instead.
    PERFORM dba_test.record_result('detach_partitions_date', 'SKIP', 'covered by detach_partitions_date_exec');
    PERFORM dba_test.record_result('detach_partitions_uuid', 'SKIP', 'covered by detach_partitions_date_exec');
END;
$$;

CREATE OR REPLACE PROCEDURE dba_test.detach_partitions_date_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_date_20200101_20200201 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_date_20990101_20990201 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case_20200101_20200201 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_case_20990101_20990201 CASCADE';
    DELETE FROM dba.partition_configuration WHERE table_name = 'detach_date' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'detach_case' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_date' AND schema = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_case' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_date (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_date_20200101_20200201 PARTITION OF dba_test.detach_date FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_date_20990101_20990201 PARTITION OF dba_test.detach_date FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    -- A mixed-case table_name in dba.partition_configuration must still match the lower-cased catalog name
    CREATE TABLE dba_test.detach_case (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.detach_case_20200101_20200201 PARTITION OF dba_test.detach_case FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.detach_case_20990101_20990201 PARTITION OF dba_test.detach_case FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','detach_date','{"detach":"365 days"}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','DETACH_CASE','{"detach":"365 days"}');

    CALL dba.partition_detach_partitions();

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_date' INTO v_count;
    PERFORM dba_test.record_result('detach_partitions_date', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_case' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result('detach_partitions_mixed_case', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    EXECUTE 'DROP TABLE IF EXISTS dba_test.detach_uuid CASCADE';
    DELETE FROM dba.partition_configuration WHERE table_name = 'detach_uuid' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'detach_uuid' AND schema = 'dba_test';

    CREATE TABLE dba_test.detach_uuid (id uuid not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.detach_uuid_20200101_20200201 PARTITION OF dba_test.detach_uuid
        FOR VALUES FROM ('00000000-0000-7000-8000-000000000000') TO ('00000000-0000-7000-8000-000000000100');
    CREATE TABLE dba_test.detach_uuid_20990101_20990201 PARTITION OF dba_test.detach_uuid
        FOR VALUES FROM ('ffffffff-ffff-7fff-8fff-fffffffffff0') TO ('ffffffff-ffff-7fff-8fff-ffffffffffff');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','detach_uuid','{"detach":"365 days"}');

    CALL dba.partition_detach_partitions();

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'detach_uuid' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result('detach_partitions_uuid', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);
END;
$$;
