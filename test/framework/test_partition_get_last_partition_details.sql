/*
Test: test_partition_get_last_partition_details
Function under test: dba.partition_get_last_partition_details
Run: ./test/framework/run_partition_tests.sh test_partition_get_last_partition_details
Purpose: Return the last (latest) partition name and range for a table.
Test coverage: Creates range tables and verifies the latest partition is returned, including a multi-range table with mixed-case arguments and a range identifier.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_get_last_partition_details()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_name text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.last_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.last_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.last_ts CASCADE';
    CREATE TABLE dba_test.last_int (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.last_int_0_10 PARTITION OF dba_test.last_int FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.last_int_10_20 PARTITION OF dba_test.last_int FOR VALUES FROM (10) TO (20);

    SELECT v_childrelname FROM dba.partition_get_last_partition_details('dba_test','last_int') INTO v_name;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('last_int_10_20', v_name, 'get_last_partition_details');

    CREATE TABLE dba_test.last_date (id bigint, record_date date not null) PARTITION BY RANGE (record_date);
    CREATE TABLE dba_test.last_date_20240101_20240201 PARTITION OF dba_test.last_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE dba_test.last_date_20240201_20240301 PARTITION OF dba_test.last_date FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

    SELECT v_childrelname FROM dba.partition_get_last_partition_details('dba_test','last_date') INTO v_name;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('last_date_20240201_20240301', v_name, 'get_last_partition_details_date');

    CREATE TABLE dba_test.last_ts (id bigint, record_ts timestamptz not null) PARTITION BY RANGE (record_ts);
    CREATE TABLE dba_test.last_ts_20240101_20240201 PARTITION OF dba_test.last_ts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE dba_test.last_ts_20240201_20240301 PARTITION OF dba_test.last_ts FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

    SELECT v_childrelname FROM dba.partition_get_last_partition_details('dba_test','last_ts') INTO v_name;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('last_ts_20240201_20240301', v_name, 'get_last_partition_details_ts');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.last_multi CASCADE';
    CREATE TABLE dba_test.last_multi (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.last_multi_r1_0_10 PARTITION OF dba_test.last_multi FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.last_multi_r1_10_20 PARTITION OF dba_test.last_multi FOR VALUES FROM (10) TO (20);
    CREATE TABLE dba_test.last_multi_r2_100_110 PARTITION OF dba_test.last_multi FOR VALUES FROM (100) TO (110);

    SELECT v_childrelname FROM dba.partition_get_last_partition_details('DBA_TEST','Last_Multi') INTO v_name;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('last_multi_r2_100_110', v_name, 'get_last_partition_details_mixed_case');

    SELECT v_childrelname FROM dba.partition_get_last_partition_details('dba_test','LAST_MULTI','R1') INTO v_name;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('last_multi_r1_10_20', v_name, 'get_last_partition_details_range_mixed_case');

    RETURN;
END;
$$;
