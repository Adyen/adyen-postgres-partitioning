/*
Test: test_partition_calculate_free_partitions
Function under test: dba.partition_calculate_free_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_calculate_free_partitions
Purpose: Calculate the number of free (unused) partitions for a partitioned table.
Test coverage: Creates integer/date/timestamp and multi-range fixtures, exercises free counts, unsupported types, non-partitioned errors, mixed-case arguments, and skips UUIDv7 when unavailable.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_calculate_free_partitions()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_range_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_text CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_multi CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_quoted CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_quoted_collision CASCADE';

    CREATE TABLE dba_test.calc_int (id bigint not null, record_date date) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.calc_int_0_10 PARTITION OF dba_test.calc_int FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.calc_int_10_20 PARTITION OF dba_test.calc_int FOR VALUES FROM (10) TO (20);
    CREATE TABLE dba_test.calc_int_20_30 PARTITION OF dba_test.calc_int FOR VALUES FROM (20) TO (30);

    INSERT INTO dba_test.calc_int VALUES (5, current_date);
    SELECT dba.partition_calculate_free_partitions('dba_test','calc_int') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'calc_free_int_partially_filled');

    CREATE TABLE dba_test.calc_quoted ("PartitionId" bigint not null) PARTITION BY RANGE ("PartitionId");
    CREATE TABLE dba_test.calc_quoted_0_10 PARTITION OF dba_test.calc_quoted FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.calc_quoted_10_20 PARTITION OF dba_test.calc_quoted FOR VALUES FROM (10) TO (20);
    INSERT INTO dba_test.calc_quoted ("PartitionId") VALUES (5);
    SELECT dba.partition_calculate_free_partitions('dba_test', 'calc_quoted') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'calc_free_quoted_column');

    CREATE TABLE dba_test.calc_quoted_collision (partitionid bigint, "PartitionId" bigint not null)
        PARTITION BY RANGE ("PartitionId");
    CREATE TABLE dba_test.calc_quoted_collision_0_10
        PARTITION OF dba_test.calc_quoted_collision FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.calc_quoted_collision_10_20
        PARTITION OF dba_test.calc_quoted_collision FOR VALUES FROM (10) TO (20);
    INSERT INTO dba_test.calc_quoted_collision (partitionid, "PartitionId") VALUES (15, 5);
    SELECT dba.partition_calculate_free_partitions(
        'dba_test', 'calc_quoted_collision', 'PartitionId', 'int8'
    ) INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'calc_free_quoted_column_exact_match');

    BEGIN
        SELECT dba.partition_calculate_free_partitions('DBA_TEST','CALC_INT') INTO v_count;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'calc_free_int_mixed_case');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('calc_free_int_mixed_case', 'FAIL', SQLERRM);
    END;

    INSERT INTO dba_test.calc_int VALUES (25, current_date);
    SELECT dba.partition_calculate_free_partitions('dba_test','calc_int') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'calc_free_int_full');

    CREATE TABLE dba_test.calc_date (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.calc_date_past PARTITION OF dba_test.calc_date FOR VALUES FROM (current_date - 10) TO (current_date - 5);
    CREATE TABLE dba_test.calc_date_future PARTITION OF dba_test.calc_date FOR VALUES FROM (current_date + 1) TO (current_date + 10);
    SELECT dba.partition_calculate_free_partitions('dba_test','calc_date') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'calc_free_date_future');

    CREATE TABLE dba_test.calc_ts (id bigint, trip_ts timestamptz not null) PARTITION BY RANGE (trip_ts);
    CREATE TABLE dba_test.calc_ts_past PARTITION OF dba_test.calc_ts FOR VALUES FROM (current_timestamp - interval '10 days') TO (current_timestamp - interval '5 days');
    CREATE TABLE dba_test.calc_ts_future PARTITION OF dba_test.calc_ts FOR VALUES FROM (current_timestamp + interval '1 day') TO (current_timestamp + interval '10 days');
    SELECT dba.partition_calculate_free_partitions('dba_test','calc_ts') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'calc_free_ts_future');

    CREATE TABLE dba_test.calc_multi (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.calc_multi_r1_0_10 PARTITION OF dba_test.calc_multi FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.calc_multi_r1_10_20 PARTITION OF dba_test.calc_multi FOR VALUES FROM (10) TO (20);
    CREATE TABLE dba_test.calc_multi_r2_100_110 PARTITION OF dba_test.calc_multi FOR VALUES FROM (100) TO (110);
    CREATE TABLE dba_test.calc_multi_r2_110_120 PARTITION OF dba_test.calc_multi FOR VALUES FROM (110) TO (120);

    INSERT INTO dba_test.calc_multi VALUES (5, 'a');
    SELECT dba.partition_calculate_free_partitions('dba_test','calc_multi') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'calc_free_multi_min_range');

    SELECT dba.partition_calculate_free_partitions('dba_test','calc_multi', 'id', 'int8', 'r1') INTO v_range_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_range_count, 'calc_free_multi_r1');

    BEGIN
        SELECT dba.partition_calculate_free_partitions('DBA_TEST','CALC_MULTI','ID','INT8','R1') INTO v_range_count;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_range_count, 'calc_free_multi_r1_mixed_case');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('calc_free_multi_r1_mixed_case', 'FAIL', SQLERRM);
    END;

    CREATE TABLE dba_test.calc_text (id text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.calc_text_a PARTITION OF dba_test.calc_text FOR VALUES FROM ('a') TO ('m');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_calculate_free_partitions(''dba_test'',''calc_text'')',
        'Z1001',
        'calc_free_unsupported_type_raises'
    );

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_calculate_free_partitions(''dba_test'',''non_partitioned'')',
        'Z1002',
        'calc_free_non_partitioned_raises'
    );

    IF NOT dba_test.uuidv7_supported() THEN
        RETURN QUERY SELECT * FROM dba_test.skip('calc_free_uuid_skip', 'no uuidv7 support');
    END IF;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_text CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_multi CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_quoted CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.calc_quoted_collision CASCADE';

    RETURN;
END;
$$;
