/*
Test: test_partition_get_current_partition_boundaries
Function under test: dba.partition_get_current_partition_boundaries
Run: ./test/framework/run_partition_tests.sh test_partition_get_current_partition_boundaries
Purpose: Return the partition name and boundaries for the partition containing the max value of the partition column.
Test coverage: Creates integer/date/timestamp fixtures, verifies correct partition is returned for the max value,
               and validates error paths for empty tables, non-partitioned tables, and unsupported types.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_get_current_partition_boundaries()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_name text;
    v_lower_bound    text;
    v_upper_bound    text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_text CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_empty CASCADE';

    -- Integer partitioned table: three partitions, max value in the second
    CREATE TABLE dba_test.curpart_int (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.curpart_int_0_10   PARTITION OF dba_test.curpart_int FOR VALUES FROM (0)  TO (10);
    CREATE TABLE dba_test.curpart_int_10_20  PARTITION OF dba_test.curpart_int FOR VALUES FROM (10) TO (20);
    CREATE TABLE dba_test.curpart_int_20_30  PARTITION OF dba_test.curpart_int FOR VALUES FROM (20) TO (30);

    INSERT INTO dba_test.curpart_int VALUES (15);

    SELECT p.v_partition_name, p.v_lower_bound, p.v_upper_bound
    INTO v_partition_name, v_lower_bound, v_upper_bound
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_int') p;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('curpart_int_10_20', v_partition_name, 'curpart_int_partition_name');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('10', v_lower_bound, 'curpart_int_lower_bound');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('20', v_upper_bound, 'curpart_int_upper_bound');

    -- Max value in the last partition
    INSERT INTO dba_test.curpart_int VALUES (25);

    SELECT p.v_partition_name, p.v_lower_bound, p.v_upper_bound
    INTO v_partition_name, v_lower_bound, v_upper_bound
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_int') p;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('curpart_int_20_30', v_partition_name, 'curpart_int_last_partition_name');

    -- Date partitioned table
    CREATE TABLE dba_test.curpart_date (record_date date not null) PARTITION BY RANGE (record_date);
    CREATE TABLE dba_test.curpart_date_20240101_20240201 PARTITION OF dba_test.curpart_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE dba_test.curpart_date_20240201_20240301 PARTITION OF dba_test.curpart_date FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
    CREATE TABLE dba_test.curpart_date_20240301_20240401 PARTITION OF dba_test.curpart_date FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

    INSERT INTO dba_test.curpart_date VALUES ('2024-02-15');

    SELECT p.v_partition_name, p.v_lower_bound, p.v_upper_bound
    INTO v_partition_name, v_lower_bound, v_upper_bound
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_date') p;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('curpart_date_20240201_20240301', v_partition_name, 'curpart_date_partition_name');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2024-02-01', v_lower_bound, 'curpart_date_lower_bound');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2024-03-01', v_upper_bound, 'curpart_date_upper_bound');

    -- Timestamp partitioned table
    CREATE TABLE dba_test.curpart_ts (record_ts timestamp not null) PARTITION BY RANGE (record_ts);
    CREATE TABLE dba_test.curpart_ts_20240101_20240201 PARTITION OF dba_test.curpart_ts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE dba_test.curpart_ts_20240201_20240301 PARTITION OF dba_test.curpart_ts FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
    CREATE TABLE dba_test.curpart_ts_20240301_20240401 PARTITION OF dba_test.curpart_ts FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

    INSERT INTO dba_test.curpart_ts VALUES ('2024-03-10 12:00:00');

    SELECT p.v_partition_name, p.v_lower_bound, p.v_upper_bound
    INTO v_partition_name, v_lower_bound, v_upper_bound
    FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_ts') p;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('curpart_ts_20240301_20240401', v_partition_name, 'curpart_ts_partition_name');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2024-03-01 00:00:00', v_lower_bound, 'curpart_ts_lower_bound');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2024-04-01 00:00:00', v_upper_bound, 'curpart_ts_upper_bound');

    -- A partition (not a partitioned table) raises an exception
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT * FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_int_0_10')$sql$,
        '45002',
        'curpart_partition_not_partitioned_raises'
    );

    -- Empty partitioned table raises an exception
    CREATE TABLE dba_test.curpart_empty (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.curpart_empty_0_10 PARTITION OF dba_test.curpart_empty FOR VALUES FROM (0) TO (10);
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT * FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_empty')$sql$,
        '45003',
        'curpart_empty_table_raises'
    );

    -- Non-partitioned table raises an exception
    CREATE TABLE dba_test.curpart_plain (id bigint);
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT * FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_plain')$sql$,
        '45002',
        'curpart_non_partitioned_raises'
    );

    -- Unsupported column type raises an exception
    CREATE TABLE dba_test.curpart_text (id text not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.curpart_text_a PARTITION OF dba_test.curpart_text FOR VALUES FROM ('a') TO ('m');
    INSERT INTO dba_test.curpart_text VALUES ('b');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT * FROM dba.partition_get_current_partition_boundaries('dba_test', 'curpart_text')$sql$,
        'P0001',
        'curpart_unsupported_type_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_text CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_plain CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.curpart_empty CASCADE';

    RETURN;
END;
$$;
