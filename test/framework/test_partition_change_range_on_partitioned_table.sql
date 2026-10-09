/*
Test: test_partition_change_range_on_partitioned_table
Function under test: dba.partition_change_range_on_partitioned_table
Run: ./test/framework/run_partition_tests.sh test_partition_change_range_on_partitioned_table
Purpose: Replace empty partitions with new ranges for a table.
Test coverage: Creates integer/date/timestamp fixtures, inserts data in the first partition, runs the change-range function, and verifies partition removal plus new range boundaries. Mixed-case arguments are covered as well.
*/

CREATE OR REPLACE PROCEDURE dba_test.test_partition_change_range_on_partitioned_table()
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists int;
    v_lower bigint;
    v_upper bigint;
    v_prev_upper bigint;
    v_date_lower date;
    v_date_upper date;
    v_prev_date_upper date;
    v_ts_lower timestamptz;
    v_ts_upper timestamptz;
    v_prev_ts_upper timestamptz;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.range_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.range_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.range_ts CASCADE';

    CREATE TABLE dba_test.range_int (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.range_int_0_10 PARTITION OF dba_test.range_int FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.range_int_10_20 PARTITION OF dba_test.range_int FOR VALUES FROM (10) TO (20);
    CREATE TABLE dba_test.range_int_20_30 PARTITION OF dba_test.range_int FOR VALUES FROM (20) TO (30);

    UPDATE pg_attribute SET attoptions = '{}' WHERE attrelid = 'dba_test.range_int_0_10'::regclass;

    INSERT INTO dba_test.range_int VALUES (1);
    INSERT INTO dba_test.range_int VALUES (21);
    EXECUTE 'ANALYZE dba_test.range_int_0_10';
    EXECUTE 'ANALYZE dba_test.range_int_10_20';
    EXECUTE 'ANALYZE dba_test.range_int_20_30';

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_prev_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint DESC
    LIMIT 1;

    PERFORM dba.partition_change_range_on_partitioned_table('dba_test','range_int','50', 0, false, true);

    SELECT count(*) FROM pg_class WHERE relname = 'range_int_10_20' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    PERFORM dba_test.record_result('change_range_int_removed', CASE WHEN v_exists = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint,
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_lower, v_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint DESC
    LIMIT 1;

    PERFORM dba_test.record_result('change_range_int_new_partition', CASE WHEN v_lower = 30 THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_int_interval', CASE WHEN (v_upper - v_lower) = 50 THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_int_boundary_matches', CASE WHEN v_lower = v_prev_upper THEN 'PASS' ELSE 'FAIL' END, NULL);

    CREATE TABLE dba_test.range_date (id bigint, record_date date not null) PARTITION BY RANGE (record_date);
    CREATE TABLE dba_test.range_date_20240101_20240111 PARTITION OF dba_test.range_date FOR VALUES FROM ('2024-01-01') TO ('2024-01-11');
    CREATE TABLE dba_test.range_date_20240111_20240121 PARTITION OF dba_test.range_date FOR VALUES FROM ('2024-01-11') TO ('2024-01-21');
    CREATE TABLE dba_test.range_date_20240121_20240131 PARTITION OF dba_test.range_date FOR VALUES FROM ('2024-01-21') TO ('2024-01-31');

    UPDATE pg_attribute SET attoptions = '{}' WHERE attrelid = 'dba_test.range_date_20240101_20240111'::regclass;

    INSERT INTO dba_test.range_date VALUES (1, '2024-01-05');
    INSERT INTO dba_test.range_date VALUES (2, '2024-01-25');
    EXECUTE 'ANALYZE dba_test.range_date_20240101_20240111';
    EXECUTE 'ANALYZE dba_test.range_date_20240111_20240121';
    EXECUTE 'ANALYZE dba_test.range_date_20240121_20240131';

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::date
    INTO v_prev_date_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date DESC
    LIMIT 1;

    PERFORM dba.partition_change_range_on_partitioned_table('dba_test','range_date','10 days', 0, false, true);

    SELECT count(*) FROM pg_class WHERE relname = 'range_date_20240111_20240121' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    PERFORM dba_test.record_result('change_range_date_removed', CASE WHEN v_exists = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date,
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::date
    INTO v_date_lower, v_date_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date DESC
    LIMIT 1;

    PERFORM dba_test.record_result('change_range_date_new_partition', CASE WHEN v_date_lower = '2024-01-31' THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_date_interval', CASE WHEN (v_date_upper - v_date_lower) = 10 THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_date_boundary_matches', CASE WHEN v_date_lower = v_prev_date_upper THEN 'PASS' ELSE 'FAIL' END, NULL);

    CREATE TABLE dba_test.range_ts (id bigint, record_ts timestamptz not null) PARTITION BY RANGE (record_ts);
    CREATE TABLE dba_test.range_ts_20240101_20240102 PARTITION OF dba_test.range_ts FOR VALUES FROM ('2024-01-01 00:00:00+00') TO ('2024-01-02 00:00:00+00');
    CREATE TABLE dba_test.range_ts_20240102_20240103 PARTITION OF dba_test.range_ts FOR VALUES FROM ('2024-01-02 00:00:00+00') TO ('2024-01-03 00:00:00+00');
    CREATE TABLE dba_test.range_ts_20240103_20240104 PARTITION OF dba_test.range_ts FOR VALUES FROM ('2024-01-03 00:00:00+00') TO ('2024-01-04 00:00:00+00');

    UPDATE pg_attribute SET attoptions = '{}' WHERE attrelid = 'dba_test.range_ts_20240101_20240102'::regclass;

    INSERT INTO dba_test.range_ts VALUES (1, '2024-01-01 12:00:00+00');
    INSERT INTO dba_test.range_ts VALUES (2, '2024-01-03 12:00:00+00');
    EXECUTE 'ANALYZE dba_test.range_ts_20240101_20240102';
    EXECUTE 'ANALYZE dba_test.range_ts_20240102_20240103';
    EXECUTE 'ANALYZE dba_test.range_ts_20240103_20240104';

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::timestamptz
    INTO v_prev_ts_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz DESC
    LIMIT 1;

    PERFORM dba.partition_change_range_on_partitioned_table('dba_test','range_ts','1 day', 0, false, true);

    SELECT count(*) FROM pg_class WHERE relname = 'range_ts_20240102_20240103' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    PERFORM dba_test.record_result('change_range_ts_removed', CASE WHEN v_exists = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz,
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::timestamptz
    INTO v_ts_lower, v_ts_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'range_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz DESC
    LIMIT 1;

    PERFORM dba_test.record_result('change_range_ts_new_partition', CASE WHEN v_ts_lower = '2024-01-04 00:00:00+00'::timestamptz THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_ts_interval', CASE WHEN (v_ts_upper - v_ts_lower) = interval '1 day' THEN 'PASS' ELSE 'FAIL' END, NULL);
    PERFORM dba_test.record_result('change_range_ts_boundary_matches', CASE WHEN v_ts_lower = v_prev_ts_upper THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- Mixed-case arguments must be handled case-insensitively: the empty partition
    -- is detached and replaced with a new range on the same table.
    EXECUTE 'DROP TABLE IF EXISTS dba_test.range_case CASCADE';

    CREATE TABLE dba_test.range_case (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.range_case_0_10 PARTITION OF dba_test.range_case FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.range_case_10_20 PARTITION OF dba_test.range_case FOR VALUES FROM (10) TO (20);

    UPDATE pg_attribute SET attoptions = '{}' WHERE attrelid = 'dba_test.range_case_0_10'::regclass;

    INSERT INTO dba_test.range_case VALUES (1);
    EXECUTE 'ANALYZE dba_test.range_case_0_10';
    EXECUTE 'ANALYZE dba_test.range_case_10_20';

    BEGIN
        PERFORM dba.partition_change_range_on_partitioned_table('DBA_TEST','RANGE_CASE','50', 0, false, true);

        SELECT count(*) FROM pg_class WHERE relname = 'range_case_10_20' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
        PERFORM dba_test.record_result('change_range_mixed_case_removed', CASE WHEN v_exists = 0 THEN 'PASS' ELSE 'FAIL' END, NULL);

        SELECT count(*) FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = 'range_case' AND parent.relnamespace = 'dba_test'::regnamespace
          AND child.relname = 'range_case_10_60'
        INTO v_exists;
        PERFORM dba_test.record_result('change_range_mixed_case_new_partition', CASE WHEN v_exists = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('change_range_mixed_case_removed', 'FAIL', SQLERRM);
        PERFORM dba_test.record_result('change_range_mixed_case_new_partition', 'FAIL', SQLERRM);
    END;
END;
$$;
