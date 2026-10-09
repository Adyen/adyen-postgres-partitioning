/*
Test: test_partition_report_table_free_extends_below_date_threshold
Function under test: dba.partition_report_table_free_extends_below_date_threshold
Run: ./test/framework/run_partition_tests.sh test_partition_report_table_free_extends_below_date_threshold
Purpose: Write a report for date/timestamp tables below the future-days threshold.
Test coverage: Runs report generation on a date-partitioned table.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_report_table_free_extends_below_date_threshold()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    -- Random name, so nobody can plant a file or symlink at the path in advance.
    v_path text := '/tmp/partition_report_' || md5(random()::text || clock_timestamp()::text) || '.csv';
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rep_date_part CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rep_ts_part CASCADE';

    CREATE TABLE dba_test.rep_date_part (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.rep_date_part_20200101_20200201 PARTITION OF dba_test.rep_date_part FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');

    CREATE TABLE dba_test.rep_ts_part (id bigint, trip_ts timestamptz not null) PARTITION BY RANGE (trip_ts);
    CREATE TABLE dba_test.rep_ts_part_20200101_20200201 PARTITION OF dba_test.rep_ts_part FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');

    PERFORM dba.partition_report_table_free_extends_below_date_threshold(7, v_path);

    DROP TABLE IF EXISTS pg_temp.tmp_report;
    CREATE TEMP TABLE tmp_report(schema text, relname text, days_to_go int);
    EXECUTE format('COPY tmp_report FROM %L CSV HEADER', v_path);
    EXECUTE format('COPY (SELECT 1 WHERE false) TO %L', v_path);

    SELECT count(*) FROM tmp_report WHERE schema = 'dba_test' AND relname = 'rep_date_part' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'report_free_extends_below_date_threshold_date');

    SELECT count(*) FROM tmp_report WHERE schema = 'dba_test' AND relname = 'rep_ts_part' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'report_free_extends_below_date_threshold_ts');

    RETURN;
END;
$$;
