/*
Test: test_partition_report_table_free_extends_below_threshold
Function under test: dba.partition_report_table_free_extends_below_threshold
Run: ./test/framework/run_partition_tests.sh test_partition_report_table_free_extends_below_threshold
Purpose: Write a report for tables below the free-partition threshold.
Test coverage: Runs report generation on a test partitioned table.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_report_table_free_extends_below_threshold()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    -- Random name, so nobody can plant a file or symlink at the path in advance.
    v_path text := '/tmp/partition_report_' || md5(random()::text || clock_timestamp()::text) || '.csv';
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.rep_part CASCADE';

    CREATE TABLE dba_test.rep_part (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.rep_part_0_10 PARTITION OF dba_test.rep_part FOR VALUES FROM (0) TO (10);

    PERFORM dba.partition_report_table_free_extends_below_threshold(3, v_path);

    EXECUTE 'DROP TABLE IF EXISTS tmp_report';
    CREATE TEMP TABLE tmp_report(schema text, relname text, free_extends int);
    EXECUTE format('COPY tmp_report FROM %L CSV HEADER', v_path);
    EXECUTE format('COPY (SELECT 1 WHERE false) TO %L', v_path);

    SELECT count(*) FROM tmp_report WHERE schema = 'dba_test' AND relname = 'rep_part' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'report_free_extends_below_threshold');

    RETURN;
END;
$$;
