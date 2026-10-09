/*
Test: test_partition_add_concurrent_index_on_partitioned_table
Function under test: dba.partition_add_concurrent_index_on_partitioned_table
Run: ./test/framework/run_partition_tests.sh test_partition_add_concurrent_index_on_partitioned_table
Purpose: Generate concurrent index creation statements for all partitions.
Test coverage: Asserts that at least one statement is produced for a partitioned table.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_add_concurrent_index_on_partitioned_table()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cidx_part CASCADE';

    CREATE TABLE dba_test.cidx_part (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.cidx_part_0_10 PARTITION OF dba_test.cidx_part FOR VALUES FROM (0) TO (10);

    SELECT count(*) FROM dba.partition_add_concurrent_index_on_partitioned_table('dba_test','cidx_part', ARRAY['val']) INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 1, 'add_concurrent_index_generates_statements');

    RETURN;
END;
$$;
