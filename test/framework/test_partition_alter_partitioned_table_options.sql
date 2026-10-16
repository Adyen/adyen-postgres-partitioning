/*
Test: test_partition_alter_partitioned_table_options
Function under test: dba.partition_alter_partitioned_table_options
Run: ./test/framework/run_partition_tests.sh test_partition_alter_partitioned_table_options
Purpose: Generate ALTER statements for all partitions of a table.
Test coverage: Ensures statements are generated for all children.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_alter_partitioned_table_options()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.alt_part CASCADE';

    CREATE TABLE dba_test.alt_part (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.alt_part_0_10 PARTITION OF dba_test.alt_part FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.alt_part_10_20 PARTITION OF dba_test.alt_part FOR VALUES FROM (10) TO (20);

    SELECT count(*) FROM dba.partition_alter_partitioned_table_options('dba_test','alt_part','SET (fillfactor=70)') INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'alter_partitioned_table_options');

    RETURN;
END;
$$;
