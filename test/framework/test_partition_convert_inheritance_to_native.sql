/*
Test: test_partition_convert_inheritance_to_native
Function under test: dba.partition_convert_inheritance_to_native
Run: ./test/framework/run_partition_tests.sh test_partition_convert_inheritance_to_native
Purpose: Convert an inheritance-partitioned table to native partitioning.
Test coverage: Creates an inheritance fixture and verifies native partitioning exists.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_convert_inheritance_to_native()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.conv_table CASCADE';

    CREATE TABLE dba_test.conv_table (id bigint not null);
    CREATE TABLE dba_test.conv_table_0_10 (CHECK (id >= 0 AND id < 10)) INHERITS (dba_test.conv_table);
    INSERT INTO dba_test.conv_table_0_10 VALUES (1);

    PERFORM dba.partition_convert_inheritance_to_native('dba_test','conv_table','id','10');

    SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'conv_table' and n.nspname = 'dba_test' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'convert_inheritance_to_native');

    RETURN;
END;
$$;
