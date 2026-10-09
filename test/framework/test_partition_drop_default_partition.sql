/*
Test: test_partition_drop_default_partition
Function under test: dba.partition_drop_default_partition
Run: ./test/framework/run_partition_tests.sh test_partition_drop_default_partition
Purpose: Detach and drop an empty default partition.
Test coverage: Creates a default partition and verifies it is removed.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_drop_default_partition()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.def_part CASCADE';

    CREATE TABLE dba_test.def_part (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.def_part_0_10 PARTITION OF dba_test.def_part FOR VALUES FROM (0) TO (10);
    CREATE TABLE dba_test.def_part_default PARTITION OF dba_test.def_part DEFAULT;

    PERFORM dba.partition_drop_default_partition('dba_test','def_part');

    SELECT count(*) FROM pg_class WHERE relname = 'def_part_default' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_exists, 'drop_default_partition');

    RETURN;
END;
$$;
