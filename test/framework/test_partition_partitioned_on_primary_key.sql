/*
Test: test_partition_partitioned_on_primary_key
Function under test: dba.partition_partitioned_on_primary_key
Run: ./test/framework/run_partition_tests.sh test_partition_partitioned_on_primary_key
Purpose: Check if a table is partitioned on a primary key column.
Test coverage: Verifies true for PK-partitioned table and false for non-PK partitioned table.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_partitioned_on_primary_key()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result boolean;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pk_part CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.npk_part CASCADE';

    CREATE TABLE dba_test.pk_part (id bigint not null, val text, PRIMARY KEY (id)) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.pk_part_0_10 PARTITION OF dba_test.pk_part FOR VALUES FROM (0) TO (10);

    SELECT dba.partition_partitioned_on_primary_key('dba_test','pk_part') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result, 'partitioned_on_pk_true');

    CREATE TABLE dba_test.npk_part (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.npk_part_0_10 PARTITION OF dba_test.npk_part FOR VALUES FROM (0) TO (10);

    SELECT dba.partition_partitioned_on_primary_key('dba_test','npk_part') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_true(NOT v_result, 'partitioned_on_pk_false');

    RETURN;
END;
$$;
