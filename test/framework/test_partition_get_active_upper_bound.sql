/*
Test: test_partition_get_active_upper_bound
Function under test: dba.partition_get_active_upper_bound
Run: ./test/framework/run_partition_tests.sh test_partition_get_active_upper_bound
Purpose: Verify that the exclusive upper bound of the partition containing max(key) is returned.
Test coverage:
  - max value in first partition returns first partition's upper bound
  - max value in second partition returns second partition's upper bound
  - raises when table is not partitioned
  - raises when table is empty
  - raises when partition key is not an integer type
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_get_active_upper_bound()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_plain CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_empty CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_text_key CASCADE';

    CREATE TABLE dba_test.gaub_int (id bigint NOT NULL) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.gaub_int_0_1000    PARTITION OF dba_test.gaub_int FOR VALUES FROM (0)    TO (1000);
    CREATE TABLE dba_test.gaub_int_1000_2000 PARTITION OF dba_test.gaub_int FOR VALUES FROM (1000) TO (2000);
    CREATE TABLE dba_test.gaub_int_2000_3000 PARTITION OF dba_test.gaub_int FOR VALUES FROM (2000) TO (3000);

    -- ----------------------------------------------------------------
    -- Test 1: max in first partition → upper bound = 1000
    -- ----------------------------------------------------------------
    INSERT INTO dba_test.gaub_int VALUES (500);

    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        1000::bigint, dba.partition_get_active_upper_bound('dba_test', 'gaub_int'),
        'gaub_active_upper_first_partition'
    );

    -- ----------------------------------------------------------------
    -- Test 2: max in second partition → upper bound = 2000
    -- ----------------------------------------------------------------
    INSERT INTO dba_test.gaub_int VALUES (1500);

    RETURN QUERY SELECT * FROM dba_test.assert_equals(
        2000::bigint, dba.partition_get_active_upper_bound('dba_test', 'gaub_int'),
        'gaub_active_upper_second_partition'
    );

    -- ----------------------------------------------------------------
    -- Test 3: raises when not a partitioned table
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.gaub_plain (id bigint NOT NULL PRIMARY KEY);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_get_active_upper_bound('dba_test', 'gaub_plain')$sql$,
        'P0001',
        'gaub_not_partitioned_raises'
    );

    -- ----------------------------------------------------------------
    -- Test 4: raises when table is empty
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.gaub_empty (id bigint NOT NULL) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.gaub_empty_0_1000 PARTITION OF dba_test.gaub_empty FOR VALUES FROM (0) TO (1000);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_get_active_upper_bound('dba_test', 'gaub_empty')$sql$,
        'P0001',
        'gaub_empty_table_raises'
    );

    -- ----------------------------------------------------------------
    -- Test 5: raises when partition key is not an integer type
    -- ----------------------------------------------------------------
    CREATE TABLE dba_test.gaub_text_key (k text NOT NULL) PARTITION BY RANGE (k);
    CREATE TABLE dba_test.gaub_text_key_a_m PARTITION OF dba_test.gaub_text_key FOR VALUES FROM ('a') TO ('m');

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        $sql$SELECT dba.partition_get_active_upper_bound('dba_test', 'gaub_text_key')$sql$,
        'P0001',
        'gaub_non_integer_key_raises'
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_plain CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_empty CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.gaub_text_key CASCADE';

    RETURN;
END;
$$;
