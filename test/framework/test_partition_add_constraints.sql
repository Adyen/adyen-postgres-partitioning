/*
Test: test_partition_add_constraints
Function under test: dba.partition_add_constraints
Run: ./test/framework/run_partition_tests.sh test_partition_add_constraints
Purpose: Add min/max pruning constraints on non-partition columns for integer-partitioned tables.
Test coverage: Validates min/max constraint creation, invalid column type error handling and
mixed-case schema/table/column arguments.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_add_constraints()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.constr_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.constr_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.constr_case CASCADE';

    CREATE TABLE dba_test.constr_int (id bigint not null, record_date date not null, note text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.constr_int_0_10 PARTITION OF dba_test.constr_int FOR VALUES FROM (0) TO (10);

    INSERT INTO dba_test.constr_int VALUES (1, current_date, 'a');
    PERFORM dba.partition_add_constraints('dba_test','constr_int','record_date','record_date');

    SELECT count(*) FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'constr_int_0_10' AND c.conname = 'constr_int_0_10_record_date_min'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'add_constraints_min');

    INSERT INTO dba_test.constr_int VALUES (9, current_date, 'b');
    PERFORM dba.partition_add_constraints('dba_test','constr_int','record_date','record_date');

    SELECT count(*) FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'constr_int_0_10' AND c.conname = 'constr_int_0_10_record_date_max'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'add_constraints_max');

    -- Mixed-case schema, table and column arguments must behave like lower-cased ones
    CREATE TABLE dba_test.constr_case (id bigint not null, record_date date not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.constr_case_0_10 PARTITION OF dba_test.constr_case FOR VALUES FROM (0) TO (10);

    INSERT INTO dba_test.constr_case VALUES (1, current_date);
    INSERT INTO dba_test.constr_case VALUES (9, current_date);

    BEGIN
        PERFORM dba.partition_add_constraints('DBA_TEST','CONSTR_CASE','record_date','RECORD_DATE');

        SELECT count(*) FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'constr_case_0_10'
          AND c.conname IN ('constr_case_0_10_record_date_min', 'constr_case_0_10_record_date_max')
        INTO v_count;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'add_constraints_mixed_case');
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT 'add_constraints_mixed_case'::text, 'FAIL'::text, SQLERRM::text;
    END;

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_add_constraints(''dba_test'',''constr_int'',''record_date'',''note'')',
        'P0001',
        'add_constraints_invalid_column_type'
    );

    CREATE TABLE dba_test.constr_date (id bigint, record_date date not null) PARTITION BY RANGE (record_date);
    CREATE TABLE dba_test.constr_date_20240101_20240201 PARTITION OF dba_test.constr_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_add_constraints(''dba_test'',''constr_date'',''record_date'',''record_date'')',
        'P0001',
        'add_constraints_non_int_partitioned'
    );

    RETURN;
END;
$$;
