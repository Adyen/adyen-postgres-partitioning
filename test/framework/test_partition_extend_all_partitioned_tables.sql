/*
Test: test_partition_extend_all_partitioned_tables
Function under test: dba.partition_extend_all_partitioned_tables
Run: ./test/framework/run_partition_tests.sh test_partition_extend_all_partitioned_tables
Purpose: Extend all configured tables to ensure minimum free partitions.
Test coverage: Adds configuration and checks that new partitions are created.
             Also covers that a failure on one table does not abort processing of remaining tables.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_extend_all_partitioned_tables()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_prev_upper bigint;
    v_new_lower bigint;
    v_prev_date_upper date;
    v_new_date_lower date;
    v_prev_ts_upper timestamptz;
    v_new_ts_lower timestamptz;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_ts CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name IN ('extend_int','extend_date','extend_ts');

    CREATE TABLE dba_test.extend_int (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.extend_int_0_10 PARTITION OF dba_test.extend_int FOR VALUES FROM (0) TO (10);

    CREATE TABLE dba_test.extend_date (id bigint, record_date date not null) PARTITION BY RANGE (record_date);
    CREATE TABLE dba_test.extend_date_20240101_20240201 PARTITION OF dba_test.extend_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

    CREATE TABLE dba_test.extend_ts (id bigint, record_ts timestamptz not null) PARTITION BY RANGE (record_ts);
    CREATE TABLE dba_test.extend_ts_20240101_20240201 PARTITION OF dba_test.extend_ts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','extend_int','{"auto-maintenance": true, "nr": 1}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','extend_date','{"auto-maintenance": true, "nr": 1}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','extend_ts','{"auto-maintenance": true, "nr": 1}');

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_prev_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint DESC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::date
    INTO v_prev_date_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date DESC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::timestamptz
    INTO v_prev_ts_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz DESC
    LIMIT 1;

    PERFORM dba.partition_extend_all_partitioned_tables();

    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.extend_int'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2, 'extend_all_partitioned_tables');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint)
    INTO v_new_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint >= v_prev_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_upper, v_new_lower, 'extend_int_boundary_matches');

    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.extend_date'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2, 'extend_all_partitioned_tables_date');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date)
    INTO v_new_date_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date >= v_prev_date_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_date_upper, v_new_date_lower, 'extend_date_boundary_matches');

    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.extend_ts'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2, 'extend_all_partitioned_tables_ts');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz)
    INTO v_new_ts_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'extend_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz >= v_prev_ts_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_ts_upper, v_new_ts_lower, 'extend_ts_boundary_matches');

    RETURN;
END;
$$;

/*
Test: test_partition_extend_all_continues_after_error
Function under test: dba.partition_extend_all_partitioned_tables
Purpose: Verify that a failure on one table does not abort processing of remaining tables.

To trigger a controlled failure, a RANGE-partitioned table on a numeric column is created.
partition_calculate_free_partitions raises SQLSTATE Z1001 for unsupported column types, which
is caught by the WHEN sqlstate 'Z1001' handler. Before the fix this caused RETURN FALSE,
stopping the loop. After the fix it sets v_all_succeeded := FALSE and the loop continues.

The failing table (err_numeric) is created first so it receives a lower OID and is typically
returned first by the catalog query (which has no ORDER BY). The healthy table is created
second and should be extended even though the first table fails.
*/
CREATE OR REPLACE FUNCTION dba_test.test_partition_extend_all_continues_after_error()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result  BOOLEAN;
    v_count   INT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.err_numeric CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.healthy_date_cont CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name IN ('err_numeric', 'healthy_date_cont');

    -- numeric column type: partition_calculate_free_partitions raises Z1001, simulating a failure.
    CREATE TABLE dba_test.err_numeric (id numeric) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.err_numeric_0_10 PARTITION OF dba_test.err_numeric FOR VALUES FROM (0) TO (10);
    INSERT INTO dba.partition_configuration VALUES ('dba_test', 'err_numeric', '{"auto-maintenance": true, "nr": 1}');

    -- Healthy date-partitioned table: should be extended even when err_numeric fails first.
    CREATE TABLE dba_test.healthy_date_cont (id bigint, d date NOT NULL) PARTITION BY RANGE (d);
    CREATE TABLE dba_test.healthy_date_cont_20240101_20240201 PARTITION OF dba_test.healthy_date_cont
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    INSERT INTO dba.partition_configuration VALUES ('dba_test', 'healthy_date_cont', '{"auto-maintenance": true, "nr": 1}');

    SELECT dba.partition_extend_all_partitioned_tables() INTO v_result;

    -- Returns FALSE because err_numeric failed.
    RETURN QUERY SELECT * FROM dba_test.assert_equals(FALSE, v_result,
        'extend_continues_returns_false_on_partial_failure');

    -- healthy_date_cont must have received new partitions: the loop continued past the failure.
    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.healthy_date_cont'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2,
        'extend_continues_healthy_table_was_extended_after_failure');

    -- err_numeric itself must not have gained any extra partitions (the error occurred before attach).
    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.err_numeric'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count,
        'extend_continues_failing_table_partition_count_unchanged');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.err_numeric CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.healthy_date_cont CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name IN ('err_numeric', 'healthy_date_cont');

    RETURN;
END;
$$;

/*
Test: test_partition_extend_all_returns_false_single_failing_table
Function under test: dba.partition_extend_all_partitioned_tables
Purpose: Verify that the function returns FALSE (not TRUE) when every configured table fails,
         i.e. the return value correctly reflects partial or total failure.
*/
CREATE OR REPLACE FUNCTION dba_test.test_partition_extend_all_returns_false_single_failing_table()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result BOOLEAN;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.err_numeric2 CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'err_numeric2';

    CREATE TABLE dba_test.err_numeric2 (id numeric) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.err_numeric2_0_10 PARTITION OF dba_test.err_numeric2 FOR VALUES FROM (0) TO (10);
    INSERT INTO dba.partition_configuration VALUES ('dba_test', 'err_numeric2', '{"auto-maintenance": true, "nr": 1}');

    SELECT dba.partition_extend_all_partitioned_tables() INTO v_result;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(FALSE, v_result,
        'extend_all_returns_false_when_only_table_fails');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.err_numeric2 CASCADE';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test' AND table_name = 'err_numeric2';

    RETURN;
END;
$$;

/*
Test: test_partition_extend_all_continues_after_false_result
Function under test: dba.partition_extend_all_partitioned_tables
Purpose: Verify that a FALSE result from partition_add_up_to_nr_of_free_partitions
         does not abort processing of the remaining tables.
*/
CREATE OR REPLACE FUNCTION dba_test.test_partition_extend_all_continues_after_false_result()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_result BOOLEAN;
    v_count INT;
    v_successful_table TEXT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_false_result_first CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_false_result_second CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.partition_extend_false_result_calls';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test'
      AND table_name IN ('extend_false_result_first', 'extend_false_result_second');

    CREATE TABLE dba_test.extend_false_result_first (id bigint, d date NOT NULL) PARTITION BY RANGE (d);
    CREATE TABLE dba_test.extend_false_result_first_20240101_20240201
        PARTITION OF dba_test.extend_false_result_first FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE dba_test.extend_false_result_second (id bigint, d date NOT NULL) PARTITION BY RANGE (d);
    CREATE TABLE dba_test.extend_false_result_second_20240101_20240201
        PARTITION OF dba_test.extend_false_result_second FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    INSERT INTO dba.partition_configuration VALUES
        ('dba_test', 'extend_false_result_first', '{"auto-maintenance": true, "nr": 1}'),
        ('dba_test', 'extend_false_result_second', '{"auto-maintenance": true, "nr": 1}');

    CREATE TABLE dba_test.partition_extend_false_result_calls (
        call_order serial PRIMARY KEY,
        table_name text NOT NULL
    );

    ALTER FUNCTION dba.partition_add_up_to_nr_of_free_partitions(text, text, int)
        RENAME TO partition_add_up_to_nr_of_free_partitions_test_delegate;

    CREATE FUNCTION dba.partition_add_up_to_nr_of_free_partitions(
        v_schema text,
        v_relname text,
        v_number_of_additional_partitions int
    )
    RETURNS boolean
    LANGUAGE plpgsql
    AS $wrapper$
    DECLARE
        v_call_count int;
    BEGIN
        IF lower(v_schema) = 'dba_test'
           AND lower(v_relname) IN ('extend_false_result_first', 'extend_false_result_second') THEN
            INSERT INTO dba_test.partition_extend_false_result_calls(table_name) VALUES (v_relname);
            SELECT count(*) INTO v_call_count FROM dba_test.partition_extend_false_result_calls;

            IF v_call_count = 1 THEN
                RETURN FALSE;
            END IF;
        END IF;

        RETURN dba.partition_add_up_to_nr_of_free_partitions_test_delegate(
            v_schema,
            v_relname,
            v_number_of_additional_partitions
        );
    END;
    $wrapper$;

    SELECT dba.partition_extend_all_partitioned_tables() INTO v_result;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(FALSE, v_result,
        'extend_all_returns_false_when_partition_add_returns_false');

    SELECT count(*) INTO v_count FROM dba_test.partition_extend_false_result_calls;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count,
        'extend_all_continues_after_partition_add_returns_false');

    SELECT table_name INTO v_successful_table
    FROM dba_test.partition_extend_false_result_calls
    ORDER BY call_order DESC
    LIMIT 1;
    SELECT count(*) INTO v_count
    FROM pg_inherits
    WHERE inhparent = format('dba_test.%I', v_successful_table)::regclass;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2,
        'extend_all_extends_table_after_partition_add_returns_false');

    DROP FUNCTION dba.partition_add_up_to_nr_of_free_partitions(text, text, int);
    ALTER FUNCTION dba.partition_add_up_to_nr_of_free_partitions_test_delegate(text, text, int)
        RENAME TO partition_add_up_to_nr_of_free_partitions;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_false_result_first CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.extend_false_result_second CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.partition_extend_false_result_calls';
    DELETE FROM dba.partition_configuration
    WHERE schema_name = 'dba_test'
      AND table_name IN ('extend_false_result_first', 'extend_false_result_second');

    RETURN;
END;
$$;
