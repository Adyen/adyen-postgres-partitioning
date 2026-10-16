/*
Test: test_partition_inheritance
Function under test: dba.partition_inheritance
Run: ./test/framework/run_partition_tests.sh test_partition_inheritance
Purpose: Convert a table to inheritance-based partitions.
Test coverage: Verifies overflow table creation after conversion.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_inheritance()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists int;
    v_lower date;
    v_int_lower bigint;
    v_ts_lower timestamptz;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.inh_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.inh_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.inh_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.inh_text CASCADE';

    CREATE TABLE dba_test.inh_table (id bigint not null, record_date date not null);
    INSERT INTO dba_test.inh_table VALUES (1, '2020-01-01');

    PERFORM dba.partition_inheritance('dba_test','inh_table','record_date','2020-01-01','2020-01-31','1 month');

    SELECT count(*) FROM pg_class WHERE relname = 'inh_table_overflow' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_exists, 'partition_inheritance_overflow');

    SELECT substring(pg_get_constraintdef(c.oid) from '''([0-9-]+)''::date')::date
    INTO v_lower
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relnamespace = 'dba_test'::regnamespace
      AND t.relname LIKE 'inh_table_%'
      AND c.conname LIKE '%_check'
      AND pg_get_constraintdef(c.oid) ~ 'record_date'
    ORDER BY substring(pg_get_constraintdef(c.oid) from '''([0-9-]+)''::date')::date DESC
    LIMIT 1;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-01'::date, v_lower, 'partition_inheritance_date_boundary');

    CREATE TABLE dba_test.inh_int (id bigint not null);
    INSERT INTO dba_test.inh_int VALUES (1);

    PERFORM dba.partition_inheritance('dba_test','inh_int','id','0','10','10');

    SELECT substring(pg_get_constraintdef(c.oid) from '>= ''([0-9]+)''::bigint')::bigint
    INTO v_int_lower
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relnamespace = 'dba_test'::regnamespace
      AND t.relname LIKE 'inh_int_%'
      AND c.conname LIKE '%_check'
      AND pg_get_constraintdef(c.oid) ~ 'id'
    ORDER BY substring(pg_get_constraintdef(c.oid) from '>= ''([0-9]+)''::bigint')::bigint DESC
    LIMIT 1;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(11::bigint, v_int_lower, 'partition_inheritance_int_boundary');

    CREATE TABLE dba_test.inh_ts (id bigint not null, record_ts timestamptz not null);
    INSERT INTO dba_test.inh_ts VALUES (1, '2020-01-01 00:00:00+00');

    PERFORM dba.partition_inheritance('dba_test','inh_ts','record_ts','2020-01-01','2020-01-31','1 month');

    SELECT substring(pg_get_constraintdef(c.oid) from '>= ''([^'']+)''::timestamp with time zone')::timestamptz
    INTO v_ts_lower
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relnamespace = 'dba_test'::regnamespace
      AND t.relname LIKE 'inh_ts_%'
      AND c.conname LIKE '%_check'
      AND pg_get_constraintdef(c.oid) ~ 'record_ts'
    ORDER BY substring(pg_get_constraintdef(c.oid) from '>= ''([^'']+)''::timestamp with time zone')::timestamptz DESC
    LIMIT 1;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-01 00:00:00+01'::timestamptz, v_ts_lower, 'partition_inheritance_ts_boundary');

    CREATE TABLE dba_test.inh_text (id text not null);
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_inheritance(''dba_test'',''inh_text'',''id'',''a'',''b'',''1'')',
        'P0001',
        'partition_inheritance_unsupported_type'
    );

    RETURN;
END;
$$;
