-- Test helpers. The dba schema, dba.partition_configuration and dba.detached_partitions are created from
-- test/setup_schema.sql by the runner.
CREATE SCHEMA IF NOT EXISTS dba_test;

CREATE TYPE dba_test.test_result AS (
    test_name text,
    result text,
    detail text
);

CREATE TABLE IF NOT EXISTS dba_test.test_results (
    test_name text,
    result text,
    detail text
);

CREATE OR REPLACE FUNCTION dba_test.uuidv7_supported() RETURNS boolean
LANGUAGE sql
AS $$
    SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'uuid_v7_to_timestamptz');
$$;

CREATE OR REPLACE FUNCTION dba_test.skip(v_test_name text, v_detail text) RETURNS dba_test.test_result
LANGUAGE sql
AS $$
    SELECT v_test_name, 'SKIP', v_detail;
$$;

CREATE OR REPLACE FUNCTION dba_test.assert_true(v_condition boolean, v_test_name text, v_detail text DEFAULT NULL)
RETURNS dba_test.test_result
LANGUAGE plpgsql
AS $$
BEGIN
    IF v_condition THEN
        RETURN (v_test_name, 'PASS'::text, v_detail);
    END IF;
    RETURN (v_test_name, 'FAIL'::text, COALESCE(v_detail, 'assert_true failed'));
END;
$$;

CREATE OR REPLACE FUNCTION dba_test.assert_equals(v_expected anyelement, v_actual anyelement, v_test_name text, v_detail text DEFAULT NULL)
RETURNS dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_msg text;
BEGIN
    IF v_expected IS NOT DISTINCT FROM v_actual THEN
        RETURN (v_test_name, 'PASS'::text, v_detail);
    END IF;
    v_msg := COALESCE(v_detail, format('expected=%s actual=%s', v_expected, v_actual));
    RETURN (v_test_name, 'FAIL'::text, v_msg);
END;
$$;

CREATE OR REPLACE FUNCTION dba_test.assert_raises(v_sql text, v_expected_sqlstate text, v_test_name text)
RETURNS dba_test.test_result
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE v_sql;
    RETURN (v_test_name, 'FAIL'::text, 'no exception raised'::text);
EXCEPTION
    WHEN OTHERS THEN
        IF SQLSTATE = v_expected_sqlstate THEN
            RETURN (v_test_name, 'PASS'::text, NULL::text);
        END IF;
        RETURN (v_test_name, 'FAIL'::text, format('unexpected sqlstate %s', SQLSTATE)::text);
END;
$$;

CREATE OR REPLACE FUNCTION dba_test.record_result(v_test_name text, v_result text, v_detail text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dba_test.test_results(test_name, result, detail)
    VALUES (v_test_name, v_result, v_detail);
END;
$$;

CREATE OR REPLACE PROCEDURE dba_test.run_all_tests()
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
BEGIN
    TRUNCATE dba_test.test_results;
    FOR r IN
        SELECT p.proname, p.prokind
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'dba_test' AND p.proname LIKE 'test_%'
        ORDER BY p.proname
    LOOP
        IF r.prokind = 'f' THEN
            EXECUTE format('INSERT INTO dba_test.test_results SELECT * FROM dba_test.%I()', r.proname);
        ELSIF r.prokind = 'p' THEN
            EXECUTE format('CALL dba_test.%I()', r.proname);
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE dba_test.run_test(v_test_name text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_prokind char;
BEGIN
    TRUNCATE dba_test.test_results;

    RAISE DEBUG 'running only test %', v_test_name;
    SELECT p.prokind
    INTO v_prokind
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'dba_test' AND p.proname = v_test_name;

    IF v_prokind = 'f' THEN
        EXECUTE format('INSERT INTO dba_test.test_results SELECT * FROM dba_test.%I()', v_test_name);
    ELSIF v_prokind = 'p' THEN
        EXECUTE format('CALL dba_test.%I()', v_test_name);
    ELSE
        RAISE EXCEPTION 'Test % not found', v_test_name;
    END IF;
END;
$$;
