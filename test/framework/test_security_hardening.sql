/*
Test: test_security_hardening
Functions under test: dba.partition_detach_partitions, dba.partition_drop_detached_partition,
    dba.partition_get_partition_column_info, dba.partition_calculate_free_partitions,
    dba.uuid_v7_to_timestamptz, dba.uuid_timestamptz_to_v7
Run: ./test/framework/run_partition_tests.sh security_quote_in_partition_name_exec
     ./test/framework/run_partition_tests.sh test_security_search_path_pinned
     ./test/framework/run_partition_tests.sh test_security_uuid_v7_helpers
Purpose: Guard the quoting of catalog names in generated SQL, the pinned search_path of the framework
functions and the bundled UUIDv7 helpers.
Test coverage: a partition whose name contains a single quote is detached and dropped; a function named
lower(name) in a schema on the caller's search_path is not used by framework functions; UUIDv7 helpers
round-trip and match the PostgreSQL 18 built-ins when available.
*/

-- partition_detach_partitions commits, so this runs at the top level from the runner instead of run_all_tests.
CREATE OR REPLACE PROCEDURE dba_test.security_quote_in_partition_name_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_dropped boolean;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.sec_quote CASCADE';
    EXECUTE $sql$DROP TABLE IF EXISTS dba_test."sec_quote_o'q" CASCADE$sql$;
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'sec_quote';
    DELETE FROM dba.detached_partitions WHERE schema = 'dba_test' AND parent_relname = 'sec_quote';

    CREATE TABLE dba_test.sec_quote (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    EXECUTE $sql$CREATE TABLE dba_test."sec_quote_o'q" PARTITION OF dba_test.sec_quote FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')$sql$;
    CREATE TABLE dba_test.sec_quote_20990101_20990201 PARTITION OF dba_test.sec_quote FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');
    INSERT INTO dba.partition_configuration VALUES ('dba_test', 'sec_quote', '{"detach":"365 days"}');

    CALL dba.partition_detach_partitions();

    SELECT count(*) INTO v_count
    FROM dba.detached_partitions
    WHERE schema = 'dba_test' AND parent_relname = 'sec_quote' AND partition_relname = $n$sec_quote_o'q$n$;
    PERFORM dba_test.record_result('security_detach_quote_in_partition_name',
        CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, format('detached rows=%s', v_count));

    v_dropped := dba.partition_drop_detached_partition('dba_test', 'sec_quote', $n$sec_quote_o'q$n$);

    SELECT count(*) INTO v_count
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'dba_test' AND c.relname = $n$sec_quote_o'q$n$;
    PERFORM dba_test.record_result('security_drop_quote_in_partition_name',
        CASE WHEN v_dropped AND v_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        format('dropped=%s remaining tables=%s', v_dropped, v_count));

    SELECT count(*) INTO v_count
    FROM dba.detached_partitions
    WHERE schema = 'dba_test' AND parent_relname = 'sec_quote';
    PERFORM dba_test.record_result('security_drop_quote_removes_detached_row',
        CASE WHEN v_count = 0 THEN 'PASS' ELSE 'FAIL' END, format('remaining rows=%s', v_count));

    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'sec_quote';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.sec_quote CASCADE';
END;
$$;

CREATE OR REPLACE FUNCTION dba_test.test_security_search_path_pinned()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_path text := current_setting('search_path');
    v_hits int;
BEGIN
    EXECUTE 'DROP SCHEMA IF EXISTS dba_test_evil CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.sec_path CASCADE';
    CREATE SCHEMA dba_test_evil;
    CREATE TABLE dba_test_evil.hits (n int);
    -- lower(name) is an exact match for LOWER(relname), so it wins over pg_catalog.lower(text) when
    -- dba_test_evil is on the search_path.
    CREATE FUNCTION dba_test_evil.lower(name) RETURNS text LANGUAGE sql VOLATILE
        AS $f$ INSERT INTO dba_test_evil.hits VALUES (1); SELECT pg_catalog.lower($1::text) $f$;

    CREATE TABLE dba_test.sec_path (id bigint not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.sec_path_0_100 PARTITION OF dba_test.sec_path FOR VALUES FROM (0) TO (100);
    CREATE TABLE dba_test.sec_path_100_200 PARTITION OF dba_test.sec_path FOR VALUES FROM (100) TO (200);

    PERFORM pg_catalog.set_config('search_path', 'dba_test_evil, public', true);

    -- Sanity check: unpinned SQL does pick up the planted function.
    PERFORM count(*) FROM pg_catalog.pg_class c WHERE lower(c.relname) = 'pg_class';
    SELECT count(*) INTO v_hits FROM dba_test_evil.hits;
    RETURN NEXT dba_test.assert_true(v_hits > 0, 'security_search_path_hijack_sanity', 'planted function was not used');
    TRUNCATE dba_test_evil.hits;

    PERFORM dba.partition_get_partition_column_info('dba_test', 'sec_path');
    PERFORM dba.partition_calculate_free_partitions('dba_test', 'sec_path');
    SELECT count(*) INTO v_hits FROM dba_test_evil.hits;

    PERFORM pg_catalog.set_config('search_path', v_old_path, true);
    RETURN NEXT dba_test.assert_equals(0, v_hits, 'security_search_path_pinned');

    EXECUTE 'DROP SCHEMA dba_test_evil CASCADE';
    EXECUTE 'DROP TABLE dba_test.sec_path CASCADE';
END;
$$;

CREATE OR REPLACE FUNCTION dba_test.test_security_uuid_v7_helpers()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_ts timestamptz := '2023-01-02 04:26:40.637+00';
    v_uuid uuid;
    v_mismatches int;
BEGIN
    RETURN NEXT dba_test.assert_equals('018570bb-4a7d-7000-8000-000000000000'::uuid,
        dba.uuid_timestamptz_to_v7(v_ts, true), 'security_uuid_v7_zero_value');

    v_uuid := dba.uuid_timestamptz_to_v7(v_ts);
    RETURN NEXT dba_test.assert_equals(v_ts, dba.uuid_v7_to_timestamptz(v_uuid), 'security_uuid_v7_round_trip');
    RETURN NEXT dba_test.assert_equals('7', substr(v_uuid::text, 15, 1), 'security_uuid_v7_version');
    RETURN NEXT dba_test.assert_true(substr(v_uuid::text, 20, 1) IN ('8', '9', 'a', 'b'), 'security_uuid_v7_variant');

    IF current_setting('server_version_num')::int >= 180000 THEN
        EXECUTE $sql$
            SELECT count(*) FILTER (WHERE dba.uuid_v7_to_timestamptz(u) <> uuid_extract_timestamp(u))
            FROM (SELECT uuidv7() AS u FROM generate_series(1, 1000)) s
        $sql$ INTO v_mismatches;
        RETURN NEXT dba_test.assert_equals(0, v_mismatches, 'security_uuid_v7_matches_builtin');
    ELSE
        RETURN NEXT dba_test.skip('security_uuid_v7_matches_builtin', 'requires PostgreSQL 18');
    END IF;
END;
$$;
