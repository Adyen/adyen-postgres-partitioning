/*
Test: test_partition_add_up_to_nr_of_free_partitions
Function under test: dba.partition_add_up_to_nr_of_free_partitions
Run: ./test/framework/run_partition_tests.sh test_partition_add_up_to_nr_of_free_partitions
Purpose: Add partitions until the requested number of free partitions exists.
Test coverage: Builds integer/date/timestamp fixtures, verifies new partitions and option copying, validates error paths for unsupported/non-partitioned tables, and skips UUIDv7 when unavailable.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_add_up_to_nr_of_free_partitions()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_reloptions text;
    v_attoptions text[];
    v_prev_upper bigint;
    v_new_lower bigint;
    v_prev_date_upper date;
    v_new_date_lower date;
    v_prev_ts_upper timestamptz;
    v_new_ts_lower timestamptz;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_text CASCADE';

    CREATE TABLE dba_test.add_int (id bigint not null, val text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.add_int_0_10 PARTITION OF dba_test.add_int FOR VALUES FROM (0) TO (10);
    ALTER TABLE dba_test.add_int_0_10 SET (fillfactor = 80);
    ALTER TABLE ONLY dba_test.add_int_0_10 ALTER COLUMN id SET (n_distinct = 10);

    INSERT INTO dba_test.add_int VALUES (1, 'a');
    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::bigint
    INTO v_prev_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint DESC
    LIMIT 1;

    PERFORM dba.partition_add_up_to_nr_of_free_partitions('dba_test','add_int', 2);

    SELECT count(*) FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'dba_test.add_int'::regclass
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 3, 'add_up_int_created_partitions');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint)
    INTO v_new_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint >= v_prev_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_upper, v_new_lower, 'add_up_int_boundary_matches');

    SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname = 'add_int_10_20' AND relnamespace = 'dba_test'::regnamespace
    INTO v_reloptions;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_reloptions LIKE '%fillfactor=80%', 'add_up_int_copies_reloptions');

    SELECT attoptions FROM pg_attribute WHERE attrelid = 'dba_test.add_int_10_20'::regclass AND attname = 'id'
    INTO v_attoptions;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_attoptions::text LIKE '%n_distinct=10%', 'add_up_int_copies_attoptions');

    CREATE TABLE dba_test.add_date (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.add_date_20240101_20240201 PARTITION OF dba_test.add_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::date
    INTO v_prev_date_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date DESC
    LIMIT 1;

    PERFORM dba.partition_add_up_to_nr_of_free_partitions('dba_test','add_date', 1);

    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.add_date'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2, 'add_up_date_created_partitions');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date)
    INTO v_new_date_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date >= v_prev_date_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_date_upper, v_new_date_lower, 'add_up_date_boundary_matches');

    CREATE TABLE dba_test.add_ts (id bigint, trip_ts timestamptz not null) PARTITION BY RANGE (trip_ts);
    CREATE TABLE dba_test.add_ts_20240101_20240201 PARTITION OF dba_test.add_ts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]::timestamptz
    INTO v_prev_ts_upper
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz DESC
    LIMIT 1;

    PERFORM dba.partition_add_up_to_nr_of_free_partitions('dba_test','add_ts', 1);

    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.add_ts'::regclass INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 2, 'add_up_ts_created_partitions');

    SELECT min((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz)
    INTO v_new_ts_lower
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'add_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz >= v_prev_ts_upper;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_prev_ts_upper, v_new_ts_lower, 'add_up_ts_boundary_matches');

    CREATE TABLE dba_test.add_text (id text) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.add_text_a PARTITION OF dba_test.add_text FOR VALUES FROM ('a') TO ('m');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_add_up_to_nr_of_free_partitions(''dba_test'',''add_text'', 1)',
        'Z1001',
        'add_up_unsupported_type_raises'
    );

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_add_up_to_nr_of_free_partitions(''dba_test'',''non_partitioned'', 1)',
        'P0001',
        'add_up_non_partitioned_raises'
    );

    IF NOT dba_test.uuidv7_supported() THEN
        RETURN QUERY SELECT * FROM dba_test.skip('add_up_uuid_skip', 'no uuidv7 support');
    END IF;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.add_text CASCADE';

    RETURN;
END;
$$;



