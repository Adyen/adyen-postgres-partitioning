/*
Test: test_partition_table_native_wrapper
Function under test: dba.partition_table_native_wrapper
Run: ./test/framework/run_partition_tests.sh test_partition_table_native_wrapper
Purpose: Convert an unpartitioned table to native partitioning and add initial partitions.
Test coverage: Partitions date/int/timestamp tables and verifies partitioned state and configuration entries. Mixed-case arguments must register a lowercase configuration entry.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_table_native_wrapper()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_lower_first text;
    v_upper_first text;
    v_lower_second text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.wrap_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.wrap_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.wrap_ts CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name IN ('wrap_date','wrap_int','wrap_ts');

    CREATE TABLE dba_test.wrap_date (id bigint not null, record_date date not null, PRIMARY KEY (id, record_date));
    INSERT INTO dba_test.wrap_date VALUES (1, '2020-01-01');
    PERFORM dba.partition_table_native_wrapper('dba_test','wrap_date','record_date','2020-01-01','2020-02-01','1 month');

    SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'wrap_date' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_date_partitioned');

    SELECT count(*) FROM dba.partition_configuration WHERE table_name = 'wrap_date' AND schema_name = 'dba_test' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_date_config_added');

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1],
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_lower_first, v_upper_first
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date ASC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]
    INTO v_lower_second
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_date' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date ASC
    OFFSET 1 LIMIT 1;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_first::date, v_lower_second::date, 'wrapper_date_boundary_matches');

    CREATE TABLE dba_test.wrap_int (id bigint not null, ref_id bigint not null, PRIMARY KEY (id, ref_id));
    ALTER TABLE dba_test.wrap_int ALTER COLUMN ref_id SET STATISTICS 400;
    CREATE STATISTICS dba_test.wrap_int_ndist (ndistinct) ON id, ref_id FROM dba_test.wrap_int;
    INSERT INTO dba_test.wrap_int VALUES (1, 1);
    PERFORM dba.partition_table_native_wrapper(
        'dba_test',
        'wrap_int',
        'ref_id',
        '0',
        '100',
        '10',
        p_copy_statistics_to_children := TRUE
    );

    SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'wrap_int' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_int_partitioned');

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1],
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_lower_first, v_upper_first
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]
    INTO v_lower_second
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_int' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::bigint ASC
    OFFSET 1 LIMIT 1;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_first::bigint, v_lower_second::bigint, 'wrapper_int_boundary_matches');

    SELECT a.attstattarget
    FROM pg_attribute AS a
    JOIN pg_class AS child ON child.oid = a.attrelid
    JOIN pg_inherits AS i ON i.inhrelid = child.oid
    JOIN pg_class AS parent ON parent.oid = i.inhparent
    WHERE parent.relnamespace = 'dba_test'::regnamespace
        AND parent.relname = 'wrap_int'
        AND child.relname <> 'wrap_int_mammoth'
        AND a.attname = 'ref_id'
    LIMIT 1
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(400, v_count, 'wrapper_statistics_copied_to_child');

    SELECT count(*)
    FROM pg_statistic_ext AS s
    JOIN pg_class AS child ON child.oid = s.stxrelid
    JOIN pg_inherits AS i ON i.inhrelid = child.oid
    JOIN pg_class AS parent ON parent.oid = i.inhparent
    WHERE parent.relnamespace = 'dba_test'::regnamespace
        AND parent.relname = 'wrap_int'
        AND child.relname <> 'wrap_int_mammoth'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'wrapper_extended_statistics_copied_to_children');

    CREATE TABLE dba_test.wrap_ts (id bigint not null, created_at timestamptz not null, PRIMARY KEY (id, created_at));
    INSERT INTO dba_test.wrap_ts VALUES (1, '2020-01-01 00:00:00+00');
    PERFORM dba.partition_table_native_wrapper('dba_test','wrap_ts','created_at','2020-01-01','2020-02-01','1 month', true, 1000, 5, 5, true);

    SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'wrap_ts' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_ts_partitioned');

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1],
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_lower_first, v_upper_first
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz ASC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]
    INTO v_lower_second
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'wrap_ts' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::timestamptz ASC
    OFFSET 1 LIMIT 1;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_first::timestamptz, v_lower_second::timestamptz, 'wrapper_ts_boundary_matches');

    -- Mixed-case arguments must partition the table and register a lowercase row
    -- in dba.partition_configuration.
    EXECUTE 'DROP TABLE IF EXISTS dba_test.wrap_case CASCADE';
    DELETE FROM dba.partition_configuration WHERE schema_name = 'dba_test' AND table_name = 'wrap_case';

    CREATE TABLE dba_test.wrap_case (id bigint not null, ref_id bigint not null, PRIMARY KEY (id, ref_id));
    INSERT INTO dba_test.wrap_case VALUES (1, 1);

    BEGIN
        PERFORM dba.partition_table_native_wrapper('DBA_TEST','WRAP_CASE','REF_ID','0','100','10');

        SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'wrap_case' INTO v_count;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_case_partitioned');

        SELECT count(*) FROM dba.partition_configuration WHERE table_name = 'wrap_case' AND schema_name = 'dba_test' INTO v_count;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'wrapper_case_config_added_lowercase');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('wrapper_case_partitioned', 'FAIL', SQLERRM);
        PERFORM dba_test.record_result('wrapper_case_config_added_lowercase', 'FAIL', SQLERRM);
    END;

    RETURN;
END;
$$;
