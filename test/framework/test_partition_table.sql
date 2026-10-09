/*
Test: test_partition_table
Function under test: dba.partition_table
Run: ./test/framework/run_partition_tests.sh test_partition_table
Purpose: Wrapper to partition a table using native or inheritance method.
Test coverage: Runs native partitioning and verifies the table is partitioned.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_table()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_exists int;
    v_lower_first text;
    v_upper_first text;
    v_lower_second text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ptable CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.ptable_inh CASCADE';

    CREATE TABLE dba_test.ptable (id bigint not null, record_date date not null, PRIMARY KEY (id, record_date));
    INSERT INTO dba_test.ptable VALUES (1, '2020-01-01');

    PERFORM dba.partition_table('dba_test','ptable','record_date','2020-01-01','2020-02-01','1 month','native');

    SELECT count(*) FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'ptable' INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'partition_table_native');

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1],
           (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[2]
    INTO v_lower_first, v_upper_first
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'ptable' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date ASC
    LIMIT 1;

    SELECT (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]
    INTO v_lower_second
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'ptable' AND parent.relnamespace = 'dba_test'::regnamespace
      AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'))[1]::date ASC
    OFFSET 1 LIMIT 1;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_first::date, v_lower_second::date, 'partition_table_native_boundary_matches');

    CREATE TABLE dba_test.ptable_inh (id bigint not null, record_date date not null);
    INSERT INTO dba_test.ptable_inh VALUES (1, '2020-01-01');

    PERFORM dba.partition_table('dba_test','ptable_inh','record_date','2020-01-01','2020-01-31','1 month','inheritance');

    SELECT count(*) FROM pg_class WHERE relname = 'ptable_inh_overflow' AND relnamespace = 'dba_test'::regnamespace INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_exists, 'partition_table_inheritance');

    RETURN;
END;
$$;
