/*
Test: test_optimized_column_order
Function under test: dba.get_optimized_column_order, dba.generate_create_table_optimized_columns, dba.partition_add_up_to_nr_of_free_partitions
Run: ./test/framework/run_partition_tests.sh test_optimized_column_order
Purpose: Validate optimized column ordering and propagation to generated/partitioned tables.
Test coverage: Ordering for bad/optimal tables, generated table defaults/constraints, and partitioned table propagation.
*/

CREATE OR REPLACE FUNCTION dba_test.test_optimized_column_order()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_expected TEXT;
    v_actual TEXT;
    v_created_table TEXT;
    v_exists BOOLEAN;
    v_index_count INT;
    v_constraint_count INT;
    v_relname TEXT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_bad_order CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_optimal_order CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_source_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_target_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_partitioned_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_partitioned_table_0_10 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_date_partitioned CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_date_partitioned_20240101_20240201 CASCADE';

    -- Test 1: alignment ordering
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_bad_order (
            small_val smallint,
            big_val bigint,
            flag boolean,
            int_val integer,
            txt text,
            ts timestamp
        )
    $sql$;

    SELECT array_to_string(dba.get_optimized_column_order('dba_test', 'opt_bad_order'), ',')
    INTO v_actual;
    v_expected := 'big_val,ts,int_val,small_val,flag,txt';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'optimized_order_basic');

    -- Test 2: already optimal
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_optimal_order (
            big_val bigint,
            ts timestamp,
            txt text,
            int_val integer,
            small_val smallint,
            flag boolean
        )
    $sql$;
    SELECT array_to_string(dba.get_optimized_column_order('dba_test', 'opt_optimal_order'), ',')
    INTO v_actual;
    v_expected := 'big_val,ts,int_val,small_val,flag,txt';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'optimized_order_already_optimal');

    -- Test 3: generate_create_table_optimized_columns
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_source_table (
            small_val smallint,
            big_val bigint DEFAULT 7,
            flag boolean NOT NULL,
            int_val integer,
            txt text,
            ts timestamp
        )
    $sql$;
    SELECT dba.generate_create_table_optimized_columns('dba_test', 'opt_source_table', 'dba_test', 'opt_target_table')
    INTO v_created_table;
    EXECUTE v_created_table;

    SELECT array_to_string(array_agg(attname ORDER BY attnum), ',')
    FROM pg_attribute
    WHERE attrelid = 'dba_test.opt_target_table'::regclass
        AND attnum > 0
        AND NOT attisdropped
    INTO v_actual;
    v_expected := 'big_val,ts,int_val,small_val,flag,txt';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'optimized_columns_target_order');

    SELECT pg_get_expr(ad.adbin, ad.adrelid)
    FROM pg_attrdef ad
    JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
    WHERE ad.adrelid = 'dba_test.opt_target_table'::regclass
        AND a.attname = 'big_val'
    INTO v_actual;
    v_expected := '7';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'optimized_columns_default');

    SELECT attnotnull
    FROM pg_attribute
    WHERE attrelid = 'dba_test.opt_target_table'::regclass
        AND attname = 'flag'
    INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_exists IS TRUE, 'optimized_columns_not_null', 'expected TRUE');

    -- Test 4: partition_add_up_to_nr_of_free_partitions (integer)
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_partitioned_table (
            id integer NOT NULL,
            small_val smallint,
            big_val bigint,
            flag boolean,
            ts timestamp,
            descr text
        ) PARTITION BY RANGE (id)
    $sql$;
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_partitioned_table_0_10 (
            id integer NOT NULL,
            small_val smallint,
            big_val bigint,
            flag boolean,
            ts timestamp,
            descr text
        )
    $sql$;
    EXECUTE 'ALTER TABLE dba_test.opt_partitioned_table ATTACH PARTITION dba_test.opt_partitioned_table_0_10 FOR VALUES FROM (0) TO (10)';
    EXECUTE 'CREATE INDEX opt_partitioned_table_0_10_idx ON dba_test.opt_partitioned_table_0_10 (big_val)';
    EXECUTE 'ALTER TABLE dba_test.opt_partitioned_table_0_10 ADD CONSTRAINT opt_partitioned_table_0_10_chk CHECK (big_val > 0)';

    PERFORM dba.partition_add_up_to_nr_of_free_partitions('dba_test', 'opt_partitioned_table', 2);

    FOR v_relname IN
        SELECT child.relname
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = child.relnamespace
        WHERE parent.relname = 'opt_partitioned_table'
            AND n.nspname = 'dba_test'
            AND child.relname <> 'opt_partitioned_table_0_10'
    LOOP
        SELECT array_to_string(array_agg(attname ORDER BY attnum), ',')
        FROM pg_attribute
        WHERE attrelid = (format('dba_test.%s', v_relname))::regclass
            AND attnum > 0
            AND NOT attisdropped
        INTO v_actual;
        v_expected := 'big_val,ts,id,small_val,flag,descr';
        RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'partition_add_free_partitions_order');

        SELECT count(*)
        FROM pg_indexes
        WHERE schemaname = 'dba_test'
            AND tablename = v_relname
            AND indexname LIKE '%_idx%'
        INTO v_index_count;
        RETURN QUERY SELECT * FROM dba_test.assert_true(v_index_count >= 1, 'partition_add_free_partitions_indexes', 'expected >= 1');

        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid = (format('dba_test.%s', v_relname))::regclass
            AND contype = 'c'
            AND conname LIKE '%_chk'
        INTO v_constraint_count;
        RETURN QUERY SELECT * FROM dba_test.assert_true(v_constraint_count >= 1, 'partition_add_free_partitions_checks', 'expected >= 1');
    END LOOP;

    -- Test 5: partition_add_up_to_nr_of_free_partitions (date)
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_date_partitioned (
            created_at date NOT NULL,
            small_val smallint,
            big_val bigint,
            flag boolean,
            descr text
        ) PARTITION BY RANGE (created_at)
    $sql$;
    EXECUTE $sql$
        CREATE TABLE dba_test.opt_date_partitioned_20240101_20240201 (
            created_at date NOT NULL,
            small_val smallint,
            big_val bigint,
            flag boolean,
            descr text
        )
    $sql$;
    EXECUTE 'ALTER TABLE dba_test.opt_date_partitioned ATTACH PARTITION dba_test.opt_date_partitioned_20240101_20240201 FOR VALUES FROM (''2024-01-01'') TO (''2024-02-01'')';

    PERFORM dba.partition_add_up_to_nr_of_free_partitions('dba_test', 'opt_date_partitioned', 2);

    FOR v_relname IN
        SELECT child.relname
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = child.relnamespace
        WHERE parent.relname = 'opt_date_partitioned'
            AND n.nspname = 'dba_test'
            AND child.relname <> 'opt_date_partitioned_20240101_20240201'
    LOOP
        SELECT array_to_string(array_agg(attname ORDER BY attnum), ',')
        FROM pg_attribute
        WHERE attrelid = (format('dba_test.%s', v_relname))::regclass
            AND attnum > 0
            AND NOT attisdropped
        INTO v_actual;
        v_expected := 'big_val,created_at,small_val,flag,descr';
        RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'partition_add_free_partitions_date_order');
    END LOOP;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_bad_order CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_optimal_order CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_source_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_target_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_partitioned_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_partitioned_table_0_10 CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_date_partitioned CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.opt_date_partitioned_20240101_20240201 CASCADE';

    RETURN;
END;
$$;
