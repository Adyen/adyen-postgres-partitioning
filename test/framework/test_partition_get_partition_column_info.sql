/*
Test: test_partition_get_partition_column_info
Function under test: dba.partition_get_partition_column_info
Run: ./test/framework/run_partition_tests.sh test_partition_get_partition_column_info
Purpose: Return the partitioning column name and type for a partitioned table.
Test coverage: Integer, date, timestamp, and UUID (if supported) partitioned tables; lowercase
column name; non-partitioned table raises an error; schema/table name case-insensitivity.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_get_partition_column_info()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_column_name text;
    v_column_type text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_notpartitioned CASCADE';

    -- integer partition: verify column name and type
    CREATE TABLE dba_test.colinfo_int (part_id bigint not null) PARTITION BY RANGE (part_id);
    CREATE TABLE dba_test.colinfo_int_0_100 PARTITION OF dba_test.colinfo_int FOR VALUES FROM (0) TO (100);

    SELECT pci.v_column_name, pci.v_column_type
    INTO v_column_name, v_column_type
    FROM dba.partition_get_partition_column_info('dba_test', 'colinfo_int') AS pci;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('part_id', v_column_name, 'colinfo_int_column_name');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('int8', v_column_type, 'colinfo_int_column_type');

    -- date partition
    CREATE TABLE dba_test.colinfo_date (trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.colinfo_date_p1 PARTITION OF dba_test.colinfo_date FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

    SELECT pci.v_column_name, pci.v_column_type
    INTO v_column_name, v_column_type
    FROM dba.partition_get_partition_column_info('dba_test', 'colinfo_date') AS pci;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('trip_date', v_column_name, 'colinfo_date_column_name');
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_column_type ~ 'date', 'colinfo_date_column_type');

    -- timestamp partition
    CREATE TABLE dba_test.colinfo_ts (trip_ts timestamptz not null) PARTITION BY RANGE (trip_ts);
    CREATE TABLE dba_test.colinfo_ts_p1 PARTITION OF dba_test.colinfo_ts FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

    SELECT pci.v_column_name, pci.v_column_type
    INTO v_column_name, v_column_type
    FROM dba.partition_get_partition_column_info('dba_test', 'colinfo_ts') AS pci;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('trip_ts', v_column_name, 'colinfo_ts_column_name');
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_column_type ~ 'timestamp', 'colinfo_ts_column_type');

    -- non-partitioned table raises an error
    CREATE TABLE dba_test.colinfo_notpartitioned (id bigint);

    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT * FROM dba.partition_get_partition_column_info(''dba_test'', ''colinfo_notpartitioned'')',
        'P0001',
        'colinfo_nonpartitioned_raises_error'
    );

    -- schema and table name lookups are case-insensitive
    SELECT pci.v_column_name
    INTO v_column_name
    FROM dba.partition_get_partition_column_info('DBA_TEST', 'COLINFO_INT') AS pci;

    RETURN QUERY SELECT * FROM dba_test.assert_equals('part_id', v_column_name, 'colinfo_case_insensitive');

    -- UUID partition (optional, requires uuidv7 extension)
    IF NOT dba_test.uuidv7_supported() THEN
        RETURN QUERY SELECT * FROM dba_test.skip('colinfo_uuid_column_name', 'no uuidv7 support');
        RETURN QUERY SELECT * FROM dba_test.skip('colinfo_uuid_column_type', 'no uuidv7 support');
    ELSE
        EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_uuid CASCADE';
        EXECUTE 'CREATE TABLE dba_test.colinfo_uuid (part_uuid uuid not null) PARTITION BY RANGE (part_uuid)';
        EXECUTE $q$ CREATE TABLE dba_test.colinfo_uuid_p1 PARTITION OF dba_test.colinfo_uuid
                    FOR VALUES FROM ('00000000-0000-7000-8000-000000000000')
                               TO   ('ffffffff-ffff-7fff-bfff-ffffffffffff') $q$;

        SELECT pci.v_column_name, pci.v_column_type
        INTO v_column_name, v_column_type
        FROM dba.partition_get_partition_column_info('dba_test', 'colinfo_uuid') AS pci;

        RETURN QUERY SELECT * FROM dba_test.assert_equals('part_uuid', v_column_name, 'colinfo_uuid_column_name');
        RETURN QUERY SELECT * FROM dba_test.assert_true(v_column_type ~ 'uuid', 'colinfo_uuid_column_type');

        EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_uuid CASCADE';
    END IF;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_date CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_ts CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.colinfo_notpartitioned CASCADE';

    RETURN;
END;
$$;
