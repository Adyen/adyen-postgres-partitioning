/*
Test: test_partition_maintenance
Function under test: partition_maintenance.sql (maintenance script)
Run: ./test/framework/run_partition_tests.sh test_partition_maintenance
Purpose: Run the maintenance orchestration across partition utilities.
Test coverage: Sets up a partitioned fixture and runs the same maintenance steps (extend, add constraints, detach, drop) used by the maintenance script.
*/

CREATE OR REPLACE PROCEDURE dba_test.partition_maintenance_exec()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.maint_int CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.maint_date CASCADE';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'maint_int' AND schema_name = 'dba_test';
    DELETE FROM dba.partition_configuration WHERE lower(table_name) = 'maint_date' AND schema_name = 'dba_test';
    DELETE FROM dba.detached_partitions WHERE parent_relname = 'maint_date' AND schema = 'dba_test';

    CREATE TABLE dba_test.maint_int (id bigint not null, record_date date not null) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.maint_int_0_10 PARTITION OF dba_test.maint_int FOR VALUES FROM (0) TO (10);

    CREATE TABLE dba_test.maint_date (id bigint, trip_date date not null) PARTITION BY RANGE (trip_date);
    CREATE TABLE dba_test.maint_date_20200101_20200201 PARTITION OF dba_test.maint_date FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE dba_test.maint_date_20990101_20990201 PARTITION OF dba_test.maint_date FOR VALUES FROM ('2099-01-01') TO ('2099-02-01');

    INSERT INTO dba.partition_configuration VALUES ('dba_test','maint_int','{"auto-maintenance": true, "nr": 1, "detach":"365 days", "drop_detached":"5 days"}');
    INSERT INTO dba.partition_configuration VALUES ('dba_test','maint_date','{"detach":"365 days"}');

    PERFORM dba.partition_extend_all_partitioned_tables();
    SELECT count(*) FROM pg_inherits WHERE inhparent = 'dba_test.maint_int'::regclass INTO v_count;
    PERFORM dba_test.record_result('partition_maintenance_smoke', CASE WHEN v_count >= 3 THEN 'PASS' ELSE 'FAIL' END, NULL);

    INSERT INTO dba_test.maint_int VALUES (1, current_date);
    PERFORM dba.partition_add_constraints('dba_test','maint_int','record_date','record_date');
    SELECT count(*) FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'maint_int_0_10' AND t.relnamespace = 'dba_test'::regnamespace AND c.conname = 'maint_int_0_10_record_date_min'
    INTO v_count;
    PERFORM dba_test.record_result('partition_maintenance_add_constraints_min', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    INSERT INTO dba_test.maint_int VALUES (9, current_date);
    PERFORM dba.partition_add_constraints('dba_test','maint_int','record_date','record_date');
    SELECT count(*) FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'maint_int_0_10' AND t.relnamespace = 'dba_test'::regnamespace AND c.conname = 'maint_int_0_10_record_date_max'
    INTO v_count;
    PERFORM dba_test.record_result('partition_maintenance_add_constraints_max', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- A mixed-case table_name with a date_constraint config entry must not break the
    -- constraint step of partition_maintenance.sql (mirrored below; the runner does not
    -- load that script because it executes immediately).
    INSERT INTO dba.partition_configuration VALUES (
        'dba_test',
        'MAINT_INT',
        '{"date_constraint":{"marker":"recdate","constraint_column":"record_date"}}'
    );

    BEGIN
        WITH config AS (
            SELECT q.schema_name, q.table_name, d.key, d.value::json
            FROM dba.partition_configuration q
            JOIN json_each_text(configuration) d ON true
            ORDER BY 1, 2
        ),
        constraint_set AS (
            SELECT * FROM config WHERE key = 'date_constraint'
        ),
        applied AS (
            SELECT dba.partition_add_constraints(schema_name, table_name, marker, constraint_column)
            FROM constraint_set, json_to_record(constraint_set.value) AS x(constraint_column text, marker text)
        )
        SELECT count(*) INTO v_count FROM applied;

        SELECT count(*) FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'maint_int_0_10'
          AND c.conname IN ('maint_int_0_10_recdate_min', 'maint_int_0_10_recdate_max')
        INTO v_count;
        PERFORM dba_test.record_result(
            'partition_maintenance_constraints_mixed_case',
            CASE WHEN v_count = 2 THEN 'PASS' ELSE 'FAIL' END,
            NULL
        );
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('partition_maintenance_constraints_mixed_case', 'FAIL', SQLERRM);
    END;

    CALL dba.partition_detach_partitions_without_uuidv7();
    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'maint_date' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result('partition_maintenance_detach_date', CASE WHEN v_count = 1 THEN 'PASS' ELSE 'FAIL' END, NULL);

    -- A mixed-case table_name in dba.partition_configuration must still be matched by the
    -- drop-detached step of partition_maintenance.sql. The runner does not load that script
    -- (it executes immediately), so the step is mirrored here, adapted to PL/pgSQL with a
    -- count instead of a bare SELECT.
    UPDATE dba.detached_partitions SET detached_date = current_date - 5 WHERE parent_relname = 'maint_date' AND schema = 'dba_test';
    INSERT INTO dba.partition_configuration VALUES ('dba_test','MAINT_DATE','{"drop_detached":"5 days"}');

    WITH config AS (
        SELECT q.schema_name, q.table_name, d.key, d.value::text
        FROM dba.partition_configuration q
        JOIN json_each_text(configuration) d ON true
        ORDER BY 1, 2
    ),
    drop_detach_set AS (
        SELECT *
        FROM config
        WHERE key = 'drop_detached'
    ),
    dropped AS (
        SELECT dba.partition_drop_detached_partition(schema_name, table_name, partition_relname) AS is_dropped
        FROM drop_detach_set
        LEFT JOIN LATERAL (
            SELECT partition_relname FROM dba.detached_partitions
            WHERE LOWER(parent_relname) = LOWER(drop_detach_set.table_name)
                AND detached_date <= current_date - GREATEST(drop_detach_set.value::interval, '4 days'::interval)
                AND LOWER(schema) = LOWER(drop_detach_set.schema_name)
        ) drop_table_set ON 1=1
        WHERE partition_relname IS NOT NULL
    )
    SELECT count(*) INTO v_count FROM dropped WHERE is_dropped;

    SELECT count(*) FROM dba.detached_partitions WHERE parent_relname = 'maint_date' AND schema = 'dba_test' INTO v_count;
    PERFORM dba_test.record_result(
        'partition_maintenance_drop_mixed_case',
        CASE WHEN v_count = 0 AND to_regclass('dba_test.maint_date_20200101_20200201') IS NULL THEN 'PASS' ELSE 'FAIL' END,
        NULL
    );

END;
$$;
