/*
Test: test_create_optimized_table_copy
Function under test: dba.create_optimized_table_copy
Run: ./test/framework/run_partition_tests.sh test_create_optimized_table_copy
Purpose: Validate that create_optimized_table_copy produces a full table copy with optimized
         column order, copying indexes, foreign keys, check constraints, storage parameters,
         column options, and table owner.
*/

CREATE OR REPLACE FUNCTION dba_test.test_create_optimized_table_copy()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_actual        TEXT;
    v_expected      TEXT;
    v_exists        BOOLEAN;
    v_count         INT;
    v_result        BOOLEAN;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_ref_table CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_quote_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test."cot_target""quoted" CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_part_parent CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_part_copy CASCADE';
    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.cot_trigger_fn() CASCADE';

    EXECUTE $sql$
        CREATE OR REPLACE FUNCTION dba_test.cot_trigger_fn()
        RETURNS TRIGGER LANGUAGE plpgsql AS $trg$ BEGIN RETURN NEW; END; $trg$
    $sql$;

    -- Reference table for foreign key tests
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_ref_table (
            id bigint PRIMARY KEY
        )
    $sql$;

    -- Source table: intentionally bad column order to confirm optimized reordering
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_source (
            flag        boolean NOT NULL,
            small_val   smallint,
            ref_id      bigint REFERENCES dba_test.cot_ref_table(id),
            int_val     integer DEFAULT 42,
            txt         text,
            big_val     bigint
        )
    $sql$;

    -- Primary key
    EXECUTE 'ALTER TABLE dba_test.cot_source ADD CONSTRAINT cot_source_pkey PRIMARY KEY (big_val)';

    -- Extra index
    EXECUTE 'CREATE INDEX cot_source_int_val_idx ON dba_test.cot_source (int_val)';

    -- Check constraint
    EXECUTE 'ALTER TABLE dba_test.cot_source ADD CONSTRAINT cot_source_small_val_chk CHECK (small_val > 0)';

    -- Storage parameter
    EXECUTE 'ALTER TABLE dba_test.cot_source SET (fillfactor = 80)';

    -- Column option
    EXECUTE 'ALTER TABLE ONLY dba_test.cot_source ALTER COLUMN int_val SET (n_distinct = 100)';

    -- Per-column statistics target
    EXECUTE 'ALTER TABLE dba_test.cot_source ALTER COLUMN int_val SET STATISTICS 500';

    -- Extended statistics object
    EXECUTE 'CREATE STATISTICS dba_test.cot_source_ndist (ndistinct) ON int_val, big_val FROM dba_test.cot_source';

    -- Direct trigger (should be copied to target)
    EXECUTE 'CREATE TRIGGER cot_direct_trg AFTER INSERT ON dba_test.cot_source FOR EACH ROW EXECUTE FUNCTION dba_test.cot_trigger_fn()';

    -- Create the copy
    SELECT dba.create_optimized_table_copy('dba_test', 'cot_source', 'dba_test', 'cot_target')
    INTO v_result;

    RETURN QUERY SELECT * FROM dba_test.assert_true(v_result IS TRUE, 'copy_returns_true', 'expected TRUE');

    -- Test 1: target table exists
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'dba_test' AND c.relname = 'cot_target'
    ) INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_exists, 'target_table_exists', 'expected table to exist');

    -- Test 2: column order is optimized (fixed-size types before variable-size)
    SELECT array_to_string(array_agg(attname ORDER BY attnum), ',')
    FROM pg_attribute
    WHERE attrelid = 'dba_test.cot_target'::regclass
        AND attnum > 0
        AND NOT attisdropped
    INTO v_actual;
    v_expected := 'ref_id,big_val,int_val,small_val,flag,txt';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_actual, 'optimized_column_order');

    -- Test 3: column default is copied
    SELECT pg_get_expr(ad.adbin, ad.adrelid)
    FROM pg_attrdef ad
    JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
    WHERE ad.adrelid = 'dba_test.cot_target'::regclass
        AND a.attname = 'int_val'
    INTO v_actual;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('42', v_actual, 'column_default_copied');

    -- Test 4: NOT NULL is copied
    SELECT attnotnull
    FROM pg_attribute
    WHERE attrelid = 'dba_test.cot_target'::regclass
        AND attname = 'flag'
    INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_exists IS TRUE, 'not_null_copied');

    -- Test 5: primary key is copied
    SELECT EXISTS (
        SELECT 1
        FROM pg_indexes
        JOIN pg_class ON pg_class.relname = pg_indexes.indexname
        JOIN pg_index ON pg_class.oid = pg_index.indexrelid
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = 'dba_test'
            AND schemaname = 'dba_test'
            AND tablename = 'cot_target'
            AND indisprimary = TRUE
    ) INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_exists, 'primary_key_copied');

    -- Test 6: extra index is copied
    SELECT count(*)
    FROM pg_indexes
    WHERE schemaname = 'dba_test'
        AND tablename = 'cot_target'
        AND indexname LIKE '%int_val%'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 1, 'extra_index_copied', 'expected >= 1 index on int_val');

    -- Test 7: foreign key constraint is copied
    SELECT count(*)
    FROM pg_constraint
    WHERE conrelid = 'dba_test.cot_target'::regclass
        AND contype = 'f'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 1, 'foreign_key_copied', 'expected >= 1 FK');

    -- Test 8: check constraint is copied and renamed
    SELECT count(*)
    FROM pg_constraint
    WHERE conrelid = 'dba_test.cot_target'::regclass
        AND contype = 'c'
        AND conname LIKE '%cot_target%'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 1, 'check_constraint_copied_and_renamed', 'expected >= 1 check constraint with target name');

    -- Test 9: storage parameter (fillfactor) is copied
    SELECT array_to_string(reloptions, ',')
    FROM pg_class
    WHERE relname = 'cot_target'
        AND relnamespace::regnamespace::text = 'dba_test'
    INTO v_actual;
    RETURN QUERY SELECT * FROM dba_test.assert_true(
        v_actual LIKE '%fillfactor=80%',
        'storage_parameter_copied',
        format('reloptions: %s', v_actual)
    );

    -- Test 10: column option (n_distinct) is copied
    SELECT unnest(attoptions)
    FROM pg_attribute
    WHERE attrelid = 'dba_test.cot_target'::regclass
        AND attname = 'int_val'
        AND attoptions IS NOT NULL
    LIMIT 1
    INTO v_actual;
    RETURN QUERY SELECT * FROM dba_test.assert_true(
        v_actual = 'n_distinct=100',
        'column_option_copied',
        format('attoption: %s', v_actual)
    );

    -- Test 11: per-column statistics target (attstattarget) is copied
    SELECT attstattarget
    FROM pg_attribute
    WHERE attrelid = 'dba_test.cot_target'::regclass
        AND attname = 'int_val'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(500, v_count, 'attstattarget_copied');

    -- Test 12: extended statistics object is copied with a target-derived name
    SELECT count(*)
    FROM pg_statistic_ext
    WHERE stxrelid = 'dba_test.cot_target'::regclass
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'extended_stats_copied');

    -- Test 13: quoted target identifier is safely rendered in extended-statistics DDL
    EXECUTE 'CREATE TABLE dba_test.cot_quote_source (first_col integer, second_col integer)';
    EXECUTE 'CREATE STATISTICS dba_test.cot_quote_source_ndist (ndistinct) ON first_col, second_col FROM dba_test.cot_quote_source';
    PERFORM dba.create_optimized_table_copy('dba_test', 'cot_quote_source', 'dba_test', 'cot_target"quoted');

    SELECT count(*)
    FROM pg_statistic_ext
    WHERE stxrelid = 'dba_test."cot_target""quoted"'::regclass
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'quoted_target_extended_stats_copied');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_quote_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test."cot_target""quoted" CASCADE';

    -- Test 14: owner matches source
    DECLARE
        v_source_owner NAME;
        v_target_owner NAME;
    BEGIN
        SELECT tableowner FROM pg_tables WHERE schemaname = 'dba_test' AND tablename = 'cot_source' INTO v_source_owner;
        SELECT tableowner FROM pg_tables WHERE schemaname = 'dba_test' AND tablename = 'cot_target' INTO v_target_owner;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(v_source_owner::text, v_target_owner::text, 'owner_copied');
    END;

    -- Test 15: direct trigger (tgparentid = 0) is copied to target
    SELECT EXISTS (
        SELECT 1 FROM pg_trigger t
        WHERE t.tgrelid = 'dba_test.cot_target'::regclass
            AND t.tgname = 'cot_direct_trg'
            AND NOT t.tgisinternal
            AND t.tgconstraint = 0
    ) INTO v_exists;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_exists, 'direct_trigger_copied', 'expected direct trigger to be copied to target');

    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_ref_table CASCADE';

    -- Test 16: cloned trigger (tgparentid != 0) is NOT copied; ATTACH PARTITION will re-create it
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_part_parent (id bigint, val integer) PARTITION BY RANGE (id)
    $sql$;
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_part_child PARTITION OF dba_test.cot_part_parent
        FOR VALUES FROM (1) TO (1000000)
    $sql$;
    -- Row-level trigger on parent: PostgreSQL auto-clones it to cot_part_child (tgparentid != 0)
    EXECUTE 'CREATE TRIGGER cot_parent_trg AFTER INSERT ON dba_test.cot_part_parent FOR EACH ROW EXECUTE FUNCTION dba_test.cot_trigger_fn()';

    PERFORM dba.create_optimized_table_copy('dba_test', 'cot_part_child', 'dba_test', 'cot_part_copy');

    SELECT count(*) FROM pg_trigger t
    WHERE t.tgrelid = 'dba_test.cot_part_copy'::regclass
        AND NOT t.tgisinternal
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(
        v_count = 0,
        'cloned_trigger_not_copied',
        format('expected 0 triggers on copy (clone is re-added by ATTACH PARTITION), got %s', v_count)
    );

    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_part_copy CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_part_parent CASCADE';
    EXECUTE 'DROP FUNCTION IF EXISTS dba_test.cot_trigger_fn() CASCADE';

    -- Test 14: partition-inherited check constraints are copied and ATTACH PARTITION succeeds
    --
    -- When a constraint is defined on the parent partitioned table, existing partitions have it
    -- with conislocal = false and conparentid != 0. A new table created via create_optimized_table_copy
    -- from that partition must also get the constraint, otherwise ATTACH PARTITION will fail with
    -- "child table is missing constraint".
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_chk_parent CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_chk_copy CASCADE';

    EXECUTE $sql$
        CREATE TABLE dba_test.cot_chk_parent (
            id    bigint NOT NULL,
            ts    timestamp NOT NULL,
            payload text
        ) PARTITION BY RANGE (ts)
    $sql$;
    EXECUTE $sql$
        ALTER TABLE dba_test.cot_chk_parent ADD CONSTRAINT cot_chk_id_or_payload
            CHECK (id > 0 OR payload IS NOT NULL)
    $sql$;
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_chk_child PARTITION OF dba_test.cot_chk_parent
            FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
    $sql$;

    PERFORM dba.create_optimized_table_copy('dba_test', 'cot_chk_child', 'dba_test', 'cot_chk_copy');

    -- The partition-inherited constraint must exist on the copy
    SELECT count(*)
    FROM pg_constraint
    WHERE conrelid = 'dba_test.cot_chk_copy'::regclass
        AND contype = 'c'
        AND conname = 'cot_chk_id_or_payload'
    INTO v_count;
    RETURN QUERY SELECT * FROM dba_test.assert_true(
        v_count = 1,
        'partition_inherited_check_constraint_copied',
        format('expected constraint cot_chk_id_or_payload on copy, found %s', v_count)
    );

    -- ATTACH PARTITION must succeed without error; catch any error and report as test failure
    DECLARE
        v_attach_ok BOOLEAN := FALSE;
    BEGIN
        ALTER TABLE dba_test.cot_chk_parent
            ATTACH PARTITION dba_test.cot_chk_copy
            FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
        v_attach_ok := TRUE;
        RETURN QUERY SELECT * FROM dba_test.assert_true(
            v_attach_ok,
            'attach_partition_with_inherited_constraint_succeeds',
            'expected ATTACH PARTITION to succeed when partition-inherited constraint is present'
        );
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT * FROM dba_test.assert_true(
            FALSE,
            'attach_partition_with_inherited_constraint_succeeds',
            format('ATTACH PARTITION failed: %s', SQLERRM)
        );
    END;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_chk_parent CASCADE';

    -- Test 17: stored generated columns are copied as generated columns (not as DEFAULT) and
    -- the copy can be attached as a partition.
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_gen_parent CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_gen_copy CASCADE';

    EXECUTE $sql$
        CREATE TABLE dba_test.cot_gen_parent (
            id        bigint NOT NULL,
            ts        timestamptz NOT NULL DEFAULT now(),
            doc       varchar NOT NULL,
            doc_kind  varchar GENERATED ALWAYS AS (CAST(doc AS jsonb) ->> 'kind') STORED,
            PRIMARY KEY (id, ts)
        ) PARTITION BY RANGE (ts)
    $sql$;
    EXECUTE $sql$
        CREATE TABLE dba_test.cot_gen_child PARTITION OF dba_test.cot_gen_parent
            FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
    $sql$;

    DECLARE
        v_gen_ok BOOLEAN := FALSE;
    BEGIN
        PERFORM dba.create_optimized_table_copy('dba_test', 'cot_gen_child', 'dba_test', 'cot_gen_copy');

        SELECT attgenerated::text
        FROM pg_attribute
        WHERE attrelid = 'dba_test.cot_gen_copy'::regclass
            AND attname = 'doc_kind'
        INTO v_actual;
        RETURN QUERY SELECT * FROM dba_test.assert_equals('s', v_actual, 'generated_column_copied_as_generated');

        ALTER TABLE dba_test.cot_gen_parent
            ATTACH PARTITION dba_test.cot_gen_copy
            FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
        v_gen_ok := TRUE;
        RETURN QUERY SELECT * FROM dba_test.assert_true(
            v_gen_ok,
            'attach_partition_with_generated_column_succeeds',
            'expected ATTACH PARTITION to succeed for a copy with generated columns'
        );
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT * FROM dba_test.assert_true(
            FALSE,
            'attach_partition_with_generated_column_succeeds',
            format('copy/attach failed: %s', SQLERRM)
        );
    END;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_gen_parent CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cot_gen_copy CASCADE';

    RETURN;
END;
$$;
