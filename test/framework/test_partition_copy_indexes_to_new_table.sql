/*
Test: test_partition_copy_indexes_to_new_table
Function under test: dba.partition_copy_indexes_to_new_table
Run: ./test/framework/run_partition_tests.sh test_partition_copy_indexes_to_new_table
Purpose: Validate that copy_indexes_to_new_table correctly copies indexes with the
         p_skip_unique_indexes and p_skip_unique_index_column_name parameters.
Test coverage:
  - Default behavior: all indexes (including unique) are copied.
  - p_skip_unique_indexes=TRUE, no column: all unique non-primary indexes are skipped.
  - p_skip_unique_indexes=TRUE with column: unique indexes NOT containing the column are
    skipped; unique indexes containing the column and primary keys are always copied.
  - p_skip_unique_indexes=FALSE: no indexes skipped even when column name is provided.
  - Index names prefixed with the schema name (<schema>_<table>_<columns>) are copied to the
    target table instead of producing a schema qualified index name on the source table.
  - The access method, a partial index predicate containing a literal % and the target table
    reference survive the copy.
  - An ON ONLY index on a partitioned source table is copied with ON ONLY preserved, so the
    target parent gets an (invalid) parent index and no index is built on its partitions.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_copy_indexes_to_new_table()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INT;
    v_error TEXT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_target CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_src_mammoth CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_src CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_part_source CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.cit_part_target CASCADE';

    -- Source table: PK on (id, partition_col), unique index on email only, unique index on
    -- (partition_col, email), and a plain non-unique index on payload.
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_source (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;
    EXECUTE 'ALTER TABLE dba_test.cit_source ADD CONSTRAINT cit_source_pkey PRIMARY KEY (id, partition_col)';
    EXECUTE 'CREATE UNIQUE INDEX cit_source_email_idx ON dba_test.cit_source (email)';
    EXECUTE 'CREATE UNIQUE INDEX cit_source_partition_col_email_idx ON dba_test.cit_source (partition_col, email)';
    EXECUTE 'CREATE INDEX cit_source_payload_idx ON dba_test.cit_source (payload)';

    -- -------------------------------------------------------------------------
    -- Test 1: default behavior - all indexes including unique ones are copied.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_target (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;

    PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_source', 'cit_target');

    -- Primary key must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'default_primary_key_copied');

    -- Unique index on email must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisunique AND NOT i.indisprimary AND idx.indexdef LIKE '%email%' AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'default_unique_indexes_copied');

    -- Plain index on payload must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_target' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'default_plain_index_copied');

    EXECUTE 'DROP TABLE dba_test.cit_target CASCADE';

    -- -------------------------------------------------------------------------
    -- Test 2: p_skip_unique_indexes=TRUE, no column - all unique non-primary
    --         indexes are skipped; primary key and plain index are kept.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_target (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;

    PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_source', 'cit_target', FALSE, TRUE, NULL);

    -- Primary key must still be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'skip_all_primary_key_kept');

    -- All unique non-primary indexes must be absent
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisunique AND NOT i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'skip_all_unique_indexes_absent');

    -- Plain index on payload must still be present
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_target' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'skip_all_plain_index_kept');

    EXECUTE 'DROP TABLE dba_test.cit_target CASCADE';

    -- -------------------------------------------------------------------------
    -- Test 3: p_skip_unique_indexes=TRUE with p_skip_unique_index_column_name.
    --         Unique indexes NOT containing 'partition_col' are skipped.
    --         Unique indexes containing 'partition_col' and the primary key are kept.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_target (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;

    PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_source', 'cit_target', FALSE, TRUE, 'partition_col');

    -- Primary key must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'skip_with_col_primary_key_kept');

    -- Unique index on email only (no partition_col) must be absent
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisunique AND NOT i.indisprimary
      AND idx.indexdef LIKE '%email%' AND idx.indexdef NOT LIKE '%partition_col%'
      AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'skip_with_col_email_only_unique_absent');

    -- Unique index on (partition_col, email) must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisunique AND NOT i.indisprimary
      AND idx.indexdef LIKE '%partition_col%' AND idx.indexdef LIKE '%email%'
      AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'skip_with_col_composite_unique_kept');

    -- Plain index on payload must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_target' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'skip_with_col_plain_index_kept');

    EXECUTE 'DROP TABLE dba_test.cit_target CASCADE';

    -- -------------------------------------------------------------------------
    -- Test 4: p_skip_unique_indexes=FALSE - no indexes skipped even when column
    --         name is provided.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_target (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;

    PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_source', 'cit_target', FALSE, FALSE, 'partition_col');

    -- Both unique non-primary indexes must be present
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_target'
      AND i.indisunique AND NOT i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'no_skip_all_unique_indexes_copied');

    EXECUTE 'DROP TABLE dba_test.cit_target CASCADE';
    EXECUTE 'DROP TABLE dba_test.cit_source CASCADE';

    -- -------------------------------------------------------------------------
    -- Test 5: index names prefixed with the schema name
    --         (<schema>_<table>_<columns>). This is the shape produced on
    --         the mammoth table by dba.partition_native before it copies the
    --         indexes to the new partitioned parent. A schema || '.' || table
    --         regular expression matches such an index name before the ON clause
    --         because '.' is a wildcard, so the copy has to be built from the
    --         definition instead. Also covers a non-btree access method and a
    --         partial index predicate containing a literal %.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_src_mammoth (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;
    EXECUTE 'ALTER TABLE dba_test.cit_src_mammoth ADD CONSTRAINT dba_test_cit_src_mammoth_pkey PRIMARY KEY (id, partition_col)';
    EXECUTE 'CREATE UNIQUE INDEX dba_test_cit_src_mammoth_partition_col_email_idx ON dba_test.cit_src_mammoth (partition_col, email)';
    EXECUTE 'CREATE INDEX dba_test_cit_src_mammoth_payload_idx ON dba_test.cit_src_mammoth (payload) WHERE payload LIKE ''%pct%''';
    EXECUTE 'CREATE INDEX dba_test_cit_src_mammoth_email_idx ON dba_test.cit_src_mammoth USING hash (email)';

    EXECUTE $sql$
        CREATE TABLE dba_test.cit_src (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            email         text   NOT NULL,
            payload       text
        )
    $sql$;

    -- Capture the failure instead of letting it abort the whole test function, so the
    -- assertions below still report which parts of the copy went wrong.
    BEGIN
        PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_src_mammoth', 'cit_src', FALSE, TRUE, 'partition_col');
        v_error := NULL;
    EXCEPTION WHEN OTHERS THEN
        v_error := SQLERRM;
    END;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_error IS NULL, 'schema_prefixed_copy_succeeds', v_error);

    -- Primary key must be copied
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_src'
      AND i.indisprimary AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'schema_prefixed_primary_key_copied');

    -- Unique index containing the partition column must be copied
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_src'
      AND i.indisunique AND NOT i.indisprimary
      AND idx.indexdef LIKE '%partition_col%' AND idx.indexdef LIKE '%email%'
      AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'schema_prefixed_unique_index_copied');

    -- Partial index must be copied with its predicate intact
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_src'
      AND indexdef LIKE '%(payload)%' AND indexdef LIKE '%pct%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'schema_prefixed_partial_index_copied');

    -- Access method must be preserved
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_src'
      AND indexdef LIKE '%USING hash%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'schema_prefixed_access_method_preserved');

    -- No index name may be schema qualified
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_src'
      AND indexname LIKE '%.%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'schema_prefixed_index_names_not_qualified');

    -- The source table must keep exactly the four indexes it started with
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_src_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count, 'schema_prefixed_source_indexes_untouched');

    EXECUTE 'DROP TABLE dba_test.cit_src CASCADE';
    EXECUTE 'DROP TABLE dba_test.cit_src_mammoth CASCADE';

    -- -------------------------------------------------------------------------
    -- Test 6: the source is a partitioned table whose index was created with
    --         ON ONLY. pg_get_indexdef keeps that keyword, so the copy must keep
    --         it too: dropping ONLY would recurse into the target's partitions
    --         and build an index on every existing partition. The copy is
    --         verified by the parent index being present but invalid (an ON ONLY
    --         index stays invalid until it is attached to child indexes) and by
    --         the target partition carrying no index at all.
    -- -------------------------------------------------------------------------
    EXECUTE $sql$
        CREATE TABLE dba_test.cit_part_source (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            payload       text
        ) PARTITION BY RANGE (partition_col)
    $sql$;
    EXECUTE 'CREATE TABLE dba_test.cit_part_source_0_100 PARTITION OF dba_test.cit_part_source FOR VALUES FROM (0) TO (100)';
    EXECUTE 'CREATE INDEX cit_part_source_payload_idx ON ONLY dba_test.cit_part_source (payload)';

    EXECUTE $sql$
        CREATE TABLE dba_test.cit_part_target (
            id            bigint NOT NULL,
            partition_col bigint NOT NULL,
            payload       text
        ) PARTITION BY RANGE (partition_col)
    $sql$;
    EXECUTE 'CREATE TABLE dba_test.cit_part_target_0_100 PARTITION OF dba_test.cit_part_target FOR VALUES FROM (0) TO (100)';

    -- Capture the failure instead of letting it abort the whole test function, so the
    -- assertions below still report which parts of the copy went wrong.
    BEGIN
        PERFORM dba.partition_copy_indexes_to_new_table('dba_test', 'cit_part_source', 'cit_part_target', FALSE);
        v_error := NULL;
    EXCEPTION WHEN OTHERS THEN
        v_error := SQLERRM;
    END;
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_error IS NULL, 'on_only_copy_succeeds', v_error);

    -- The parent index must be copied to the target parent
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_part_target'
      AND indexdef LIKE '%(payload)%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'on_only_parent_index_copied');

    -- The copied index must be invalid, which only holds when ON ONLY was preserved:
    -- a plain CREATE INDEX on a partitioned table builds the child indexes and ends
    -- up valid.
    SELECT count(*) INTO v_count
    FROM pg_indexes idx
    JOIN pg_class c ON c.relname = idx.indexname
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE idx.schemaname = 'dba_test' AND idx.tablename = 'cit_part_target'
      AND idx.indexdef LIKE '%(payload)%'
      AND i.indisvalid IS FALSE
      AND n.nspname = 'dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'on_only_parent_index_invalid');

    -- The target partition must have no index, proving the copy did not recurse
    SELECT count(*) INTO v_count
    FROM pg_indexes
    WHERE schemaname = 'dba_test' AND tablename = 'cit_part_target_0_100';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'on_only_partition_has_no_index');

    EXECUTE 'DROP TABLE dba_test.cit_part_target CASCADE';
    EXECUTE 'DROP TABLE dba_test.cit_part_source CASCADE';

    RETURN;
END;
$$;
