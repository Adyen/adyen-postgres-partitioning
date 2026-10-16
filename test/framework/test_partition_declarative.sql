/*
Test: test_partition_declarative
Function under test: dba.partition_native
Run: ./test/framework/run_partition_tests.sh test_partition_declarative_*

Covers (v_nopk=TRUE paths intentionally excluded):
  guards, column types, structure, indexes, index-rename edge cases, triggers,
  column/table options, column statistics, outgoing FK, incoming FK from a
  regular table, incoming FK from a partitioned table, statistics objects
  (attstattarget and extended statistics on parent, mammoth, and new partition),
  and mixed-case identifiers (uppercase arguments normalized to lowercase).
*/

-- Shared trigger function used by all trigger tests.
CREATE OR REPLACE FUNCTION dba_test.pnd_trg_fn()
RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;


-- ===========================================================================
-- 1. GUARDS
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_guards()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_result BOOLEAN;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_guard_text    CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_guard_float   CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_guard_bnd     CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_guard_uq      CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_guard_pk_only CASCADE';

    -- Unsupported column type: text
    CREATE TABLE dba_test.pnd_guard_text (id bigint, k text, PRIMARY KEY (id, k));
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_text'',''k'',''a'',''b'',''1'')',
        'P0001', 'guard_unsupported_type_text');

    -- Unsupported column type: float4
    CREATE TABLE dba_test.pnd_guard_float (id bigint, k float4, PRIMARY KEY (id));
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_float'',''k'',''1'',''2'',''1'')',
        'P0001', 'guard_unsupported_type_float');

    -- Data AT the endkey boundary for date (row >= endkey is a violation)
    CREATE TABLE dba_test.pnd_guard_bnd (id bigint, k date, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_guard_bnd VALUES (1, '2020-02-01');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_bnd'',''k'',''2020-01-01'',''2020-02-01'',''1 month'')',
        'P0001', 'guard_boundary_violation_date_at_endkey');

    EXECUTE 'DROP TABLE dba_test.pnd_guard_bnd CASCADE';
    -- Data above endkey+1 for int (value 101 >= 101 → violation)
    CREATE TABLE dba_test.pnd_guard_bnd (id bigint, k bigint, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_guard_bnd VALUES (1, 101);
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_bnd'',''k'',''0'',''100'',''10'')',
        'P0001', 'guard_boundary_violation_int_above');

    EXECUTE 'DROP TABLE dba_test.pnd_guard_bnd CASCADE';
    -- Data exactly at endkey for int (value=100, endkey becomes 101, 100<101 → OK)
    CREATE TABLE dba_test.pnd_guard_bnd (id bigint, k bigint, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_guard_bnd VALUES (1, 100);
    SELECT dba.partition_native('dba_test','pnd_guard_bnd','k','0','100','10') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(TRUE, v_result, 'guard_boundary_int_at_endkey_ok');

    -- Unique index without partition key, no flag → raises
    CREATE TABLE dba_test.pnd_guard_uq (id bigint, k bigint, email text, PRIMARY KEY (id, k));
    CREATE UNIQUE INDEX ON dba_test.pnd_guard_uq (email);
    INSERT INTO dba_test.pnd_guard_uq VALUES (1, 1, 'x@x.com');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_uq'',''k'',''0'',''100'',''10'')',
        'P0001', 'guard_unique_no_flag_raises');

    EXECUTE 'DROP TABLE dba_test.pnd_guard_uq CASCADE';
    -- NULL flag is also not an explicit opt-in → raises
    CREATE TABLE dba_test.pnd_guard_uq (id bigint, k bigint, email text, PRIMARY KEY (id, k));
    CREATE UNIQUE INDEX ON dba_test.pnd_guard_uq (email);
    INSERT INTO dba_test.pnd_guard_uq VALUES (1, 1, 'x@x.com');
    RETURN QUERY SELECT * FROM dba_test.assert_raises(
        'SELECT dba.partition_native(''dba_test'',''pnd_guard_uq'',''k'',''0'',''100'',''10'',false,true,NULL)',
        'P0001', 'guard_unique_null_flag_raises');

    -- Table with ONLY a PK including partition key → no guard fires
    CREATE TABLE dba_test.pnd_guard_pk_only (id bigint, k bigint, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_guard_pk_only VALUES (1, 1);
    SELECT dba.partition_native('dba_test','pnd_guard_pk_only','k','0','100','10') INTO v_result;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(TRUE, v_result, 'guard_pk_only_no_guard_fires');

    RETURN;
END; $$;


-- ===========================================================================
-- 2. COLUMN TYPES AND BOUNDARY CALCULATION
-- Use the same two-capture-group regex pattern as the proven existing tests.
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_col_types()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_lower_m  text;
    v_upper_m  text;
    v_lower_n  text;
    v_upper_n  text;
    v_count    int;
    v_pat      text := '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*';
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_type_date  CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_type_int   CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_type_ts    CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_type_tstz  CASCADE';

    -- ---- date ----
    CREATE TABLE dba_test.pnd_type_date (id bigint, k date, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_type_date VALUES (1, '2020-01-15');
    PERFORM dba.partition_native('dba_test','pnd_type_date','k','2020-01-01','2020-02-01','1 month');

    SELECT count(*) INTO v_count FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid=pt.partrelid
    WHERE c.relname='pnd_type_date' AND c.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'col_date_is_partitioned');

    SELECT count(*) INTO v_count FROM pg_inherits i
    JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_date' AND p.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'col_date_two_partitions');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_m, v_upper_m
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_date' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_date_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-01-01', v_lower_m, 'col_date_mammoth_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-01', v_upper_m, 'col_date_mammoth_upper');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_n, v_upper_n
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_date' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_date_20200201_20200301';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-01', v_lower_n, 'col_date_new_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-03-01', v_upper_n, 'col_date_new_upper');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_m::date, v_lower_n::date, 'col_date_contiguous');

    -- ---- bigint ----
    CREATE TABLE dba_test.pnd_type_int (id bigint, k bigint, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_type_int VALUES (1, 50);
    PERFORM dba.partition_native('dba_test','pnd_type_int','k','0','100','50');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_m, v_upper_m
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_int' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_int_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('0',   v_lower_m, 'col_int_mammoth_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('101', v_upper_m, 'col_int_mammoth_upper');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_n, v_upper_n
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_int' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_int_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('101', v_lower_n, 'col_int_new_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('151', v_upper_n, 'col_int_new_upper');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_m::bigint, v_lower_n::bigint, 'col_int_contiguous');

    -- ---- timestamp ----
    CREATE TABLE dba_test.pnd_type_ts (id bigint, k timestamp, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_type_ts VALUES (1, '2020-01-15 12:00:00');
    PERFORM dba.partition_native('dba_test','pnd_type_ts','k','2020-01-01','2020-02-01','1 month');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_m, v_upper_m
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_ts' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_ts_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-01-01 00:00:00', v_lower_m, 'col_ts_mammoth_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-02 00:00:00', v_upper_m, 'col_ts_mammoth_upper');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_n, v_upper_n
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_ts' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname LIKE 'pnd_type_ts_20200202%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('2020-02-02 00:00:00', v_lower_n, 'col_ts_new_lower');
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_m::timestamp, v_lower_n::timestamp, 'col_ts_contiguous');

    -- ---- timestamptz ----
    CREATE TABLE dba_test.pnd_type_tstz (id bigint, k timestamptz, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_type_tstz VALUES (1, '2020-01-15 00:00:00+00');
    PERFORM dba.partition_native('dba_test','pnd_type_tstz','k','2020-01-01','2020-02-01','1 month');

    SELECT count(*) INTO v_count FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid=pt.partrelid
    WHERE c.relname='pnd_type_tstz' AND c.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'col_tstz_is_partitioned');

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1],
           (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[2]
    INTO v_lower_m, v_upper_m
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_tstz' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname='pnd_type_tstz_mammoth';

    SELECT (regexp_match(pg_get_expr(c.relpartbound,c.oid),v_pat))[1]
    INTO v_lower_n
    FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_type_tstz' AND p.relnamespace='dba_test'::regnamespace
      AND c.relname LIKE 'pnd_type_tstz_20200202%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_upper_m::timestamptz, v_lower_n::timestamptz, 'col_tstz_contiguous');

    RETURN;
END; $$;


-- ===========================================================================
-- 3. STRUCTURE
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_structure()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_count int;
    v_name  text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_struct CASCADE';

    CREATE TABLE dba_test.pnd_struct (id bigint, k bigint, payload text, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_struct VALUES (1, 50, 'hello');
    PERFORM dba.partition_native('dba_test','pnd_struct','k','0','100','50');

    -- Original table no longer exists as a plain (non-partitioned) table
    SELECT count(*) INTO v_count FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_struct' AND c.relkind != 'p';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'struct_original_not_plain_table');

    -- Mammoth exists
    SELECT count(*) INTO v_count FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_struct_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_mammoth_exists');

    -- Parent is a partitioned table
    SELECT count(*) INTO v_count FROM pg_partitioned_table pt JOIN pg_class c ON c.oid=pt.partrelid
    WHERE c.relname='pnd_struct' AND c.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_parent_is_partitioned');

    -- Partitioned by the correct column
    SELECT a.attname INTO v_name
    FROM pg_partitioned_table pt JOIN pg_class c ON c.oid=pt.partrelid
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=ANY(pt.partattrs::int2[])
    WHERE c.relname='pnd_struct' AND c.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('k', v_name, 'struct_partition_key_column');

    -- Exactly 2 partitions
    SELECT count(*) INTO v_count FROM pg_inherits i
    JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_struct' AND p.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'struct_exactly_two_partitions');

    -- Mammoth is a child of parent
    SELECT count(*) INTO v_count FROM pg_inherits i
    JOIN pg_class p ON p.oid=i.inhparent JOIN pg_class c ON c.oid=i.inhrelid
    WHERE p.relname='pnd_struct' AND c.relname='pnd_struct_mammoth'
      AND p.relnamespace='dba_test'::regnamespace;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_mammoth_is_child');

    -- Data from before partitioning accessible via parent
    SELECT count(*) INTO v_count FROM dba_test.pnd_struct WHERE id=1 AND k=50;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_data_via_parent');

    -- Data lives in the mammoth
    SELECT count(*) INTO v_count FROM dba_test.pnd_struct_mammoth WHERE id=1 AND k=50;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_data_in_mammoth');

    -- New partition is empty
    SELECT count(*) INTO v_count FROM dba_test.pnd_struct_101_151;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'struct_new_partition_empty');

    -- Insert routes to new partition
    INSERT INTO dba_test.pnd_struct VALUES (2, 110, 'world');
    SELECT count(*) INTO v_count FROM dba_test.pnd_struct_101_151 WHERE id=2;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'struct_insert_routes_to_new_partition');

    -- mammoth_check absent from mammoth after attach
    SELECT count(*) INTO v_count FROM pg_constraint
    WHERE conrelid='dba_test.pnd_struct_mammoth'::regclass AND conname LIKE '%mammoth_check%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'struct_mammoth_check_absent_from_mammoth');

    -- mammoth_check absent from new partition
    SELECT count(*) INTO v_count FROM pg_constraint
    WHERE conrelid='dba_test.pnd_struct_101_151'::regclass AND conname LIKE '%mammoth_check%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'struct_mammoth_check_absent_from_new_partition');

    -- Parent has an owner
    SELECT tableowner INTO v_name FROM pg_tables
    WHERE schemaname='dba_test' AND tablename='pnd_struct';
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_name IS NOT NULL, 'struct_parent_has_owner');

    RETURN;
END; $$;


-- ===========================================================================
-- 4. INDEXES
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_indexes()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_idx CASCADE';

    -- PK(id,k), unique on email (no key), unique on (k,email) (has key), plain on payload
    CREATE TABLE dba_test.pnd_idx (id bigint, k bigint, email text, payload text, PRIMARY KEY (id, k));
    CREATE UNIQUE INDEX pnd_idx_email_idx   ON dba_test.pnd_idx (email);
    CREATE UNIQUE INDEX pnd_idx_k_email_idx ON dba_test.pnd_idx (k, email);
    CREATE INDEX        pnd_idx_payload_idx ON dba_test.pnd_idx (payload);
    INSERT INTO dba_test.pnd_idx VALUES (1, 1, 'a@a.com', 'x');
    PERFORM dba.partition_native('dba_test','pnd_idx','k','0','100','50', false, true, true);

    -- PK on parent
    SELECT count(*) INTO v_count
    FROM pg_indexes idx JOIN pg_class c ON c.relname=idx.indexname
    JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE idx.schemaname='dba_test' AND idx.tablename='pnd_idx' AND i.indisprimary AND n.nspname='dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_pk_on_parent');

    -- Plain index on parent
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_plain_on_parent');

    -- Unique index including partition key on parent (use indkey to check column membership)
    SELECT count(*) INTO v_count
    FROM pg_index i
    JOIN pg_class ic ON ic.oid=i.indexrelid
    JOIN pg_class tc ON tc.oid=i.indrelid
    JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx'
      AND i.indisunique AND NOT i.indisprimary
      AND (SELECT a.attnum FROM pg_attribute a WHERE a.attrelid=tc.oid AND a.attname='k') = ANY(i.indkey);
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_unique_with_key_on_parent');

    -- Unique index WITHOUT partition key absent from parent
    SELECT count(*) INTO v_count
    FROM pg_indexes idx JOIN pg_class c ON c.relname=idx.indexname
    JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE idx.schemaname='dba_test' AND idx.tablename='pnd_idx'
      AND i.indisunique AND NOT i.indisprimary
      AND idx.indexdef LIKE '%(email)%' AND n.nspname='dba_test';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'idx_unique_without_key_absent_from_parent');

    -- Unique index without key present on mammoth
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_mammoth' AND indexdef LIKE '%(email)%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_unique_without_key_on_mammoth');

    -- Unique index without key present on new partition
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_101_151' AND indexdef LIKE '%(email)%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_unique_without_key_on_new_partition');

    -- Mammoth: all index names contain 'mammoth'
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_mammoth' AND indexname NOT LIKE '%mammoth%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'idx_mammoth_all_names_have_mammoth');

    -- Parent: no index name contains 'mammoth'
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx' AND indexname LIKE '%mammoth%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'idx_parent_no_mammoth_in_names');

    -- Mammoth has all 4 indexes
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count, 'idx_mammoth_all_four_indexes');

    -- New partition also has all 4 indexes
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(4, v_count, 'idx_new_partition_all_four_indexes');

    RETURN;
END; $$;


-- ===========================================================================
-- 5. INDEXES - TABLE WITHOUT UNIQUE NON-PK INDEXES
--    No p_allow_skipping_unique_indexes flag required.
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_idx_without_unique()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_idx_nou CASCADE';

    -- PK(id,k) + plain index only; no unique non-PK indexes
    CREATE TABLE dba_test.pnd_idx_nou (id bigint, k bigint, payload text, PRIMARY KEY (id, k));
    CREATE INDEX pnd_idx_nou_payload_idx ON dba_test.pnd_idx_nou (payload);
    INSERT INTO dba_test.pnd_idx_nou VALUES (1, 1, 'x');
    -- No p_allow_skipping_unique_indexes flag needed
    PERFORM dba.partition_native('dba_test','pnd_idx_nou','k','0','100','50');

    -- PK on parent
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_nou' AND i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'nou_pk_on_parent');

    -- Plain index on parent
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_nou' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'nou_plain_on_parent');

    -- No unique non-PK index on parent
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_nou' AND i.indisunique AND NOT i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'nou_no_unique_nonpk_on_parent');

    -- Mammoth has 2 indexes (PK + plain)
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_nou_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'nou_mammoth_two_indexes');

    -- New partition has 2 indexes
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_nou_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'nou_new_partition_two_indexes');

    -- Plain index name on mammoth contains 'mammoth'
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_nou_mammoth'
      AND indexdef LIKE '%payload%' AND indexname LIKE '%mammoth%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'nou_mammoth_plain_name_has_mammoth');

    RETURN;
END; $$;


-- ===========================================================================
-- 6. INDEXES - TABLE WHERE ALL UNIQUE INDEXES CONTAIN THE PARTITION KEY
--    No p_allow_skipping_unique_indexes flag required because no unique index
--    needs to be skipped; all are valid on the partitioned parent.
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_idx_unique_all_have_key()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_idx_ahk CASCADE';

    -- PK(id,k), unique on (k,email) (has key), plain on payload
    CREATE TABLE dba_test.pnd_idx_ahk (id bigint, k bigint, email text, payload text, PRIMARY KEY (id, k));
    CREATE UNIQUE INDEX pnd_idx_ahk_k_email_idx ON dba_test.pnd_idx_ahk (k, email);
    CREATE INDEX        pnd_idx_ahk_payload_idx  ON dba_test.pnd_idx_ahk (payload);
    INSERT INTO dba_test.pnd_idx_ahk VALUES (1, 1, 'a@a.com', 'x');
    -- No flag required: the only unique non-PK index contains the partition key
    PERFORM dba.partition_native('dba_test','pnd_idx_ahk','k','0','100','50');

    -- PK on parent
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_ahk' AND i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'ahk_pk_on_parent');

    -- Unique non-PK index on parent (the one with k)
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_ahk' AND i.indisunique AND NOT i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'ahk_unique_with_key_on_parent');

    -- Plain index on parent
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_ahk' AND indexdef LIKE '%payload%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'ahk_plain_on_parent');

    -- Mammoth has all 3 indexes (PK + unique(k,email) + plain)
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_ahk_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'ahk_mammoth_three_indexes');

    -- New partition has all 3 indexes
    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_idx_ahk_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(3, v_count, 'ahk_new_partition_three_indexes');

    -- Unique index present on mammoth
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_ahk_mammoth' AND i.indisunique AND NOT i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'ahk_unique_on_mammoth');

    -- Unique index present on new partition
    SELECT count(*) INTO v_count
    FROM pg_index i JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace n ON n.oid=tc.relnamespace
    WHERE n.nspname='dba_test' AND tc.relname='pnd_idx_ahk_101_151' AND i.indisunique AND NOT i.indisprimary;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'ahk_unique_on_new_partition');

    RETURN;
END; $$;


-- ===========================================================================
-- 7. INDEX RENAME EDGE CASES
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_idx_rename()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_count int;
    v_len   int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_rename_noname CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_rename_longtbl CASCADE';

    -- Index whose name does NOT contain the table name → counter-based fallback _idx_1
    CREATE TABLE dba_test.pnd_rename_noname (id bigint, k bigint, col1 text, PRIMARY KEY (id, k));
    EXECUTE 'CREATE INDEX standalone_custom_idx ON dba_test.pnd_rename_noname (col1)';
    INSERT INTO dba_test.pnd_rename_noname VALUES (1, 1, 'a');
    PERFORM dba.partition_native('dba_test','pnd_rename_noname','k','0','100','50');

    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test' AND tablename='pnd_rename_noname_mammoth'
      AND indexname='pnd_rename_noname_mammoth_idx_1';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'idx_rename_no_tablename_gets_counter');

    -- Long table name: PK index name after substitution exceeds 63 chars → truncated to <=63
    -- Table name = 52 chars → PK = 57 chars → after sub = 65 > 63
    EXECUTE $sql$
        CREATE TABLE dba_test.pnd_rename_longtbl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
            (id bigint, k bigint, PRIMARY KEY (id, k))
    $sql$;
    EXECUTE $sql$INSERT INTO dba_test.pnd_rename_longtbl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa VALUES (1,1)$sql$;
    EXECUTE $sql$
        SELECT dba.partition_native(
            'dba_test','pnd_rename_longtbl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','k','0','100','50')
    $sql$;

    SELECT max(length(indexname)) INTO v_len FROM pg_indexes
    WHERE schemaname='dba_test'
      AND tablename='pnd_rename_longtbl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_len <= 63, 'idx_rename_long_name_within_63');

    SELECT count(*) INTO v_count FROM pg_indexes
    WHERE schemaname='dba_test'
      AND tablename='pnd_rename_longtbl_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count > 0, 'idx_rename_long_mammoth_has_indexes');

    RETURN;
END; $$;


-- ===========================================================================
-- 6. TRIGGERS
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_triggers()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_trg       CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_trg_nomov CASCADE';

    -- --- Table with three trigger types ---
    CREATE TABLE dba_test.pnd_trg (id bigint, k bigint, PRIMARY KEY (id, k));

    -- Row-level BEFORE trigger → must be moved to parent; cloned to partitions
    EXECUTE $sql$
        CREATE TRIGGER pnd_trg_row
        BEFORE INSERT ON dba_test.pnd_trg
        FOR EACH ROW EXECUTE FUNCTION dba_test.pnd_trg_fn()
    $sql$;

    -- Statement-level AFTER trigger → must be moved to parent; NOT cloned (stmt-level)
    EXECUTE $sql$
        CREATE TRIGGER pnd_trg_stmt
        AFTER INSERT ON dba_test.pnd_trg
        FOR EACH STATEMENT EXECUTE FUNCTION dba_test.pnd_trg_fn()
    $sql$;

    -- Insert BEFORE creating the deferred constraint trigger to avoid pending trigger events.
    INSERT INTO dba_test.pnd_trg VALUES (1, 1);

    -- Constraint trigger (tgconstraint != 0) → excluded from tmp_trgs; stays on mammoth
    EXECUTE $sql$
        CREATE CONSTRAINT TRIGGER pnd_trg_constraint
        AFTER INSERT ON dba_test.pnd_trg
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION dba_test.pnd_trg_fn()
    $sql$;
    PERFORM dba.partition_native('dba_test','pnd_trg','k','0','100','50');

    -- Row-level trigger is a DIRECT trigger (tgparentid=0) on parent
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg'::regclass
      AND tgname='pnd_trg_row' AND tgparentid=0 AND NOT tgisinternal AND tgconstraint=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_row_direct_on_parent');

    -- Row-level trigger on mammoth is a CLONE (tgparentid != 0); cloned triggers have tgisinternal=true
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_mammoth'::regclass
      AND tgname='pnd_trg_row' AND tgparentid != 0 AND tgconstraint=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_row_cloned_on_mammoth');

    -- Row-level trigger on mammoth is NOT a direct trigger (was dropped and recreated on parent)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_mammoth'::regclass
      AND tgname='pnd_trg_row' AND tgparentid=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'trg_row_not_direct_on_mammoth');

    -- Row-level trigger cloned to new partition (cloned triggers have tgisinternal=true)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_101_151'::regclass
      AND tgname='pnd_trg_row' AND tgparentid != 0 AND tgconstraint=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_row_cloned_on_new_partition');

    -- Statement-level trigger is a DIRECT trigger on parent
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg'::regclass
      AND tgname='pnd_trg_stmt' AND tgparentid=0 AND NOT tgisinternal AND tgconstraint=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_stmt_direct_on_parent');

    -- Statement-level trigger NOT a direct trigger on mammoth (was moved to parent)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_mammoth'::regclass
      AND tgname='pnd_trg_stmt' AND tgparentid=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'trg_stmt_not_direct_on_mammoth');

    -- Constraint trigger NOT on parent
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg'::regclass AND tgname='pnd_trg_constraint';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'trg_constraint_not_on_parent');

    -- Constraint trigger stays on mammoth as a DIRECT trigger (not moved, not cloned)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_mammoth'::regclass
      AND tgname='pnd_trg_constraint' AND tgparentid=0 AND tgconstraint != 0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_constraint_direct_on_mammoth');

    -- Constraint trigger NOT on new partition
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_101_151'::regclass AND tgname='pnd_trg_constraint';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'trg_constraint_not_on_new_partition');

    -- --- v_move_trg=FALSE ---
    CREATE TABLE dba_test.pnd_trg_nomov (id bigint, k bigint, PRIMARY KEY (id, k));
    EXECUTE $sql$
        CREATE TRIGGER pnd_trg_nomov_row
        BEFORE INSERT ON dba_test.pnd_trg_nomov
        FOR EACH ROW EXECUTE FUNCTION dba_test.pnd_trg_fn()
    $sql$;
    INSERT INTO dba_test.pnd_trg_nomov VALUES (1, 1);
    PERFORM dba.partition_native('dba_test','pnd_trg_nomov','k','0','100','50', false, false);

    -- Parent has NO trigger (not moved)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_nomov'::regclass AND tgname='pnd_trg_nomov_row';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'trg_move_false_not_on_parent');

    -- Trigger remains a DIRECT trigger on mammoth (was never dropped)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_nomov_mammoth'::regclass
      AND tgname='pnd_trg_nomov_row' AND tgparentid=0 AND NOT tgisinternal;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_move_false_direct_on_mammoth');

    -- New partition gets the trigger copied from mammoth (create_optimized_table_copy copies tgparentid=0 triggers)
    SELECT count(*) INTO v_count FROM pg_trigger
    WHERE tgrelid='dba_test.pnd_trg_nomov_101_151'::regclass AND NOT tgisinternal;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'trg_move_false_new_partition_has_copy');

    RETURN;
END; $$;


-- ===========================================================================
-- 8. COLUMN OPTIONS, RELOPTIONS, COLUMN STATISTICS
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_column_opts()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_count int;
    v_text  text;
    v_int   int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_opts CASCADE';

    CREATE TABLE dba_test.pnd_opts (id bigint, k bigint, val text, PRIMARY KEY (id, k));
    EXECUTE 'ALTER TABLE dba_test.pnd_opts ALTER COLUMN val SET (n_distinct = -0.5)';
    EXECUTE 'ALTER TABLE dba_test.pnd_opts SET (fillfactor = 80)';
    EXECUTE 'ALTER TABLE dba_test.pnd_opts ALTER COLUMN val SET STATISTICS 300';
    INSERT INTO dba_test.pnd_opts VALUES (1, 1, 'hello');
    PERFORM dba.partition_native('dba_test','pnd_opts','k','0','100','50');

    -- n_distinct attoption copied to parent
    SELECT count(*) INTO v_count FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_opts' AND a.attname='val'
      AND 'n_distinct=-0.5' = ANY(a.attoptions);
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'opts_ndistinct_on_parent');

    -- n_distinct attoption on new partition (via create_optimized_table_copy from mammoth)
    SELECT count(*) INTO v_count FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_opts_101_151' AND a.attname='val'
      AND 'n_distinct=-0.5' = ANY(a.attoptions);
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'opts_ndistinct_on_new_partition');

    -- fillfactor reloption on new partition (create_optimized_table_copy copies from mammoth)
    SELECT btrim(reloptions::text,'{}') INTO v_text FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_opts_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_true(v_text LIKE '%fillfactor=80%', 'opts_fillfactor_on_new_partition');

    -- Custom statistics target (300) restored on parent after ANALYZE
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_opts' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(300, v_int, 'opts_stats_target_restored_on_parent');

    -- New partition: attstattarget reset to -1 because p_copy_statistics_to_children defaults to FALSE
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_opts_101_151' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(-1, v_int, 'opts_stats_target_reset_on_new_partition');

    RETURN;
END; $$;


-- ===========================================================================
-- 8. OUTGOING FOREIGN KEY
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_fk_outgoing()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int; v_text text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fko_src CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fko_ref CASCADE';

    CREATE TABLE dba_test.pnd_fko_ref (ref_id bigint PRIMARY KEY, name text);
    INSERT INTO dba_test.pnd_fko_ref VALUES (1, 'one');

    CREATE TABLE dba_test.pnd_fko_src (id bigint, k bigint, ref_id bigint, PRIMARY KEY (id, k));
    ALTER TABLE dba_test.pnd_fko_src
        ADD CONSTRAINT pnd_fko_src_ref_fk FOREIGN KEY (ref_id) REFERENCES dba_test.pnd_fko_ref(ref_id);
    INSERT INTO dba_test.pnd_fko_src VALUES (1, 1, 1);
    PERFORM dba.partition_native('dba_test','pnd_fko_src','k','0','100','50');

    -- FK exists on parent with original name
    SELECT count(*) INTO v_count FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fko_src'
      AND c.contype='f' AND c.conname='pnd_fko_src_ref_fk';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'fk_out_on_parent');

    -- FK constraint name contains no 'mammoth'
    SELECT count(*) INTO v_count FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fko_src'
      AND c.contype='f' AND c.conname LIKE '%mammoth%';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'fk_out_name_no_mammoth');

    -- FK references the correct (non-mammoth) table
    SELECT cl2.relname INTO v_text FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    JOIN pg_class cl2 ON cl2.oid=c.confrelid
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fko_src'
      AND c.contype='f' AND c.conname='pnd_fko_src_ref_fk';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('pnd_fko_ref', v_text, 'fk_out_correct_target');

    RETURN;
END; $$;


-- ===========================================================================
-- 9. INCOMING FK FROM REGULAR TABLE
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_fk_incoming_regular()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int; v_text text; v_bool boolean;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fki_reg_ref CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fki_reg_src CASCADE';

    CREATE TABLE dba_test.pnd_fki_reg_ref (ref_id bigint PRIMARY KEY, name text);
    INSERT INTO dba_test.pnd_fki_reg_ref VALUES (1, 'one');

    CREATE TABLE dba_test.pnd_fki_reg_src (id bigint PRIMARY KEY, ref_id bigint);
    ALTER TABLE dba_test.pnd_fki_reg_src
        ADD CONSTRAINT pnd_fki_reg_src_fk FOREIGN KEY (ref_id) REFERENCES dba_test.pnd_fki_reg_ref(ref_id);
    INSERT INTO dba_test.pnd_fki_reg_src VALUES (1, 1);
    PERFORM dba.partition_native('dba_test','pnd_fki_reg_ref','ref_id','0','5','3');

    -- FK still exists on referencing table with same name
    SELECT count(*) INTO v_count FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fki_reg_src'
      AND c.contype='f' AND c.conname='pnd_fki_reg_src_fk';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'fk_in_reg_still_exists');

    -- FK now references the new parent (not mammoth)
    SELECT cl2.relname INTO v_text FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    JOIN pg_class cl2 ON cl2.oid=c.confrelid
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fki_reg_src'
      AND c.contype='f' AND c.conname='pnd_fki_reg_src_fk';
    RETURN QUERY SELECT * FROM dba_test.assert_equals('pnd_fki_reg_ref', v_text, 'fk_in_reg_references_parent');

    -- FK is validated
    SELECT c.convalidated INTO v_bool FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fki_reg_src'
      AND c.contype='f' AND c.conname='pnd_fki_reg_src_fk';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(TRUE, v_bool, 'fk_in_reg_validated');

    RETURN;
END; $$;


-- ===========================================================================
-- 10. INCOMING FK FROM PARTITIONED TABLE
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_fk_incoming_partitioned()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE v_count int; v_text text;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fki_prt_ref CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_fki_prt_src CASCADE';

    -- Table that will be partitioned (the referenced one)
    CREATE TABLE dba_test.pnd_fki_prt_ref (ref_id bigint PRIMARY KEY, name text);
    INSERT INTO dba_test.pnd_fki_prt_ref VALUES (1, 'one'), (2, 'two');

    -- Partitioned referencing table with two child partitions
    CREATE TABLE dba_test.pnd_fki_prt_src (id bigint, ref_id bigint, val bigint, PRIMARY KEY (id, val))
        PARTITION BY RANGE (val);
    CREATE TABLE dba_test.pnd_fki_prt_src_0_50
        PARTITION OF dba_test.pnd_fki_prt_src FOR VALUES FROM (0)  TO (50);
    CREATE TABLE dba_test.pnd_fki_prt_src_50_100
        PARTITION OF dba_test.pnd_fki_prt_src FOR VALUES FROM (50) TO (100);
    ALTER TABLE dba_test.pnd_fki_prt_src
        ADD CONSTRAINT pnd_fki_prt_src_fk FOREIGN KEY (ref_id) REFERENCES dba_test.pnd_fki_prt_ref(ref_id);

    PERFORM dba.partition_native('dba_test','pnd_fki_prt_ref','ref_id','0','5','3');

    -- FK on the partitioned referencing table now references the new parent
    SELECT cl2.relname INTO v_text FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    JOIN pg_class cl2 ON cl2.oid=c.confrelid
    WHERE n.nspname='dba_test' AND cl.relname='pnd_fki_prt_src'
      AND c.contype='f' AND c.conname='pnd_fki_prt_src_fk' AND c.conparentid=0;
    RETURN QUERY SELECT * FROM dba_test.assert_equals('pnd_fki_prt_ref', v_text, 'fk_in_prt_parent_references_new_parent');

    -- Each child partition of the referencing table has a FK pointing to the new parent
    SELECT count(*) INTO v_count FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    JOIN pg_class cl2 ON cl2.oid=c.confrelid
    WHERE n.nspname='dba_test'
      AND cl.relname IN ('pnd_fki_prt_src_0_50','pnd_fki_prt_src_50_100')
      AND c.contype='f' AND cl2.relname='pnd_fki_prt_ref';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'fk_in_prt_each_child_has_fk');

    -- Child FKs are validated
    SELECT count(*) INTO v_count FROM pg_constraint c
    JOIN pg_class cl ON cl.oid=c.conrelid JOIN pg_namespace n ON n.oid=cl.relnamespace
    JOIN pg_class cl2 ON cl2.oid=c.confrelid
    WHERE n.nspname='dba_test'
      AND cl.relname IN ('pnd_fki_prt_src_0_50','pnd_fki_prt_src_50_100')
      AND c.contype='f' AND cl2.relname='pnd_fki_prt_ref' AND c.convalidated=TRUE;
    RETURN QUERY SELECT * FROM dba_test.assert_equals(2, v_count, 'fk_in_prt_child_fks_validated');

    RETURN;
END; $$;


-- ===========================================================================
-- 11. STATISTICS OBJECTS (attstattarget + extended statistics)
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_statistics_objects()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_count int;
    v_int   int;
BEGIN
    -- -----------------------------------------------------------------------
    -- Part A: p_copy_statistics_to_children = FALSE (default)
    -- Mammoth keeps all statistics; new partition has none.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_stats CASCADE';

    CREATE TABLE dba_test.pnd_stats (id bigint, k bigint, val text, extra bigint, PRIMARY KEY (id, k));
    EXECUTE 'ALTER TABLE dba_test.pnd_stats ALTER COLUMN val SET STATISTICS 400';
    EXECUTE 'CREATE STATISTICS dba_test.pnd_stats_ndist (ndistinct) ON k, extra FROM dba_test.pnd_stats';
    INSERT INTO dba_test.pnd_stats VALUES (1, 1, 'hello', 42);
    PERFORM dba.partition_native('dba_test','pnd_stats','k','0','100','50');

    -- Parent: attstattarget restored by the ANALYZE loop in partition_native
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(400, v_int, 'stats_default_attstattarget_on_parent');

    -- Parent: extended statistics copied from mammoth
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'stats_default_extended_on_parent');

    -- Mammoth: attstattarget is preserved (it is the original table renamed)
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats_mammoth' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(400, v_int, 'stats_default_attstattarget_on_mammoth');

    -- Mammoth: extended statistics are preserved (original table renamed)
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'stats_default_extended_on_mammoth');

    -- New partition: attstattarget reset to default (-1) because flag=FALSE
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats_101_151' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(-1, v_int, 'stats_default_attstattarget_dropped_on_partition');

    -- New partition: extended statistics dropped because flag=FALSE
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(0, v_count, 'stats_default_extended_dropped_on_partition');

    -- -----------------------------------------------------------------------
    -- Part B: p_copy_statistics_to_children = TRUE
    -- Mammoth keeps all statistics; new partition also keeps them.
    -- -----------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_stats2 CASCADE';

    CREATE TABLE dba_test.pnd_stats2 (id bigint, k bigint, val text, extra bigint, PRIMARY KEY (id, k));
    EXECUTE 'ALTER TABLE dba_test.pnd_stats2 ALTER COLUMN val SET STATISTICS 400';
    EXECUTE 'CREATE STATISTICS dba_test.pnd_stats2_ndist (ndistinct) ON k, extra FROM dba_test.pnd_stats2';
    INSERT INTO dba_test.pnd_stats2 VALUES (1, 1, 'hello', 42);
    PERFORM dba.partition_native('dba_test','pnd_stats2','k','0','100','50',FALSE,TRUE,FALSE,TRUE);

    -- Parent: extended statistics copied from mammoth (always, regardless of flag)
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats2';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'stats_copy_extended_on_parent');

    -- Mammoth: attstattarget preserved
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats2_mammoth' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(400, v_int, 'stats_copy_attstattarget_on_mammoth');

    -- Mammoth: extended statistics preserved
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats2_mammoth';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'stats_copy_extended_on_mammoth');

    -- New partition: attstattarget kept because flag=TRUE
    SELECT a.attstattarget INTO v_int FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats2_101_151' AND a.attname='val';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(400, v_int, 'stats_copy_attstattarget_on_partition');

    -- New partition: extended statistics kept because flag=TRUE
    SELECT count(*) INTO v_count FROM pg_statistic_ext s
    JOIN pg_class c ON c.oid=s.stxrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='dba_test' AND c.relname='pnd_stats2_101_151';
    RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'stats_copy_extended_on_partition');

    RETURN;
END; $$;


-- ===========================================================================
-- MIXED-CASE IDENTIFIERS
-- ===========================================================================
CREATE OR REPLACE FUNCTION dba_test.test_partition_declarative_mixed_case()
RETURNS SETOF dba_test.test_result LANGUAGE plpgsql AS $$
DECLARE
    v_result BOOLEAN;
    v_count  INT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_case CASCADE';

    CREATE TABLE dba_test.pnd_case (id bigint, k bigint, PRIMARY KEY (id, k));
    INSERT INTO dba_test.pnd_case VALUES (1, 1);

    -- Uppercase arguments must partition the table and keep the generated
    -- object names lowercase.
    BEGIN
        SELECT dba.partition_native('DBA_TEST','PND_CASE','K','0','100','10') INTO v_result;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(TRUE, v_result, 'declarative_mixed_case_partitioned');

        SELECT count(*) INTO v_count FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        WHERE c.relname = 'pnd_case' AND c.relnamespace = 'dba_test'::regnamespace;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'declarative_mixed_case_structure');

        SELECT count(*) INTO v_count FROM pg_class
        WHERE relname = 'pnd_case_mammoth' AND relnamespace = 'dba_test'::regnamespace;
        RETURN QUERY SELECT * FROM dba_test.assert_equals(1, v_count, 'declarative_mixed_case_mammoth_lowercase');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dba_test.record_result('declarative_mixed_case_partitioned', 'FAIL', SQLERRM);
        PERFORM dba_test.record_result('declarative_mixed_case_structure', 'FAIL', SQLERRM);
        PERFORM dba_test.record_result('declarative_mixed_case_mammoth_lowercase', 'FAIL', SQLERRM);
    END;

    EXECUTE 'DROP TABLE IF EXISTS dba_test.pnd_case CASCADE';
    RETURN;
END; $$;
