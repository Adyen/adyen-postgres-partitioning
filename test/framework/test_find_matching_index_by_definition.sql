/*
Test: test_find_matching_index_by_definition
Function under test: dba.find_matching_index_by_definition
Run: ./test/framework/run_partition_tests.sh test_find_matching_index_by_definition
Purpose: find duplicate index by its definition
Test coverage: Asserts that at least one statement is produced for a partitioned table.
*/

CREATE OR REPLACE FUNCTION dba_test.test_find_matching_index_by_definition()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
    v_expected int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fidx CASCADE';

    -- Test setup:
    -- Create a partitioned table with 1 parent and 5 partitions:
    -- fidx [parent]
    --      fidx_default [partition default]
    --      fidx_q1 [partition 1]
    --      fidx_q2 [partition 2]
    --      fidx_q3 [partition 3]
    --      fidx_q4 [partition 4]

    -- This gives 6 total index targets when parent + partitions are considered.

    CREATE TABLE dba_test.fidx (c1 integer,name text,attrs_a jsonb,attrs_b jsonb,createdate date,status boolean,primary key(c1,createdate))PARTITION BY range (createdate);

    CREATE TABLE dba_test.fidx_default PARTITION OF dba_test.fidx DEFAULT;

    CREATE TABLE dba_test.fidx_q1
    PARTITION OF dba_test.fidx
    FOR VALUES FROM ('2024-01-01') TO ('2024-03-31');

    CREATE TABLE dba_test.fidx_q2
    PARTITION OF dba_test.fidx
    FOR VALUES FROM ('2024-04-01') TO ('2024-06-30');

    CREATE TABLE dba_test.fidx_q3
    PARTITION OF dba_test.fidx
    FOR VALUES FROM ('2024-07-01') TO ('2024-09-30');

    CREATE TABLE dba_test.fidx_q4
    PARTITION OF dba_test.fidx
    FOR VALUES FROM ('2024-10-01') TO ('2024-12-31');

    -- Pre-create matching indexes on 2 partitions.
    -- These should be detected by dba.find_matching_index_by_definition()
    -- as already existing logical equivalents.

    CREATE INDEX fidx_default_indx ON dba_test.fidx_default USING btree ( name, createdate, (attrs_a #>> '{key_a, key_b}'), status  ) WHERE (attrs_a #>> '{key_a, key_b}') IS NOT NULL;

    CREATE INDEX fidx_q2_indx ON dba_test.fidx_q2 USING btree ( name, createdate, (attrs_a #>> '{key_a, key_b}'), status  ) WHERE (attrs_a #>> '{key_a, key_b}') IS NOT NULL;

    -- partition_add_concurrent_index_on_partitioned_table() should:
    --   1. skip partitions that already have a matching index definition
    --   2. generate statements only for missing targets
    --
    -- Matching indexes already exist on:
    --   - fidx_default [partition default]
    --   - fidx_q2 [partition 2]
    --
    -- Missing index targets are:
    --   - fidx [parent]
    --      fidx_q1 [partition 1]
    --      fidx_q3 [partition 3]
    --      fidx_q4 [partition 4]
    --
    -- Expected number of generated statements = 4
    v_expected := 4;

    -- Therefore, v_count should be 4

    SELECT count(*) FROM dba.partition_add_concurrent_index_on_partitioned_table(
        v_schema    => 'dba_test',
        v_table     => 'fidx',
        v_columns   => ARRAY[
                        'name', 
                        'createdate', 
                        '(attrs_a #>> ''{key_a, key_b}'')',
                        'status'
                        ],
        v_condition => '(attrs_a #>> ''{key_a, key_b}'') IS NOT NULL' ) INTO v_count;

    RETURN QUERY SELECT * FROM dba_test.assert_equals(v_expected, v_count, 'find_matching_index_by_definition');

    RETURN;
END;
$$;
