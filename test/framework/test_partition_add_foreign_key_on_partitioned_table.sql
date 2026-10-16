/*
Test: test_partition_add_foreign_key_on_partitioned_table
Function under test: dba.partition_add_foreign_key_on_partitioned_table
Run: ./test/framework/run_partition_tests.sh test_partition_add_foreign_key_on_partitioned_table
Purpose: Generate foreign key statements for partitioned tables and children.
Test coverage: Creates parent/child fixtures and ensures statements are returned.
*/

CREATE OR REPLACE FUNCTION dba_test.test_partition_add_foreign_key_on_partitioned_table()
RETURNS SETOF dba_test.test_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_count int;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_parent CASCADE';
    EXECUTE 'DROP TABLE IF EXISTS dba_test.fk_child CASCADE';

    CREATE TABLE dba_test.fk_parent (id bigint not null primary key);
    CREATE TABLE dba_test.fk_child (id bigint not null, parent_id bigint) PARTITION BY RANGE (id);
    CREATE TABLE dba_test.fk_child_0_10 PARTITION OF dba_test.fk_child FOR VALUES FROM (0) TO (10);

    SELECT count(*) FROM dba.partition_add_foreign_key_on_partitioned_table(
        'dba_test',
        'fk_child',
        'fk_child_parent_fk',
        'fk_parent',
        ARRAY['parent_id'],
        ARRAY['id']
    ) INTO v_count;

    RETURN QUERY SELECT * FROM dba_test.assert_true(v_count >= 3, 'add_foreign_key_generates_statements');

    RETURN;
END;
$$;
