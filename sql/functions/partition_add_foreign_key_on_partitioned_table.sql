/*
This function generates a set of statements to create add foreign key not valid statements for all partitions of a
partitioned tables and a create foreign key statement for the parent table. If a <child_table>_template exists a
create foreign key statement is returned for this table as wel. It does not execute these
statements.

    PARAMETER               TYPE    DESCRIPTION
    v_schema                TEXT    schema location for the table
    v_table                 TEXT    the partitioned child table on which to create the foreign key
    v_constraint_name       TEXT    the name of the constraint. Equal for all children and parent
    v_parent                TEXT    the name of the parent table
    v_child_column_names    TEXT[]  the column names of the child table
    v_parent_column_names   TEXT[]  the column names of the parent table

Example:
    SELECT dba.partition_add_foreign_key_on_partitioned_table('public','test_partition', 'table_fk', 'table_parent', ARRAY['id'], ARRAY['id']);

Returns:
    A table containing the following statements in order
     - A add constraint not valid statement every child table
     - A validate constraint statement every child table
     - A add constraint statement for the parent table
*/

CREATE OR REPLACE FUNCTION dba.partition_add_foreign_key_on_partitioned_table(v_schema TEXT, v_table TEXT, v_constraint_name TEXT, v_parent TEXT, v_child_column_names TEXT[], v_parent_column_names TEXT[])
RETURNS table(stmt text) LANGUAGE plpgsql AS $func$

DECLARE
    v_total_childs         INT;

BEGIN
    v_schema:=LOWER(v_schema);
    v_table:=LOWER(v_table);

    -- Create a temporary table to store the results.
    CREATE TEMP TABLE IF NOT EXISTS temp_partition_concurrent_indexes (order_number int, table_name text, index_name text)
    ON COMMIT DELETE ROWS;

    -- List all the child partitions
    -- The name of the temp table is not exactly spot on, but I don't want to create a new temp table for this function.
    EXECUTE FORMAT ($sql$
        INSERT INTO temp_partition_concurrent_indexes (
            SELECT
                1 AS order_number,
                child.relname as table_name,
                null AS index_name
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
            WHERE
                LOWER(parent.relnamespace::regnamespace::text) = LOWER('%I')
                AND LOWER(parent.relname)=LOWER('%I'))
    $sql$, v_schema, v_table);

    EXECUTE FORMAT ($sql$
        INSERT INTO temp_partition_concurrent_indexes values (2, '%I', null)
    $sql$, v_table);

    -- Check if _template table exists
    PERFORM 1
    FROM pg_class c
    WHERE
        LOWER(c.relname) = LOWER(v_table || '_template')
        AND LOWER(c.relnamespace::regnamespace::text) = LOWER(v_schema);

    IF FOUND THEN
         EXECUTE FORMAT ($sql$
              INSERT INTO temp_partition_concurrent_indexes values (3, '%I_template', null)
         $sql$, v_table, v_constraint_name);
    END IF;

    SELECT COUNT(*) FROM temp_partition_concurrent_indexes where order_number = 1
    INTO v_total_childs;

    RETURN QUERY
    EXECUTE FORMAT($sql$
        (SELECT 'alter table %I.' || table_name || ' add constraint %I foreign key (%I) references %I(%I) not valid;' FROM temp_partition_concurrent_indexes WHERE order_number = 1)
        UNION ALL
        (SELECT '/* Validating constraint ' || ROW_NUMBER() OVER (ORDER BY table_name) || ' of %s */ alter table %I.' || table_name || ' validate constraint %I;' FROM temp_partition_concurrent_indexes WHERE order_number = 1 ORDER BY table_name)
        UNION ALL
        (SELECT 'alter table %I.%I add constraint %I foreign key (%I) references %I(%I);')
        UNION ALL
        (SELECT 'alter table %I.' || table_name || ' add constraint %I foreign key (%I) references %I(%I);' FROM temp_partition_concurrent_indexes WHERE order_number = 3)
    $sql$,
    v_schema, v_constraint_name, LOWER(array_to_string(v_child_column_names, ',')), v_parent, LOWER(array_to_string(v_parent_column_names, ',')),
    v_total_childs, v_schema, v_constraint_name,
    v_schema, v_table, v_constraint_name, LOWER(array_to_string(v_child_column_names, ',')), v_parent, LOWER(array_to_string(v_parent_column_names, ',')),
    v_schema, v_constraint_name, LOWER(array_to_string(v_child_column_names, ',')), v_parent, LOWER(array_to_string(v_parent_column_names, ','))
   );
END
$func$; 
