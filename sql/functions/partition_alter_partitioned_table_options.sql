/*
This function generates a set of statements to alter all partitions of a partitioned table.
If the table is not partitioned but exists, it returns the ALTER statement for the table itself.
If the table does not exist it returns no rows.

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    schema location for the table
    v_table     TEXT    the parent table name
    v_stmt      TEXT    the ALTER TABLE clause to append (e.g. 'SET (autovacuum_enabled=false)')

Example:
    SELECT dba.partition_alter_partitioned_table_options(
        v_schema => 'public',
        v_table  => 'orders',
        v_stmt   => 'SET (autovacuum_enabled=false)');

Returns:
    A set of ALTER TABLE statements — one per partition (or just the table itself when not partitioned).
*/
CREATE OR REPLACE FUNCTION dba.partition_alter_partitioned_table_options(v_schema TEXT, v_table TEXT, v_stmt TEXT)
RETURNS TABLE(stmt text) LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_row_count INT;
BEGIN
    v_schema := LOWER(v_schema);
    v_table  := LOWER(v_table);

    CREATE TEMP TABLE IF NOT EXISTS temp_partition_alter_partitioned_table_options
        (table_schema text, table_name text)
    ON COMMIT DELETE ROWS;

    INSERT INTO temp_partition_alter_partitioned_table_options
        SELECT
            parent.relnamespace::regnamespace::text AS tableschema,
            child.relname AS tablename
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        WHERE LOWER(parent.relnamespace::regnamespace::text) = v_schema
          AND LOWER(parent.relname) = v_table;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    IF v_row_count = 0 THEN
        INSERT INTO temp_partition_alter_partitioned_table_options
            SELECT v_schema, v_table
            FROM pg_class
            WHERE LOWER(relnamespace::regnamespace::text) = v_schema
              AND LOWER(relname) = v_table;
    END IF;

    RETURN QUERY
        SELECT 'ALTER TABLE ' || quote_ident(v_schema) || '.' || quote_ident(table_name) || ' ' || v_stmt || ';' AS final_stmt
        FROM temp_partition_alter_partitioned_table_options;
END
$func$;
