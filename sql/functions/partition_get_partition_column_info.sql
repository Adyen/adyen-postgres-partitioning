/*
Returns the partitioning column name and data type for a given partitioned table.
Raises an error if the table is not partitioned.

    PARAMETER    TYPE    DESCRIPTION
    v_schema     TEXT    schema name
    v_relname    TEXT    table name

Returns: table with v_column_name TEXT, v_column_type TEXT

Example:
    SELECT * FROM dba.partition_get_partition_column_info('public', 'orders');
*/

CREATE OR REPLACE FUNCTION dba.partition_get_partition_column_info(v_schema text, v_relname text)
RETURNS TABLE(v_column_name TEXT, v_column_type TEXT)
LANGUAGE PLPGSQL
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN

    IF NOT dba.partition_table_is_partitioned(v_schema, v_relname) THEN
        RAISE EXCEPTION 'Table %.% is not partitioned', v_schema, v_relname;
    END IF;

    RETURN QUERY
    SELECT
        LOWER(col.column_name),
        t.typname::text
    FROM
        (SELECT
            partrelid,
            unnest(partattrs) column_index
         FROM
             pg_catalog.pg_partitioned_table) pt
    JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid
    JOIN information_schema.columns col ON
        col.table_schema = c.relnamespace::regnamespace::text
        AND col.table_name = c.relname
        AND ordinal_position = pt.column_index
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    WHERE
        LOWER(c.relname) = LOWER(v_relname)
        AND LOWER(c.relnamespace::regnamespace::text) = LOWER(v_schema);
END;
$func$;
