/*
Returns true if the given table is a partitioned table, false otherwise.

    PARAMETER       TYPE    DESCRIPTION
    v_schemaname    TEXT    schema name
    v_tablename     TEXT    table name

Example:
    SELECT dba.partition_table_is_partitioned('public', 'orders');
*/

CREATE OR REPLACE FUNCTION dba.partition_table_is_partitioned(v_schemaname text, v_tablename text)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        JOIN pg_class c     ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = lower(v_schemaname)
          AND lower(c.relname)  = lower(v_tablename)
    );
END;
$func$;
