/*
Check whether the table is partitioned on a column which is part of the primary key.

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    schema location for the table
    v_table     TEXT    the parent table name

Example:
    SELECT dba.partition_partitioned_on_primary_key('public', 'orders');

Returns:
    TRUE when the table is partitioned based on a column which is part of the primary key,
    FALSE otherwise.
*/
CREATE OR REPLACE FUNCTION dba.partition_partitioned_on_primary_key(v_schema TEXT, v_table TEXT)
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_partitioned_table pt
        JOIN pg_class t ON pt.partrelid = t.oid
        WHERE LOWER(relnamespace::regnamespace::text) = LOWER(v_schema)
          AND LOWER(t.relname) = LOWER(v_table)
    ) THEN
        RAISE EXCEPTION 'Table % is not a partitioned table', v_schema || '.' || v_table;
        RETURN FALSE;
    END IF;

    RETURN (
        SELECT COUNT(*) > 0
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_partitioned_table pt ON pt.partrelid = t.oid AND pt.partattrs::text = a.attnum::text
        WHERE LOWER(relnamespace::regnamespace::text) = LOWER(v_schema)
          AND LOWER(t.relname) = LOWER(v_table)
          AND ix.indisprimary
    );
END
$func$;
