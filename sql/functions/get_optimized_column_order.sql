/*
Returns the column order for a table that minimizes alignment padding by ordering
on alignment requirements.

    PARAMETER    TYPE    DESCRIPTION
    v_schema     TEXT    schema name
    v_table      TEXT    table name

Example:
    SELECT dba.get_optimized_column_order('public', 'orders');
*/

CREATE OR REPLACE FUNCTION dba.get_optimized_column_order(v_schema TEXT, v_table TEXT)
RETURNS TEXT[] LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_column_order TEXT[];
BEGIN
    SELECT array_agg(attname ORDER BY typlen desc, alignment_rank, attnum)
    INTO v_column_order
    FROM (
        SELECT
            a.attname,
            a.attnum,
            t.typlen,
            CASE t.typalign
                WHEN 'd' THEN 1
                WHEN 'i' THEN 2
                WHEN 's' THEN 3
                WHEN 'c' THEN 4
                ELSE 5
            END AS alignment_rank
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = format('%I.%I', lower(v_schema), lower(v_table))::regclass
            AND a.attnum > 0
            AND NOT a.attisdropped
    ) AS columns;

    RETURN v_column_order;
END
$func$;
