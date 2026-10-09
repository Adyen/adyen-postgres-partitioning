/*
Returns the partition width (range size) of a partitioned table by inspecting the last
non-mammoth partition's bounds. Only integer-typed partition keys are supported.

    PARAMETER       TYPE    DESCRIPTION
    v_schemaname    TEXT    schema of the partitioned table
    v_tablename     TEXT    table name

Example:
    SELECT dba.partition_get_grid_width('public', 'orders');
*/
CREATE OR REPLACE FUNCTION dba.partition_get_grid_width(v_schemaname text, v_tablename text)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_last_range text[];
BEGIN
    SELECT v_range
    INTO v_last_range
    FROM dba.partition_get_last_partition_details(v_schemaname, v_tablename);

    IF v_last_range IS NULL THEN
        RAISE EXCEPTION '%.% has no non-mammoth partitions', v_schemaname, v_tablename;
    END IF;

    RETURN v_last_range[2]::bigint - v_last_range[1]::bigint;
END;
$func$;
