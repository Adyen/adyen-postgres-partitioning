/*
This function returns the relation name and range for the last partition of a partitioned table.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname                           TEXT    the normal table name
    v_range_identifier                  TEXT    the identifier for a given range

Example:
    SELECT dba.partition_get_last_partition_details('public','partitioned_table');
    SELECT dba.partition_get_last_partition_details('public','partitioned_table', 'r1');
*/
create or replace function dba.partition_get_last_partition_details(v_schema text, v_relname text, v_range_identifier text default null)
returns table(v_childrelname text, v_range TEXT ARRAY)
language plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS 
$func$
DECLARE
  v_is_range    BOOLEAN;
  v_coltype     TEXT;
BEGIN

v_is_range := v_range_identifier IS NOT NULL;

-- Normalize the identifiers so names with uppercase letters are matched case-insensitively.
v_schema := LOWER(v_schema);
v_relname := LOWER(v_relname);
v_range_identifier := LOWER(v_range_identifier);

-- select the column type of the partitioning column.
SELECT pci.v_column_type
INTO v_coltype
FROM dba.partition_get_partition_column_info(v_schema, v_relname) AS pci;

return query execute format($sel$
SELECT
    LOWER(child.relname),
    regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*') as range
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
WHERE
    LOWER(nmsp_child.nspname)=%L
    AND LOWER(parent.relname)=%L
    AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    AND (NOT %L::boolean or LOWER(child.relname) like %L || '\_' || %L || '\_%%')
    AND NOT LOWER(child.relname) ~ 'mammoth'
-- Order by the partition lower boundary limit, casted to the partition column type.
ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[1]::%s DESC
LIMIT 1
$sel$ , v_schema, v_relname, v_is_range, v_relname, v_range_identifier, v_coltype);
END;
$func$;
