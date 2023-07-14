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

CREATE OR REPLACE FUNCTION dba.partition_get_last_partition_details(v_schema TEXT, v_relname TEXT, v_range_identifier TEXT DEFAULT NULL)
RETURNS TABLE(v_childrelname TEXT, v_range TEXT ARRAY) LANGUAGE PLPGSQL AS $func$

DECLARE
    v_is_range    BOOLEAN;
    v_coltype     TEXT;

BEGIN
    v_is_range := v_range_identifier IS NOT NULL;
    
    -- select the column type of the partitioning column.
    EXECUTE format($sel$
        SELECT
            t.typname
        FROM
            (SELECT
                partrelid,
                unnest(partattrs) column_index
             FROM
                 pg_partitioned_table) pt
        JOIN pg_class c on c.oid = pt.partrelid
        JOIN information_schema.columns col ON
            col.table_schema = c.relnamespace::regnamespace::text
            AND col.table_name = c.relname
            AND ordinal_position = pt.column_index
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        WHERE
            LOWER(c.relname) = LOWER('%I')
            and LOWER(relnamespace::regnamespace::text)=LOWER('%I')
    $sel$, v_relname, v_schema)
    INTO v_coltype;
    
    RETURN QUERY EXECUTE FORMAT($sel$
        SELECT
            LOWER(child.relname),
            regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*') as range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
        WHERE
            LOWER(nmsp_child.nspname)=LOWER('%I')
            AND LOWER(parent.relname)=LOWER('%I')
            AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
            AND (NOT '%I' or child.relname like '%I_' || %L || '_%%')
            AND NOT LOWER(child.relname) ~ 'mammoth'
        -- Order by the partition lower boundary limit, casted to the partition column type.
        ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[1]::%s DESC
        LIMIT 1
    $sel$ , v_schema, v_relname, v_is_range, v_relname, v_range_identifier, v_coltype);
END;
$func$;
