/*
Returns the partition name and range boundaries for the partition currently receiving writes.
The active partition is the one whose range contains the maximum value of the partition key.

Raises an exception when the table is empty or when the partition key type is not supported.

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    schema location for the table
    v_relname   TEXT    the table name

Example:
    SELECT * FROM dba.partition_get_current_partition_boundaries('public', 'orders');

Returns:
    v_partition_name TEXT    name of the active partition
    v_lower_bound    TEXT    lower boundary (inclusive)
    v_upper_bound    TEXT    upper boundary (exclusive)
*/
CREATE OR REPLACE FUNCTION dba.partition_get_current_partition_boundaries(v_schema TEXT, v_relname TEXT)
RETURNS TABLE(v_partition_name TEXT, v_lower_bound TEXT, v_upper_bound TEXT)
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_is_partitioned BOOLEAN;
    v_column_name    TEXT;
    v_coltype        TEXT;
    v_max_val        TEXT;
    v_boundary_regex CONSTANT TEXT := '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*';
BEGIN

v_schema  := LOWER(v_schema);
v_relname := LOWER(v_relname);

EXECUTE format($sel$
    SELECT count(*) > 0
    FROM pg_partitioned_table pt
    JOIN pg_class par ON par.oid = pt.partrelid
    WHERE LOWER(relnamespace::regnamespace::text) = LOWER(quote_ident(%L))
      AND LOWER(par.relname) = LOWER(%L)
$sel$, v_schema, v_relname)
INTO v_is_partitioned;

IF NOT v_is_partitioned THEN
    RAISE EXCEPTION 'Table % is not a partitioned table.', v_schema || '.' || v_relname USING ERRCODE = '45002';
END IF;

EXECUTE format($sel$
    SELECT LOWER(col.column_name), t.typname
    FROM (SELECT partrelid, unnest(partattrs) column_index FROM pg_partitioned_table) pt
    JOIN pg_class c ON c.oid = pt.partrelid
    JOIN information_schema.columns col
        ON col.table_schema = c.relnamespace::regnamespace::text
       AND col.table_name   = c.relname
       AND ordinal_position = pt.column_index
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    WHERE LOWER(c.relname)                          = LOWER(%L)
      AND LOWER(relnamespace::regnamespace::text)   = LOWER(quote_ident(%L))
$sel$, v_relname, v_schema)
INTO v_column_name, v_coltype;

EXECUTE format('SELECT max(%I)::text FROM %I.%I', v_column_name, v_schema, v_relname)
INTO v_max_val;

IF v_max_val IS NULL THEN
    RAISE EXCEPTION 'Table % is empty, cannot determine the current partition.', v_schema || '.' || v_relname USING ERRCODE = '45003';
END IF;

RAISE LOG 'Table % is partitioned on column % of type %, max value: %', v_relname, v_column_name, v_coltype, v_max_val USING ERRCODE = '45001';

CASE
    WHEN v_coltype ~ 'int' THEN
        RETURN QUERY EXECUTE format($sel$
            SELECT
                LOWER(child.relname)::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::text
            FROM pg_inherits
            JOIN pg_class parent         ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child          ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid        = child.relnamespace
            WHERE LOWER(nmsp_child.nspname) = LOWER(%L)
              AND LOWER(parent.relname)     = LOWER(%L)
              AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::bigint <= %L::bigint
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::bigint >  %L::bigint
        $sel$, v_boundary_regex, v_boundary_regex,
               v_schema, v_relname,
               v_boundary_regex, v_max_val,
               v_boundary_regex, v_max_val);

    WHEN v_coltype ~ 'date' THEN
        RETURN QUERY EXECUTE format($sel$
            SELECT
                LOWER(child.relname)::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::text
            FROM pg_inherits
            JOIN pg_class parent         ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child          ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid        = child.relnamespace
            WHERE LOWER(nmsp_child.nspname) = LOWER(%L)
              AND LOWER(parent.relname)     = LOWER(%L)
              AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::date <= %L::date
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::date >  %L::date
        $sel$, v_boundary_regex, v_boundary_regex,
               v_schema, v_relname,
               v_boundary_regex, v_max_val,
               v_boundary_regex, v_max_val);

    WHEN v_coltype ~ 'timestamp' THEN
        RETURN QUERY EXECUTE format($sel$
            SELECT
                LOWER(child.relname)::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::text
            FROM pg_inherits
            JOIN pg_class parent         ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child          ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid        = child.relnamespace
            WHERE LOWER(nmsp_child.nspname) = LOWER(%L)
              AND LOWER(parent.relname)     = LOWER(%L)
              AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::timestamp <= %L::timestamp
              AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::timestamp >  %L::timestamp
        $sel$, v_boundary_regex, v_boundary_regex,
               v_schema, v_relname,
               v_boundary_regex, v_max_val,
               v_boundary_regex, v_max_val);

    WHEN v_coltype ~ 'uuid' THEN
        RETURN QUERY EXECUTE format($sel$
            SELECT
                LOWER(child.relname)::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::text,
                (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::text
            FROM pg_inherits
            JOIN pg_class parent         ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child          ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_child ON nmsp_child.oid        = child.relnamespace
            WHERE LOWER(nmsp_child.nspname) = LOWER(%L)
              AND LOWER(parent.relname)     = LOWER(%L)
              AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
              AND dba.uuid_v7_to_timestamptz((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::uuid)
                  <= dba.uuid_v7_to_timestamptz(%L::uuid)
              AND dba.uuid_v7_to_timestamptz((regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::uuid)
                  >  dba.uuid_v7_to_timestamptz(%L::uuid)
        $sel$, v_boundary_regex, v_boundary_regex,
               v_schema, v_relname,
               v_boundary_regex, v_max_val,
               v_boundary_regex, v_max_val);

    ELSE
        RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype;
END CASE;

END
$func$;
