/*
Returns the upper boundary (exclusive) of the partition currently receiving writes.

Discovers the partition key column from catalog metadata, computes max(key) on the table,
finds the child partition whose range contains that value, and returns its upper bound as bigint.

Only integer-typed partition keys (int2, int4, int8) are supported.

    PARAMETER       TYPE    DESCRIPTION
    v_schemaname    TEXT    schema of the partitioned table
    v_tablename     TEXT    table name

Example:
    SELECT dba.partition_get_active_upper_bound('public', 'orders');
*/
CREATE OR REPLACE FUNCTION dba.partition_get_active_upper_bound(v_schemaname text, v_tablename text)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_column_name TEXT;
    v_coltype     TEXT;
    v_max_val     bigint;
    v_result      bigint;
BEGIN
    PERFORM 1
    FROM pg_partitioned_table pt
    JOIN pg_class c     ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = lower(v_schemaname)
      AND lower(c.relname) = lower(v_tablename);

    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% is not a partitioned table', v_schemaname, v_tablename;
    END IF;

    SELECT lower(col.column_name), t.typname
    INTO v_column_name, v_coltype
    FROM (SELECT partrelid, unnest(partattrs) column_index FROM pg_partitioned_table) pt
    JOIN pg_class c ON c.oid = pt.partrelid
    JOIN information_schema.columns col
        ON col.table_schema = c.relnamespace::regnamespace::text
       AND col.table_name   = c.relname
       AND ordinal_position = pt.column_index
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
    JOIN pg_catalog.pg_type t      ON t.oid = a.atttypid
    WHERE lower(c.relname)                          = lower(v_tablename)
      AND lower(c.relnamespace::regnamespace::text) = lower(v_schemaname);

    IF NOT v_coltype ~ 'int' THEN
        RAISE EXCEPTION '%.% is partitioned on % (type %), but only integer types are supported',
                        v_schemaname, v_tablename, v_column_name, v_coltype;
    END IF;

    EXECUTE format('SELECT max(%I) FROM %I.%I', v_column_name, v_schemaname, v_tablename)
    INTO v_max_val;

    IF v_max_val IS NULL THEN
        RAISE EXCEPTION '%.% is empty — cannot determine active partition', v_schemaname, v_tablename;
    END IF;

    SELECT min((regexp_match(
                    pg_catalog.pg_get_expr(child.relpartbound, child.oid),
                    'TO \(''?(\d+)''?\)'
                ))[1]::bigint)
    INTO v_result
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    JOIN pg_namespace n  ON n.oid = parent.relnamespace
    WHERE lower(n.nspname)      = lower(v_schemaname)
      AND lower(parent.relname) = lower(v_tablename)
      AND (regexp_match(
              pg_catalog.pg_get_expr(child.relpartbound, child.oid),
              'TO \(''?(\d+)''?\)'
          ))[1]::bigint > v_max_val;

    IF v_result IS NULL THEN
        RAISE EXCEPTION 'could not find a partition containing max(%) = % for %.%',
                        v_column_name, v_max_val, v_schemaname, v_tablename;
    END IF;

    RETURN v_result;
END;
$func$;
