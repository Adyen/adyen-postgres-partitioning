/*
All triggers on a parent partitioned table should also be present on all child partitions.
This function checks two kinds of inconsistencies and reports each at LOG level:

  1. Parent-to-child: a trigger defined on the parent is missing on one or more child partitions.
  2. Child-to-child: triggers are present on some children but not on others (regardless of the parent).

Internal constraint triggers (tgisinternal = true) are excluded.

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    schema of the partitioned table
    v_table     TEXT    name of the partitioned table

Example:
    SELECT dba.partition_check_triggers_on_all_partitions('public', 'some_table');

Returns:
    TRUE  when all child partitions have the same set of triggers as the parent and are consistent.
    FALSE when any inconsistency is found (findings reported via RAISE LOG).
*/
CREATE OR REPLACE FUNCTION dba.partition_check_triggers_on_all_partitions(v_schema TEXT, v_table TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_parent_oid        oid;
    v_parent_trigger    RECORD;
    v_child             RECORD;
    v_ref_child_oid     oid;
    v_ref_child_name    TEXT;
    v_trigger_exists    BOOLEAN;
    v_return_flag       BOOLEAN DEFAULT TRUE;
    v_all_trigger_names TEXT[];
    v_trigger_name      TEXT;
BEGIN

SELECT t.oid
INTO v_parent_oid
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE LOWER(n.nspname) = LOWER(v_schema)
  AND LOWER(t.relname) = LOWER(v_table);

IF v_parent_oid IS NULL OR NOT EXISTS (
    SELECT 1 FROM pg_partitioned_table WHERE partrelid = v_parent_oid
) THEN
    RAISE EXCEPTION 'Table %.% is not a partitioned table', v_schema, v_table;
END IF;

IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = v_parent_oid) THEN
    RETURN TRUE;
END IF;

-- 1. Parent-to-child: every non-internal trigger on the parent must exist on every child.
FOR v_parent_trigger IN
    SELECT tgname
    FROM pg_trigger
    WHERE tgrelid = v_parent_oid AND tgisinternal = false
LOOP
    FOR v_child IN
        SELECT inhrelid AS oid, c.relname
        FROM pg_inherits
        JOIN pg_class c ON c.oid = inhrelid
        WHERE inhparent = v_parent_oid
    LOOP
        SELECT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = v_child.oid AND tgname = v_parent_trigger.tgname
        ) INTO v_trigger_exists;

        IF NOT v_trigger_exists THEN
            RAISE LOG 'Partition maintenance: Trigger ''%'' on parent %.% is missing on child %',
                v_parent_trigger.tgname, v_schema, v_table, v_child.relname;
            v_return_flag := FALSE;
        END IF;
    END LOOP;
END LOOP;

-- 2. Child-to-child: directly-created child triggers (tgparentid=0) that are only on some children.
SELECT array_agg(DISTINCT tgname ORDER BY tgname)
INTO v_all_trigger_names
FROM pg_trigger t
JOIN pg_inherits i ON i.inhrelid = t.tgrelid
WHERE i.inhparent = v_parent_oid
  AND t.tgisinternal = false
  AND t.tgparentid   = 0;

IF v_all_trigger_names IS NULL THEN
    RETURN v_return_flag;
END IF;

FOREACH v_trigger_name IN ARRAY v_all_trigger_names
LOOP
    SELECT inhrelid
    INTO v_ref_child_oid
    FROM pg_inherits
    JOIN pg_trigger t ON t.tgrelid = inhrelid
    WHERE inhparent = v_parent_oid
      AND t.tgname = v_trigger_name
      AND t.tgisinternal = false
      AND t.tgparentid   = 0
    LIMIT 1;

    SELECT c.relname INTO v_ref_child_name FROM pg_class c WHERE c.oid = v_ref_child_oid;

    FOR v_child IN
        SELECT inhrelid AS oid, c.relname
        FROM pg_inherits
        JOIN pg_class c ON c.oid = inhrelid
        WHERE inhparent = v_parent_oid
    LOOP
        IF v_child.oid = v_ref_child_oid THEN
            CONTINUE;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = v_child.oid
              AND tgname = v_trigger_name
              AND tgisinternal = false
        ) THEN
            RAISE LOG 'Partition maintenance: Trigger ''%'' is present on child % but missing on child % (parent: %.%)',
                v_trigger_name, v_ref_child_name, v_child.relname, v_schema, v_table;
            v_return_flag := FALSE;
        END IF;
    END LOOP;
END LOOP;

RETURN v_return_flag;

END
$func$;
