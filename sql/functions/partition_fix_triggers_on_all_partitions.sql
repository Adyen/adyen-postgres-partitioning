/*
Repairs trigger inconsistencies on a partitioned table. Complementary to
dba.partition_check_triggers_on_all_partitions.

Two kinds of inconsistencies are repaired:

  1. Parent-to-child: a row-level trigger on the parent is missing from one or more child
     partitions. Fixed by dropping and re-creating the trigger on the parent, which forces
     PostgreSQL to re-clone it to all children.

  2. Child-to-child: a trigger is present on some children but not others (directly created,
     tgparentid = 0). Fixed based on trigger level:
       Row-level:       promote to parent; PostgreSQL auto-clones to all existing partitions.
       Statement-level: add directly to each missing child partition.

Internal triggers (tgisinternal = true), cloned triggers (tgparentid != 0), and constraint
triggers (tgconstraint != 0) are excluded.

    PARAMETER   TYPE        DESCRIPTION
    v_schema    TEXT        schema of the partitioned table
    v_table     TEXT        name of the partitioned table
    v_dry_run   BOOLEAN     when TRUE, log what would be done without applying changes (default: FALSE)

Example:
    SELECT dba.partition_fix_triggers_on_all_partitions('public', 'orders');
    SELECT dba.partition_fix_triggers_on_all_partitions('public', 'orders', v_dry_run => TRUE);

Returns:
    TRUE  when all triggers are consistent (or repaired successfully).
    FALSE when v_dry_run is TRUE and inconsistencies were found.
*/
CREATE OR REPLACE FUNCTION dba.partition_fix_triggers_on_all_partitions(v_schema TEXT, v_table TEXT, v_dry_run BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_parent_oid        oid;
    v_parent_trigger    RECORD;
    v_child             RECORD;
    v_ref_child_oid     oid;
    v_ref_child_name    TEXT;
    v_ref_tgoid         oid;
    v_return_flag       BOOLEAN DEFAULT TRUE;
    v_all_trigger_names TEXT[];
    v_trigger_name      TEXT;
    v_trigdef           TEXT;
    v_new_trigdef       TEXT;
    v_is_row_level      BOOLEAN;
BEGIN

IF NOT dba.partition_table_is_partitioned(v_schema, v_table) THEN
    RAISE EXCEPTION 'Table %.% is not a partitioned table', v_schema, v_table;
END IF;

SELECT t.oid
INTO v_parent_oid
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE LOWER(n.nspname) = LOWER(v_schema)
  AND LOWER(t.relname) = LOWER(v_table);

IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = v_parent_oid) THEN
    RETURN TRUE;
END IF;

-- 1. Parent-to-child repair: re-create row-level triggers missing on any child.
FOR v_parent_trigger IN
    SELECT t.tgname, t.oid AS tgoid
    FROM pg_trigger t
    WHERE t.tgrelid      = v_parent_oid
      AND t.tgisinternal  = false
      AND t.tgconstraint  = 0
      AND t.tgparentid    = 0
      AND (t.tgtype & 1)  = 1
LOOP
    IF EXISTS (
        SELECT 1 FROM pg_inherits
        WHERE inhparent = v_parent_oid
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t2
              WHERE t2.tgrelid = pg_inherits.inhrelid
                AND t2.tgname  = v_parent_trigger.tgname
          )
    ) THEN
        IF v_dry_run THEN
            RAISE LOG 'Partition fix (dry-run): Would re-create row-level trigger ''%'' on parent %.% to restore missing clones',
                v_parent_trigger.tgname, v_schema, v_table;
            v_return_flag := FALSE;
        ELSE
            SELECT pg_get_triggerdef(v_parent_trigger.tgoid) INTO v_trigdef;
            RAISE LOG 'Partition fix: Re-creating row-level trigger ''%'' on parent %.% to restore missing clones',
                v_parent_trigger.tgname, v_schema, v_table;
            EXECUTE format('DROP TRIGGER %I ON %I.%I', v_parent_trigger.tgname, v_schema, v_table);
            EXECUTE v_trigdef;
        END IF;
    END IF;
END LOOP;

-- 2. Child-to-child repair.
SELECT array_agg(DISTINCT tgname ORDER BY tgname)
INTO v_all_trigger_names
FROM pg_trigger t
JOIN pg_inherits i ON i.inhrelid = t.tgrelid
WHERE i.inhparent   = v_parent_oid
  AND t.tgisinternal = false
  AND t.tgconstraint = 0
  AND t.tgparentid   = 0;

IF v_all_trigger_names IS NULL THEN
    RETURN v_return_flag;
END IF;

FOREACH v_trigger_name IN ARRAY v_all_trigger_names
LOOP
    IF NOT EXISTS (
        SELECT 1 FROM pg_inherits
        WHERE inhparent = v_parent_oid
          AND NOT EXISTS (
              SELECT 1 FROM pg_trigger t2
              WHERE t2.tgrelid     = pg_inherits.inhrelid
                AND t2.tgname       = v_trigger_name
                AND t2.tgisinternal = false
          )
    ) THEN
        CONTINUE;
    END IF;

    SELECT i.inhrelid, c.relname, t.oid, (t.tgtype & 1) = 1
    INTO v_ref_child_oid, v_ref_child_name, v_ref_tgoid, v_is_row_level
    FROM pg_inherits i
    JOIN pg_trigger t  ON t.tgrelid = i.inhrelid
    JOIN pg_class c    ON c.oid     = i.inhrelid
    WHERE i.inhparent   = v_parent_oid
      AND t.tgname       = v_trigger_name
      AND t.tgisinternal = false
      AND t.tgconstraint = 0
      AND t.tgparentid   = 0
    LIMIT 1;

    SELECT pg_get_triggerdef(v_ref_tgoid) INTO v_trigdef;

    IF v_is_row_level THEN
        IF EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid     = v_parent_oid
              AND tgname       = v_trigger_name
              AND tgisinternal = false
              AND tgparentid   = 0
        ) THEN
            CONTINUE;
        END IF;

        v_new_trigdef := regexp_replace(v_trigdef, ' ON \S+', format(' ON %I.%I', v_schema, v_table));

        IF v_dry_run THEN
            RAISE LOG 'Partition fix (dry-run): Would promote row-level trigger ''%'' to parent %.% (present on child %, missing on some children)',
                v_trigger_name, v_schema, v_table, v_ref_child_name;
            v_return_flag := FALSE;
        ELSE
            RAISE LOG 'Partition fix: Promoting row-level trigger ''%'' to parent %.% (was on child %, missing on some children)',
                v_trigger_name, v_schema, v_table, v_ref_child_name;
            FOR v_child IN
                SELECT i.inhrelid AS oid, c.relname, n.nspname
                FROM pg_inherits i
                JOIN pg_trigger t  ON t.tgrelid = i.inhrelid
                JOIN pg_class c    ON c.oid = i.inhrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.inhparent   = v_parent_oid
                  AND t.tgname       = v_trigger_name
                  AND t.tgisinternal = false
                  AND t.tgparentid   = 0
            LOOP
                EXECUTE format('DROP TRIGGER %I ON %I.%I', v_trigger_name, v_child.nspname, v_child.relname);
            END LOOP;
            EXECUTE v_new_trigdef;
        END IF;
    ELSE
        FOR v_child IN
            SELECT i.inhrelid AS oid, c.relname, n.nspname
            FROM pg_inherits i
            JOIN pg_class c    ON c.oid = i.inhrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.inhparent = v_parent_oid
              AND NOT EXISTS (
                  SELECT 1 FROM pg_trigger t2
                  WHERE t2.tgrelid     = i.inhrelid
                    AND t2.tgname       = v_trigger_name
                    AND t2.tgisinternal = false
              )
        LOOP
            v_new_trigdef := regexp_replace(v_trigdef, ' ON \S+', format(' ON %I.%I', v_child.nspname, v_child.relname));

            IF v_dry_run THEN
                RAISE LOG 'Partition fix (dry-run): Would add statement-level trigger ''%'' to child %.% (parent: %.%)',
                    v_trigger_name, v_child.nspname, v_child.relname, v_schema, v_table;
                v_return_flag := FALSE;
            ELSE
                RAISE LOG 'Partition fix: Adding statement-level trigger ''%'' to child %.% (parent: %.%)',
                    v_trigger_name, v_child.nspname, v_child.relname, v_schema, v_table;
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I', v_trigger_name, v_child.nspname, v_child.relname);
                EXECUTE v_new_trigdef;
            END IF;
        END LOOP;
    END IF;
END LOOP;

RETURN v_return_flag;

END
$func$;
