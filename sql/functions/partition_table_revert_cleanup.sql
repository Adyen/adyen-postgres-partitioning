/*
Drops the detached partitions and retired shell left behind by partition_table_revert.

Must be called after partition_table_revert has completed (i.e. after the lock is released).
Re-verifies each detached partition is empty before dropping it.

Steps:
  1. Validate that <table>_partitioned_retired exists and the live table is not partitioned.
  2. For each partition recorded in dba.detached_partitions: verify empty, then drop.
  3. Drop the retired partitioned shell.
  4. Delete rows from dba.partition_configuration and dba.detached_partitions.

    PARAMETER       TYPE                DESCRIPTION
    v_schemaname    TEXT                schema of the table (same as passed to partition_table_revert)
    v_tablename     TEXT                table name (same as passed to partition_table_revert)
    v_dry_run       BOOLEAN DEFAULT TRUE  when TRUE, logs the plan but makes no changes

Example:
    SELECT dba.partition_table_revert_cleanup('public', 'orders', FALSE);
*/

CREATE OR REPLACE FUNCTION dba.partition_table_revert_cleanup(
    v_schemaname text,
    v_tablename text,
    v_dry_run boolean DEFAULT TRUE
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_retired_name             text;
    v_child_name               text;
    v_has_rows                 boolean;
    v_dropped_count            int := 0;
    v_skipped_count            int := 0;
    v_orig_client_min_messages text;
    v_dry_run_prefix           text;
BEGIN
    v_orig_client_min_messages := current_setting('client_min_messages');
    SET LOCAL client_min_messages = 'LOG';
    v_dry_run_prefix := CASE WHEN v_dry_run THEN '[DRY-RUN] ' ELSE '' END;

    v_retired_name := lower(v_tablename) || '_partitioned_retired';

    -- ----------------------------------------------------------------
    -- A. Validate: partition_table_revert must have completed
    -- ----------------------------------------------------------------

    PERFORM 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = lower(v_schemaname)
      AND lower(c.relname) = lower(v_retired_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% not found — has partition_table_revert been run first?',
                        v_schemaname, v_retired_name;
    END IF;

    PERFORM 1
    FROM pg_partitioned_table pt
    JOIN pg_class c     ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = lower(v_schemaname)
      AND lower(c.relname) = lower(v_tablename);

    IF FOUND THEN
        RAISE EXCEPTION '%.% is still partitioned — partition_table_revert does not appear to have completed',
                        v_schemaname, v_tablename;
    END IF;

    -- ----------------------------------------------------------------
    -- B. Drop each detached partition recorded by partition_table_revert
    -- ----------------------------------------------------------------

    FOR v_child_name IN
        SELECT partition_relname
        FROM dba.detached_partitions
        WHERE lower(schema)         = lower(v_schemaname)
          AND lower(parent_relname) = lower(v_tablename)
        ORDER BY partition_relname
    LOOP
        PERFORM 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = lower(v_schemaname)
          AND lower(c.relname) = lower(v_child_name);

        IF NOT FOUND THEN
            RAISE LOG 'partition % already gone — skipping', v_child_name;
            CONTINUE;
        END IF;

        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I LIMIT 1)', v_schemaname, v_child_name)
        INTO v_has_rows;

        IF v_has_rows THEN
            RAISE LOG 'WARNING: %.% still contains rows — skipping drop (investigate before dropping manually)',
                      v_schemaname, v_child_name;
            v_skipped_count := v_skipped_count + 1;
            CONTINUE;
        END IF;

        RAISE LOG '%dropping empty partition %.%', v_dry_run_prefix, v_schemaname, v_child_name;

        IF NOT v_dry_run THEN
            EXECUTE format('DROP TABLE %I.%I', v_schemaname, v_child_name);
        END IF;

        v_dropped_count := v_dropped_count + 1;
    END LOOP;

    -- ----------------------------------------------------------------
    -- C. Drop the retired partitioned shell
    -- ----------------------------------------------------------------

    RAISE LOG '%dropping retired shell %.%', v_dry_run_prefix, v_schemaname, v_retired_name;

    IF NOT v_dry_run THEN
        EXECUTE format('DROP TABLE %I.%I', v_schemaname, v_retired_name);
    END IF;

    -- ----------------------------------------------------------------
    -- D. Clean up dba metadata
    -- ----------------------------------------------------------------

    RAISE LOG '%cleaning up dba.partition_configuration and dba.detached_partitions for %.%',
              v_dry_run_prefix, v_schemaname, v_tablename;

    IF NOT v_dry_run THEN
        DELETE FROM dba.partition_configuration
        WHERE lower(schema_name) = lower(v_schemaname)
          AND lower(table_name)  = lower(v_tablename);

        DELETE FROM dba.detached_partitions
        WHERE lower(schema)         = lower(v_schemaname)
          AND lower(parent_relname) = lower(v_tablename);
    END IF;

    IF v_dry_run THEN
        RAISE LOG 'dry_run=true — no changes made; partitions to drop: %, skipped: %',
                  v_dropped_count, v_skipped_count;
    ELSE
        RAISE LOG 'cleanup of %.% complete: % partition(s) dropped, % skipped',
                  v_schemaname, v_tablename, v_dropped_count, v_skipped_count;
    END IF;

    PERFORM set_config('client_min_messages', v_orig_client_min_messages, true);
END;
$func$;
