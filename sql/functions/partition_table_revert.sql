/*
Reverts a table partitioned by partition_table_native_aligned_wrapper back to a regular (unpartitioned) table.

The procedure is safe to call within the rollback window, i.e. while all non-mammoth partitions
are still empty.  It refuses to proceed if any non-mammoth partition contains rows.

Steps:
  1. Pre-flight empty check (no lock) — fast fail if data has crossed the boundary.
  2. Take ACCESS EXCLUSIVE lock via lock_safe_execute.
  3. Re-verify all non-mammoth partitions are empty under lock.
  4. Record all incoming FK constraints (coninhcount=0) in a temp table; drop them.
  5. Detach all non-mammoth children and record them in dba.detached_partitions.
  6. Detach the mammoth partition.
  7. Rename indexes on the mammoth back to their original names (reverse of what partition_native did).
  8. Rename constraints on the mammoth that contain _mammoth back to original names.
  9. Rename the partitioned shell to <table>_partitioned_retired.
 10. Rename the mammoth to the original table name.
 11. Re-add incoming FK constraints:
     - non-partitioned referencing tables: ADD ... NOT VALID, then set convalidated=true
     - partitioned referencing tables: per-partition NOT VALID + convalidated, then ADD on parent
 12. Return — caller must COMMIT to release the lock.

After committing, call partition_table_revert_cleanup to drop the detached partitions and
remove metadata.

    PARAMETER       TYPE                DESCRIPTION
    v_schemaname    TEXT                schema of the partitioned table
    v_tablename     TEXT                name of the partitioned table
    v_dry_run       BOOLEAN DEFAULT TRUE  when TRUE, logs the plan but makes no changes

Example:
    CALL dba.partition_table_revert('public', 'orders', FALSE);
*/
CREATE OR REPLACE PROCEDURE dba.partition_table_revert(v_schemaname text, v_tablename text, v_dry_run boolean DEFAULT TRUE)
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $proc$
DECLARE
    v_mammoth_name             text;
    v_retired_name             text;
    v_child_name               text;
    v_child_range              text[];
    v_has_rows                 boolean;
    v_orig_client_min_messages text;
    v_dry_run_prefix           text;
    v_idx_name                 text;
    v_new_idx_name             text;
    v_con_name                 text;
    v_new_con_name             text;
    v_trig_name                text;
    v_trig_def                 text;
    v_fk_rec                   record;
    v_part_rec                 record;
    v_part_fk_name             text;
    v_part_fk_base             text;
    v_part_relid               oid;
    v_fk_name_counter          int;
    v_fk_count                 int;
BEGIN
    v_orig_client_min_messages := current_setting('client_min_messages');
    SET LOCAL client_min_messages = 'LOG';
    v_dry_run_prefix := CASE WHEN v_dry_run THEN '[DRY-RUN] ' ELSE '' END;

    v_mammoth_name := lower(v_tablename) || '_mammoth';
    v_retired_name := lower(v_tablename) || '_partitioned_retired';

    -- ----------------------------------------------------------------
    -- A. Validate (no lock)
    -- ----------------------------------------------------------------

    PERFORM 1
    FROM pg_partitioned_table pt
    JOIN pg_class c     ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = lower(v_schemaname)
      AND lower(c.relname) = lower(v_tablename);

    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% is not a partitioned table', v_schemaname, v_tablename;
    END IF;

    PERFORM 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE lower(n.nspname) = lower(v_schemaname)
      AND lower(c.relname) = v_mammoth_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'mammoth partition %.% not found — was this table partitioned by partition_table_native_aligned_wrapper?',
                        v_schemaname, v_mammoth_name;
    END IF;

    -- ----------------------------------------------------------------
    -- B. Pre-flight empty check (no lock, cheap fast-fail)
    -- ----------------------------------------------------------------

    FOR v_child_name IN
        SELECT lower(child.relname)
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        JOIN pg_namespace n  ON n.oid = parent.relnamespace
        WHERE lower(parent.relname) = lower(v_tablename)
          AND lower(n.nspname)      = lower(v_schemaname)
          AND NOT lower(child.relname) ~ 'mammoth'
    LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I LIMIT 1)', v_schemaname, v_child_name)
        INTO v_has_rows;

        IF v_has_rows THEN
            RAISE EXCEPTION 'cannot revert: partition %.% contains rows — data has crossed the rollback boundary',
                            v_schemaname, v_child_name;
        END IF;
    END LOOP;

    -- ----------------------------------------------------------------
    -- B1. Count incoming FK constraints (no lock) — for the plan log below
    -- ----------------------------------------------------------------

    SELECT count(DISTINCT con.conname)
    INTO v_fk_count
    FROM pg_constraint con
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype     = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = lower(v_schemaname)
      AND lower(ref_c.relname) = lower(v_tablename);

    -- ----------------------------------------------------------------
    -- C. Log the plan
    -- ----------------------------------------------------------------

    RAISE LOG '%revert plan for %.%:', v_dry_run_prefix, v_schemaname, v_tablename;
    RAISE LOG '%  LOCK %.% IN ACCESS EXCLUSIVE MODE', v_dry_run_prefix, v_schemaname, v_tablename;
    IF v_fk_count > 0 THEN
        RAISE LOG '%  DROP % incoming FK constraint(s) from other tables', v_dry_run_prefix, v_fk_count;
    END IF;
    RAISE LOG '%  DETACH all non-mammoth children (recorded in dba.detached_partitions)', v_dry_run_prefix;
    RAISE LOG '%  DETACH mammoth partition %', v_dry_run_prefix, v_mammoth_name;
    RAISE LOG '%  RENAME indexes on %.% from _mammoth back to original names', v_dry_run_prefix, v_schemaname, v_mammoth_name;
    RAISE LOG '%  RENAME constraints on %.% from _mammoth back to original names', v_dry_run_prefix, v_schemaname, v_mammoth_name;
    RAISE LOG '%  MOVE triggers from parent shell to %', v_dry_run_prefix, v_mammoth_name;
    RAISE LOG '%  RENAME %.% -> %.%', v_dry_run_prefix, v_schemaname, v_tablename,    v_schemaname, v_retired_name;
    RAISE LOG '%  RENAME %.% -> %.%', v_dry_run_prefix, v_schemaname, v_mammoth_name, v_schemaname, v_tablename;
    IF v_fk_count > 0 THEN
        RAISE LOG '%  RE-ADD % incoming FK constraint(s) as NOT VALID; validate each immediately', v_dry_run_prefix, v_fk_count;
    END IF;
    RAISE LOG '%  RETURN — caller must COMMIT to release lock; then call partition_table_revert_cleanup to drop empty tables', v_dry_run_prefix;

    IF v_dry_run THEN
        RAISE LOG 'dry_run=true — no changes made';
        PERFORM set_config('client_min_messages', v_orig_client_min_messages, true);
        RETURN;
    END IF;

    -- ----------------------------------------------------------------
    -- D. Lock the table (with retry)
    -- ----------------------------------------------------------------

    CALL dba.lock_safe_execute(
        format('lock table %I.%I in access exclusive mode', v_schemaname, v_tablename),
        null, 1000, 20, 10
    );

    RAISE LOG 'lock acquired on %.%', v_schemaname, v_tablename;

    -- ----------------------------------------------------------------
    -- E. Re-verify all non-mammoth children are empty (authoritative)
    -- ----------------------------------------------------------------

    FOR v_child_name IN
        SELECT lower(child.relname)
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        JOIN pg_namespace n  ON n.oid = parent.relnamespace
        WHERE lower(parent.relname) = lower(v_tablename)
          AND lower(n.nspname)      = lower(v_schemaname)
          AND NOT lower(child.relname) ~ 'mammoth'
    LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I LIMIT 1)', v_schemaname, v_child_name)
        INTO v_has_rows;

        IF v_has_rows THEN
            RAISE EXCEPTION 'cannot revert: %.% has rows (detected under lock) — aborting without changes',
                            v_schemaname, v_child_name;
        END IF;
    END LOOP;

    -- ----------------------------------------------------------------
    -- E1. Record all incoming FK constraints under lock (authoritative)
    --     Uses pg_get_constraintdef() so ON DELETE/UPDATE/MATCH/DEFERRABLE
    --     options are preserved exactly.  coninhcount=0 ensures we only
    --     capture the defining constraint, not auto-inherited child copies.
    -- ----------------------------------------------------------------

    DROP TABLE IF EXISTS pg_temp.tmp_incoming_fks;
    CREATE TEMP TABLE pg_temp.tmp_incoming_fks ON COMMIT DROP AS
    SELECT
        n.nspname                        AS fk_schema,
        c.relname                        AS fk_table,
        con.conname                      AS fk_name,
        pg_get_constraintdef(con.oid)    AS fk_def
    FROM pg_constraint con
    JOIN pg_class c         ON c.oid = con.conrelid
    JOIN pg_namespace n     ON n.oid = c.relnamespace
    JOIN pg_class ref_c     ON ref_c.oid = con.confrelid
    JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
    WHERE con.contype     = 'f'
      AND con.coninhcount = 0
      AND lower(ref_n.nspname) = lower(v_schemaname)
      AND lower(ref_c.relname) = lower(v_tablename);

    -- ----------------------------------------------------------------
    -- E2. Drop incoming FK constraints
    --     Dropping the parent-level constraint automatically removes any
    --     auto-inherited child copies on partitions of the referencing table.
    -- ----------------------------------------------------------------

    FOR v_fk_rec IN SELECT * FROM pg_temp.tmp_incoming_fks LOOP
        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                       v_fk_rec.fk_schema, v_fk_rec.fk_table, v_fk_rec.fk_name);
        RAISE LOG 'dropped incoming FK % on %.%', v_fk_rec.fk_name, v_fk_rec.fk_schema, v_fk_rec.fk_table;
    END LOOP;

    -- ----------------------------------------------------------------
    -- F. Detach each non-mammoth child; record for cleanup
    -- ----------------------------------------------------------------

    FOR v_child_name IN
        SELECT lower(child.relname)
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        JOIN pg_namespace n  ON n.oid = parent.relnamespace
        WHERE lower(parent.relname) = lower(v_tablename)
          AND lower(n.nspname)      = lower(v_schemaname)
          AND NOT lower(child.relname) ~ 'mammoth'
    LOOP
        SELECT regexp_match(
            pg_catalog.pg_get_expr(child.relpartbound, child.oid),
            '.*\(''?(.*?)''?\).*\(''?(.*?)''?\).*'
        )
        INTO v_child_range
        FROM pg_class child
        JOIN pg_namespace n ON n.oid = child.relnamespace
        WHERE lower(child.relname) = v_child_name
          AND lower(n.nspname)     = lower(v_schemaname);

        EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',
                       v_schemaname, v_tablename, v_schemaname, v_child_name);

        INSERT INTO dba.detached_partitions
            (schema, parent_relname, partition_relname, range, detached_date)
        VALUES
            (lower(v_schemaname), lower(v_tablename), v_child_name, v_child_range, current_date)
        ON CONFLICT (schema, parent_relname, partition_relname) DO NOTHING;

        RAISE LOG 'detached child %', v_child_name;
    END LOOP;

    -- ----------------------------------------------------------------
    -- G. Detach mammoth (now an independent table again)
    -- ----------------------------------------------------------------

    EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',
                   v_schemaname, v_tablename, v_schemaname, v_mammoth_name);

    RAISE LOG 'detached %', v_mammoth_name;

    -- ----------------------------------------------------------------
    -- G1. Rename parent shell indexes to free up original index names.
    --     After all partitions are detached the parent is an empty shell;
    --     its indexes still carry the original names (e.g. revert_target_pkey)
    --     which would collide when we rename the mammoth's indexes below.
    -- ----------------------------------------------------------------

    FOR v_idx_name IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = lower(v_schemaname)
          AND tablename  = lower(v_tablename)
    LOOP
        -- PostgreSQL identifiers are capped at 63 characters.  If appending
        -- '_retired' would exceed that limit, replace the trailing characters
        -- with the suffix instead of appending it.
        IF length(v_idx_name) + length('_retired') <= 63 THEN
            v_new_idx_name := v_idx_name || '_retired';
        ELSE
            v_new_idx_name := substring(v_idx_name, 1, 63 - length('_retired')) || '_retired';
        END IF;
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I',
                       lower(v_schemaname), v_idx_name, v_new_idx_name);
        RAISE LOG 'renamed parent shell index % -> %', v_idx_name, v_new_idx_name;
    END LOOP;

    -- ----------------------------------------------------------------
    -- G2. Rename indexes on mammoth back to original names
    -- ----------------------------------------------------------------

    FOR v_idx_name IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = lower(v_schemaname)
          AND tablename  = v_mammoth_name
    LOOP
        IF lower(v_idx_name) ~ lower(v_mammoth_name) THEN
            v_new_idx_name := regexp_replace(lower(v_idx_name), lower(v_mammoth_name), lower(v_tablename));
        ELSE
            v_new_idx_name := substring(lower(v_tablename), 1, 57) || '_idx_' || trunc(random() * 9 + 1);
        END IF;
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I',
                       lower(v_schemaname), v_idx_name, v_new_idx_name);
        RAISE LOG 'renamed index % -> %', v_idx_name, v_new_idx_name;
    END LOOP;

    -- ----------------------------------------------------------------
    -- G3. Rename constraints on mammoth back to original names
    -- ----------------------------------------------------------------

    FOR v_con_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = format('%I.%I', lower(v_schemaname), v_mammoth_name)::regclass
          AND lower(conname) ~ lower(v_mammoth_name)
    LOOP
        v_new_con_name := regexp_replace(lower(v_con_name), lower(v_mammoth_name), lower(v_tablename));
        EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
                       v_schemaname, v_mammoth_name, v_con_name, v_new_con_name);
        RAISE LOG 'renamed constraint % -> %', v_con_name, v_new_con_name;
    END LOOP;

    -- ----------------------------------------------------------------
    -- G4. Move triggers from parent shell back to mammoth
    --     Triggers were moved from the original table (now mammoth) to the
    --     parent during partitioning (partition_native with v_move_trg=TRUE).
    --     Revert must undo that: recreate each trigger on the mammoth and
    --     drop it from the parent shell.
    -- ----------------------------------------------------------------

    FOR v_trig_name, v_trig_def IN
        SELECT tgname, pg_get_triggerdef(oid)
        FROM pg_trigger
        WHERE tgrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename))::regclass
          AND NOT tgisinternal
          AND tgconstraint = 0
    LOOP
        EXECUTE replace(
            v_trig_def,
            ' ON ' || quote_ident(lower(v_schemaname)) || '.' || quote_ident(lower(v_tablename)),
            ' ON ' || quote_ident(lower(v_schemaname)) || '.' || quote_ident(v_mammoth_name)
        );

        EXECUTE format('DROP TRIGGER %I ON %I.%I', v_trig_name, v_schemaname, v_tablename);
        RAISE LOG 'moved trigger % to %', v_trig_name, v_mammoth_name;
    END LOOP;

    -- ----------------------------------------------------------------
    -- H. Rename: parent shell -> _partitioned_retired, mammoth -> original
    -- ----------------------------------------------------------------

    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', v_schemaname, v_tablename,    v_retired_name);
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', v_schemaname, v_mammoth_name, v_tablename);

    RAISE LOG 'renamed % -> %; renamed % -> %',
              v_tablename, v_retired_name, v_mammoth_name, v_tablename;

    -- ----------------------------------------------------------------
    -- H1. Re-add incoming FK constraints and validate only those we touched
    --
    --     For non-partitioned referencing tables:
    --       ADD ... NOT VALID  (skips the table scan — data was already valid)
    --       UPDATE pg_constraint SET convalidated=true  (for this constraint only)
    --
    --     For partitioned referencing tables (mirrors partition_native forward path):
    --       ADD NOT VALID on each child partition + convalidated=true per child
    --       ADD on the partitioned parent (succeeds because all children are valid,
    --       no full-table scan is needed)
    --
    --     The partitioned check uses the table NAME resolved after the rename, so
    --     a self-referencing FK is correctly treated as non-partitioned (the mammoth
    --     is not partitioned).
    -- ----------------------------------------------------------------

    FOR v_fk_rec IN SELECT * FROM pg_temp.tmp_incoming_fks LOOP
        IF EXISTS (
            SELECT 1 FROM pg_partitioned_table pt
            JOIN pg_class c     ON c.oid = pt.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE lower(n.nspname) = lower(v_fk_rec.fk_schema)
              AND lower(c.relname) = lower(v_fk_rec.fk_table)
        ) THEN
            -- Partitioned referencing table: add NOT VALID per partition,
            -- validate each via catalog update, then add on the parent.
            FOR v_part_rec IN
                SELECT n.nspname AS part_schema, c.relname AS part_name
                FROM pg_inherits i
                JOIN pg_class c     ON c.oid = i.inhrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.inhparent = format('%I.%I', v_fk_rec.fk_schema, v_fk_rec.fk_table)::regclass
            LOOP
                -- Derive the per-partition constraint name from the source FK name
                -- rather than the referenced table name: a referencing table may hold
                -- several distinct FKs pointing at the same target, and those would all
                -- collapse to one name if the target name were used. FK names are unique
                -- per table, so this keeps each per-partition name distinct.
                v_part_fk_base := v_part_rec.part_name || '_' || v_fk_rec.fk_name;
                v_part_fk_name := v_part_fk_base || '_fkey';
                v_part_relid   := (quote_ident(v_part_rec.part_schema) || '.' || quote_ident(v_part_rec.part_name))::regclass;

                -- PostgreSQL caps identifiers at 63 characters. Truncating can map two
                -- distinct FK names onto the same identifier, so append an incrementing
                -- counter until the name is free on this partition. Constraint names only
                -- need to be unique per table, hence the conrelid-scoped check.
                IF length(v_part_fk_name) > 63 THEN
                    v_fk_name_counter := 1;
                    LOOP
                        v_part_fk_name := left(v_part_fk_base,
                                               63 - length('_fkey') - length(v_fk_name_counter::text))
                                          || '_fkey' || v_fk_name_counter;
                        EXIT WHEN NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = v_part_fk_name
                              AND conrelid = v_part_relid
                        );
                        v_fk_name_counter := v_fk_name_counter + 1;
                    END LOOP;
                END IF;

                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s NOT VALID',
                    v_part_rec.part_schema, v_part_rec.part_name, v_part_fk_name, v_fk_rec.fk_def);
                UPDATE pg_constraint SET convalidated = true
                WHERE conname = v_part_fk_name
                  AND conrelid = v_part_relid;
                RAISE LOG 'added + validated FK % on partition %.%',
                          v_part_fk_name, v_part_rec.part_schema, v_part_rec.part_name;
            END LOOP;
            -- Add on the partitioned parent (no NOT VALID: all children are already valid)
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                v_fk_rec.fk_schema, v_fk_rec.fk_table, v_fk_rec.fk_name, v_fk_rec.fk_def);
            RAISE LOG 'recreated FK % on partitioned table %.%',
                      v_fk_rec.fk_name, v_fk_rec.fk_schema, v_fk_rec.fk_table;
        ELSE
            -- Non-partitioned referencing table: add NOT VALID, then validate
            -- only this specific constraint via a targeted catalog update.
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s NOT VALID',
                v_fk_rec.fk_schema, v_fk_rec.fk_table, v_fk_rec.fk_name, v_fk_rec.fk_def);
            EXECUTE format('UPDATE pg_constraint SET convalidated=true WHERE conname=%L AND conrelid=%L::regclass',
                v_fk_rec.fk_name, format('%I.%I', v_fk_rec.fk_schema, v_fk_rec.fk_table));
            RAISE LOG 'recreated + validated FK % on %.%',
                      v_fk_rec.fk_name, v_fk_rec.fk_schema, v_fk_rec.fk_table;
        END IF;
    END LOOP;

    -- ----------------------------------------------------------------
    -- I. Done — caller must COMMIT to release the ACCESS EXCLUSIVE lock
    -- ----------------------------------------------------------------

    RAISE LOG 'revert of %.% complete — lock released; call partition_table_revert_cleanup to drop detached partitions and the retired shell (%.%)',
              v_schemaname, v_tablename, v_schemaname, v_retired_name;
    RAISE LOG 'run: SELECT dba.partition_table_revert_cleanup(''%'', ''%'', FALSE);',
              v_schemaname, v_tablename;
    PERFORM set_config('client_min_messages', v_orig_client_min_messages, true);
END;
$proc$;
