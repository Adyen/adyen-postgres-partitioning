/*
This function creates a full copy of a source table under a new name with the column order
optimized for alignment padding. The following properties are copied from the source table:
  - Column definitions (types, defaults, NOT NULL, collation, storage) in optimized order
  - Indexes (including primary key)
  - Foreign key constraints
  - Check constraints (all: local and partition-inherited)
  - Storage parameters (reloptions, e.g. fillfactor, autovacuum settings)
  - Column-level options (attoptions, e.g. n_distinct)
  - Per-column statistics targets (attstattarget, set via ALTER COLUMN ... SET STATISTICS N)
  - Extended statistics objects (CREATE STATISTICS)
  - Direct triggers (excluding clones from parent, internal, and constraint triggers)
  - Table owner

The new table is identical to the source except for its name. Partition-specific steps
(partition constraint, attaching) are intentionally left to the caller.

    PARAMETER        TYPE    DESCRIPTION
    v_source_schema  TEXT    schema of the source table
    v_source_table   TEXT    source table name
    v_target_schema  TEXT    schema of the target table
    v_target_table   TEXT    target table name

Example:
    SELECT dba.create_optimized_table_copy('public', 'orders_20240101_20240201', 'public', 'orders_20240201_20240301');
*/
CREATE OR REPLACE FUNCTION dba.create_optimized_table_copy(v_source_schema TEXT, v_source_table TEXT, v_target_schema TEXT, v_target_table TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_constraint    RECORD;
    v_column_options RECORD;
    v_trigger       RECORD;
    v_stx           RECORD;
    v_reloptions    TEXT;
    v_triggerdef    TEXT;
    v_stx_def       TEXT;
    v_stx_newname   TEXT;
    v_table_owner   NAME;
BEGIN
    RAISE LOG 'Creating optimized copy of %.% as %.%',
        v_source_schema, v_source_table, v_target_schema, v_target_table;

    -- Create the table with columns in optimized alignment order
    EXECUTE dba.generate_create_table_optimized_columns(
        v_source_schema, v_source_table, v_target_schema, v_target_table
    );

    -- Copy indexes (including primary key)
    PERFORM dba.partition_copy_indexes_to_new_table(v_source_schema, v_source_table, v_target_table);

    -- Copy foreign key constraints
    PERFORM dba.partition_copy_fk_to_new_table(v_source_schema, v_source_table, v_target_table);

    -- Copy all check constraints from the source table.
    -- This includes both locally defined constraints and constraints inherited from a
    -- partitioned parent (where conislocal = false). ATTACH PARTITION requires the child
    -- to pre-have matching named constraints; native partitions never use old-style
    -- inheritance, so copying all check constraints is correct.
    FOR v_constraint IN
        SELECT conname, pg_get_constraintdef(oid) AS condef
        FROM pg_constraint
        WHERE conrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
            AND contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
            v_target_schema, v_target_table,
            replace(v_constraint.conname, LOWER(v_source_table), LOWER(v_target_table)),
            v_constraint.condef);
    END LOOP;

    -- Copy table storage parameters (e.g. fillfactor, autovacuum settings)
    EXECUTE format($sel$
        SELECT array_to_string(reloptions, ',')
        FROM pg_class
        WHERE relname = %L
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %L)
    $sel$, v_source_table, v_source_schema)
    INTO v_reloptions;

    IF v_reloptions IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I SET (%s)', v_target_schema, v_target_table, v_reloptions);
    END IF;

    -- Copy column-level options (e.g. n_distinct)
    RAISE DEBUG 'Copy column options from %.% to %.%',
        v_source_schema, v_source_table, v_target_schema, v_target_table;
    FOR v_column_options IN
        SELECT attname, unnest(attoptions) AS setting
        FROM pg_attribute
        WHERE attrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
            AND attoptions IS NOT NULL
    LOOP
        EXECUTE format('ALTER TABLE ONLY %I.%I ALTER COLUMN %I SET (%s)',
            v_target_schema, v_target_table, v_column_options.attname, v_column_options.setting);
    END LOOP;

    -- Copy per-column statistics targets (attstattarget)
    FOR v_column_options IN
        SELECT attname, attstattarget
        FROM pg_attribute
        WHERE attrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
            AND attnum > 0
            AND NOT attisdropped
            AND attstattarget != -1
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I SET STATISTICS %s',
            v_target_schema, v_target_table, v_column_options.attname, v_column_options.attstattarget);
    END LOOP;

    -- Copy extended statistics objects (CREATE STATISTICS)
    FOR v_stx IN
        SELECT s.oid AS stxoid, s.stxname
        FROM pg_statistic_ext s
        WHERE s.stxrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
    LOOP
        v_stx_def := pg_get_statisticsobjdef(v_stx.stxoid);
        -- Replace the trailing source relation with a safely quoted target relation.
        v_stx_def := regexp_replace(v_stx_def, '\sFROM\s+.+$', '')
            || format(' FROM %I.%I', v_target_schema, v_target_table);
        -- Derive new statistics object name
        IF LOWER(v_stx.stxname) ~ LOWER(v_source_table) THEN
            v_stx_newname := replace(LOWER(v_stx.stxname), LOWER(v_source_table), LOWER(v_target_table));
        ELSE
            v_stx_newname := LOWER(v_target_table) || '_' || LOWER(v_stx.stxname);
        END IF;
        v_stx_newname := left(v_stx_newname, 63);
        -- Replace the schema-qualified statistics name at the start of the DDL
        v_stx_def := format('CREATE STATISTICS %I.%I', v_target_schema, v_stx_newname)
            || regexp_replace(v_stx_def, '^CREATE STATISTICS \S+', '');
        RAISE DEBUG 'Creating extended statistics % on %.%', v_stx_newname, v_target_schema, v_target_table;
        EXECUTE v_stx_def;
    END LOOP;

    -- Copy direct triggers (tgparentid = 0) only. Cloned triggers (tgparentid != 0) are
    -- intentionally skipped: PostgreSQL re-creates them automatically when the table is
    -- attached as a partition via ATTACH PARTITION. Copying them here would create duplicates.
    -- Internal and constraint triggers are excluded.
    FOR v_trigger IN
        SELECT tgname, pg_get_triggerdef(oid) AS triggerdef
        FROM pg_trigger
        WHERE tgrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
            AND NOT tgisinternal
            AND tgconstraint = 0
            AND tgparentid = 0
    LOOP
        v_triggerdef := replace(
            v_trigger.triggerdef,
            format(' ON %I.%I', v_source_schema, v_source_table),
            format(' ON %I.%I', v_target_schema, v_target_table)
        );
        RAISE DEBUG 'Copying trigger % from %.% to %.%', v_trigger.tgname,
            v_source_schema, v_source_table, v_target_schema, v_target_table;
        EXECUTE v_triggerdef;
    END LOOP;

    -- Set table owner to match source
    SELECT tableowner
    FROM pg_tables
    WHERE schemaname = v_source_schema
        AND tablename = v_source_table
    INTO v_table_owner;

    IF v_table_owner IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', v_target_schema, v_target_table, v_table_owner);
    END IF;

    RETURN TRUE;
END
$func$;
