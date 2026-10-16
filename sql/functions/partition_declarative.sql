/*
Converting traditional/normal table to a partitioned table with declarative/native partitioning.
Supporting partitioning ONLY by RANGE.

Unlike dba.partition_inheritance function, where it will not work if the table is being
referenced by other tables, native partitioning is working fine with it being referenced by others.

The function will rename original table to $TABLE_mammoth, create an empty table
called $TABLE and put it as main table and create another partition $TABLE_$v_endkey$v_interval.
It will also run analyze with default_statistics_target = 1 on newly partitioned table.
You will have to run regular analyze after process is completed.
E.g.:

From: orders
to:
    - orders (parent table)
      - orders_mammoth
      - orders_20220401_20220430
    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the normal table name
    v_keycolumn     TEXT    column name which the table will be partitioned based on
    v_startkey      TEXT    starting value for the the column in the original table;
                            supports date & timestamp(tz) in YYYY-MM-DD format, and integers
    v_endkey        TEXT    new value for the new partition *)
    v_interval      TEXT    length for the new partition table, e.g.: 1 month, 1 week, 1000000000, and so on
    v_nopk          BOOLEAN DEPRECATED — pass FALSE or omit. TRUE raises an error.
    v_move_trg      BOOLEAN move all user-defined triggers to the new parent table (default TRUE).
                            Row-level triggers are auto-cloned by PostgreSQL to all child partitions.
                            Statement-level triggers fire once on the parent per statement.
                            Constraint and internal triggers are excluded.
    p_allow_skipping_unique_indexes
                    BOOLEAN allow unique indexes that do not include the partition key to be silently
                            skipped on the parent table (default FALSE). When FALSE, the
                            function raises an exception if such indexes are detected, because uniqueness
                            will only be enforced per individual partition, not across the entire table.
                            Set to TRUE only if you accept that cross-partition uniqueness is not guaranteed.
    p_copy_statistics_to_children
                    BOOLEAN when TRUE, extended statistics objects and per-column statistics targets
                            (attstattarget) are kept on the first new partition so they propagate to
                            future partitions via create_optimized_table_copy (default FALSE).
                            When FALSE (default), those objects are dropped from the new partition
                            after creation; the mammoth always retains its own statistics regardless.

Example:
    SELECT dba.partition_native('public','orders','order_date','2000-01-01','2022-04-01','1 month');
    SELECT dba.partition_native('public','order_logs','log_id','1','70000000','1000000');

Caveats:
    Index names from the original $TABLE are not carried to any new tables, instead the names will follow
    postgres design, e.g.: $TABLE_col1_col2_col2_idx and so on

WARNING:
    If the table is being referenced by other tables, YOU HAVE TO RUN validate constraint; we set them as not valid
    to make this function executed faster no matter the table size

Notes:
    *) Postgres native partitioning where the last value is exclusive
*/
CREATE OR REPLACE FUNCTION dba.partition_native(v_schemaname TEXT, v_tablename TEXT, v_keycolumn TEXT, v_startkey TEXT, v_endkey TEXT, v_interval TEXT, v_nopk BOOLEAN DEFAULT FALSE, v_move_trg BOOLEAN DEFAULT TRUE, p_allow_skipping_unique_indexes BOOLEAN DEFAULT FALSE, p_copy_statistics_to_children BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_suffix                    TEXT := 'mammoth';
    v_references                RECORD;
    v_rows                      RECORD;
    v_partitionname             TEXT;
    v_coltype                   TEXT;
    v_newend                    TEXT;
    v_newstart                  TEXT;
    v_newindexname              TEXT;
    v_statement                 TEXT;
    v_tablesource               TEXT;
    v_table_owner               NAME;
    has_constraint_violations   BOOLEAN :=false;
    v_idx_counter               INT;

BEGIN
    IF v_nopk IS TRUE THEN
        RAISE EXCEPTION 'partition_native: v_nopk is deprecated and cannot be TRUE. Pass FALSE or omit it.';
    END IF;

    -- Normalize the identifiers so names with uppercase letters are matched
    -- case-insensitively and used consistently in the generated DDL.
    v_schemaname := LOWER(v_schemaname);
    v_tablename := LOWER(v_tablename);
    v_keycolumn := LOWER(v_keycolumn);

    SELECT LOWER(typname::text) AS type INTO v_coltype
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = LOWER(v_schemaname::name)
        AND c.relname = LOWER(v_tablename::name)
        AND a.attname = LOWER(v_keycolumn::name);

    v_tablesource := v_tablename || '_' || v_suffix;

    IF v_coltype ~ 'timestamp' THEN
        v_startkey := v_startkey || ' 00:00:00';
        v_endkey   := v_endkey::date + 1 || ' 00:00:00';
        v_newstart := v_endkey;
        EXECUTE format($sel$SELECT (%L::%I + %L::INTERVAL)$sel$, v_newstart, v_coltype, v_interval) INTO v_newend;
    ELSE
        IF v_coltype ~ 'int' THEN
            v_endkey   := v_endkey::bigint + 1;
            v_endkey   := v_endkey::text;
            EXECUTE format($sel$SELECT %I(%L::%I)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSIF v_coltype = 'date' THEN
            EXECUTE format($sel$SELECT %I(%L::%I)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L::INTERVAL)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype;
        END IF;
    END IF;

    -- Do a check for partition boundary violations
    EXECUTE format($sel$ SELECT count(1) > 0 FROM %I.%I WHERE %I >= %L::%I $sel$, v_schemaname, v_tablename, v_keycolumn, v_endkey, v_coltype)
    INTO has_constraint_violations;

    IF (has_constraint_violations) THEN
        RAISE EXCEPTION 'The table contains data matching the requested upper boundary';
    END IF;

    SELECT tableowner FROM pg_tables WHERE schemaname = v_schemaname AND tablename = v_tablename
    INTO v_table_owner;

    v_partitionname := replace(regexp_replace(v_newstart::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(v_newend::TEXT, '\ .*', ''), '-', '');
    RAISE DEBUG 'Beginning value: %, new partition value start: % and end: %, column type: %, interval: %, partition name: %',
        v_startkey, v_newstart, v_newend, v_coltype, v_interval, v_partitionname;

    IF v_move_trg IS TRUE THEN
        -- Move all user-defined triggers to the new parent table.
        -- Row-level triggers: PostgreSQL auto-clones them to all partitions when defined on the parent.
        -- Statement-level triggers: fire once on the parent per statement, preserving original behavior.
        -- Constraint and internal triggers are excluded.
        CREATE TEMP TABLE tmp_trgs AS
        SELECT tgname, pg_get_triggerdef(oid) triggerdef
        FROM pg_trigger
        WHERE tgrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename))::regclass
            AND NOT tgisinternal
            AND tgconstraint = 0;

        FOR v_rows in
            SELECT tgname FROM tmp_trgs
        LOOP
            RAISE DEBUG 'Dropping trigger %  on original table %', v_rows.tgname, v_schemaname||'.'||v_tablename;
            EXECUTE format('DROP TRIGGER %I ON %I.%I', v_rows.tgname, v_schemaname, v_tablename);
        END LOOP;
    END IF;

    RAISE DEBUG 'original table name: %, new table name: %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', v_schemaname, v_tablename, v_tablename || '_' || v_suffix);

    RAISE DEBUG 'Renaming indexes ON %', v_schemaname ||'.'|| v_tablename || v_suffix;
    FOR v_rows IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = LOWER(v_schemaname)
            AND tablename = LOWER(v_tablename||'_'||v_suffix)
    LOOP
        IF LOWER(v_rows.indexname) ~ LOWER(v_tablename) THEN
            v_newindexname := regexp_replace(v_rows.indexname, LOWER(v_tablename), LOWER(v_tablename||'_'||v_suffix));
            IF length(v_newindexname) > 63 THEN
                -- Name exceeds PostgreSQL's 63-char limit after substitution.
                -- Truncate to 63 to preserve as much of the column-derived name as possible.
                -- Fall back to a counter-based name only when the truncated name collides.
                -- Counter sizing: prefix = 59 - length(counter::text) so that
                --   prefix + '_idx' + counter is exactly 63 chars for any counter value.
                v_newindexname := left(v_newindexname, 63);
                IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = v_newindexname AND n.nspname = LOWER(v_schemaname)) THEN
                    v_idx_counter := 1;
                    LOOP
                        v_newindexname := substring(LOWER(v_tablename||'_'||v_suffix), 1, 59 - length(v_idx_counter::text)) || '_idx' || v_idx_counter;
                        EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = v_newindexname AND n.nspname = LOWER(v_schemaname));
                        v_idx_counter := v_idx_counter + 1;
                    END LOOP;
                END IF;
            END IF;
        ELSE
            -- The index name does not contain the source table name, so a find-and-replace cannot
            -- produce a meaningful target name. Find the first available name of the form
            -- <tablename>_idx_<n> by incrementing n until no index with that name exists.
            -- Prefix is sized as 58 - length(counter::text) so the total is exactly 63 chars.
            v_idx_counter := 1;
            LOOP
                v_newindexname := substring(LOWER(v_tablename||'_'||v_suffix), 1, 58 - length(v_idx_counter::text)) || '_idx_' || v_idx_counter;
                EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = v_newindexname AND n.nspname = LOWER(v_schemaname));
                v_idx_counter := v_idx_counter + 1;
            END LOOP;
        END IF;
        RAISE DEBUG 'Renaming index from % to: %', v_rows.indexname, v_newindexname;
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', LOWER(v_schemaname), v_rows.indexname, v_newindexname);
    END LOOP;

    -- Rename extended statistics objects on the mammoth so their names include the _mammoth suffix.
    -- This mirrors the index renaming above and prevents naming conflicts when identical names are
    -- later recreated on the new parent table.
    RAISE DEBUG 'Renaming extended statistics ON %', v_schemaname ||'.'|| v_tablename || '_' || v_suffix;
    FOR v_rows IN
        SELECT s.stxname
        FROM pg_statistic_ext s
        WHERE s.stxrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename || '_' || v_suffix))::regclass
    LOOP
        IF LOWER(v_rows.stxname) ~ LOWER(v_tablename) THEN
            v_newindexname := regexp_replace(LOWER(v_rows.stxname), LOWER(v_tablename), LOWER(v_tablename || '_' || v_suffix));
            IF length(v_newindexname) > 63 THEN
                v_newindexname := left(v_newindexname, 63);
                IF EXISTS (SELECT 1 FROM pg_statistic_ext s2 JOIN pg_namespace n ON n.oid = s2.stxnamespace WHERE s2.stxname = v_newindexname AND n.nspname = LOWER(v_schemaname)) THEN
                    v_idx_counter := 1;
                    LOOP
                        v_newindexname := substring(LOWER(v_tablename || '_' || v_suffix), 1, 59 - length(v_idx_counter::text)) || '_stx' || v_idx_counter;
                        EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_statistic_ext s2 JOIN pg_namespace n ON n.oid = s2.stxnamespace WHERE s2.stxname = v_newindexname AND n.nspname = LOWER(v_schemaname));
                        v_idx_counter := v_idx_counter + 1;
                    END LOOP;
                END IF;
            END IF;
        ELSE
            v_idx_counter := 1;
            LOOP
                v_newindexname := substring(LOWER(v_tablename || '_' || v_suffix), 1, 58 - length(v_idx_counter::text)) || '_stx_' || v_idx_counter;
                EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_statistic_ext s2 JOIN pg_namespace n ON n.oid = s2.stxnamespace WHERE s2.stxname = v_newindexname AND n.nspname = LOWER(v_schemaname));
                v_idx_counter := v_idx_counter + 1;
            END LOOP;
        END IF;
        RAISE DEBUG 'Renaming extended statistics from % to: %', v_rows.stxname, v_newindexname;
        EXECUTE format('ALTER STATISTICS %I.%I RENAME TO %I', LOWER(v_schemaname), v_rows.stxname, v_newindexname);
    END LOOP;

    -- Guard: reject partitioning when incompatible unique indexes are present and the caller has not
    -- explicitly opted in. Such indexes cannot exist on the parent partitioned table, so uniqueness
    -- would only be enforced per individual partition, not across the entire table.
    -- IS NOT TRUE (rather than IS FALSE) ensures NULL is also treated as "not opted in",
    -- since NULL IS FALSE evaluates to FALSE and would silently bypass this guard.
    IF p_allow_skipping_unique_indexes IS NOT TRUE THEN
        IF EXISTS (
            SELECT 1
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_namespace ns ON ns.oid = c.relnamespace
            WHERE i.indrelid = format('%I.%I', LOWER(v_schemaname), LOWER(v_tablename || '_' || v_suffix))::regclass
              AND i.indisunique IS TRUE
              AND i.indisprimary IS FALSE
              AND NOT EXISTS (
                  SELECT 1 FROM pg_attribute a
                  WHERE a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey::int2[])
                    AND a.attname = LOWER(v_keycolumn)
              )
        ) THEN
            RAISE EXCEPTION
                'Table %.% has unique indexes that do not include the partition key column "%". '
                'These indexes cannot exist on a partitioned parent table and will be absent at the parent level. '
                'Uniqueness will only be enforced per individual partition, not across the entire table. '
                'Pass p_allow_skipping_unique_indexes := TRUE to accept this and proceed.',
                v_schemaname, v_tablename, v_keycolumn;
        END IF;
    END IF;

    RAISE DEBUG 'Creating partitioned table % based on %', v_schemaname || '.' || v_tablename, v_tablesource;
    -- Use EXCLUDING INDEXES so we can selectively recreate indexes, skipping unique indexes
    -- that do not include the partition key (PostgreSQL rejects those on partitioned tables).
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL EXCLUDING INDEXES) PARTITION BY RANGE (%I)',
        v_schemaname, v_tablename, v_schemaname, v_tablesource, v_keycolumn
        );

    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I',
        v_schemaname, v_tablename, v_table_owner
        );

    RAISE LOG 'Copying indexes FROM % TO %', v_schemaname ||'.'|| v_tablesource, v_schemaname ||'.'|| v_tablename;
    PERFORM dba.partition_copy_indexes_to_new_table(v_schemaname, v_tablesource, v_tablename, FALSE, TRUE, v_keycolumn);

    IF v_move_trg IS TRUE THEN
        FOR v_rows in
            SELECT triggerdef, tgname FROM tmp_trgs
        LOOP
            RAISE DEBUG 'Creating trigger % on new table %', v_rows.tgname, v_schemaname||'.'||v_tablename;
            EXECUTE v_rows.triggerdef;
        END LOOP;
    END IF;

    -- Copying column options from original table to the partitioned one (etc: n_distinct)
    RAISE DEBUG 'Copy column options from % to %', v_schemaname || '.' || v_tablename || '_' || v_suffix,  v_schemaname || '.' || v_tablename;
    FOR v_rows IN (SELECT attname, unnest(attoptions) as setting
                   FROM pg_attribute a
                   WHERE attrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename || '_' || v_suffix))::regclass AND attoptions IS NOT NULL)
    LOOP
        EXECUTE format('ALTER TABLE ONLY %I.%I ALTER COLUMN %I SET (%s)', v_schemaname, v_tablename, v_rows.attname, v_rows.setting);
    END LOOP;

    RAISE DEBUG 'Copying FK % based on %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix;
    FOR v_references IN
        SELECT conname, pg_get_constraintdef(oid) as statement from pg_constraint where conrelid=format('%I.%I', LOWER(v_schemaname), LOWER(v_tablename || '_' || v_suffix))::regclass and contype='f' and conparentid = 0
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s;', v_schemaname, v_tablename,
            regexp_replace(v_references.conname, LOWER(v_tablename || '_' || v_suffix), LOWER(v_tablename), 'g'), v_references.statement);
    END LOOP;

    RAISE DEBUG 'Finding foreign keys from other tables referencing to %', v_schemaname || '.' || v_tablename;
    CREATE TEMP TABLE tmp_fks AS
    SELECT quote_ident(n.nspname) || '.' || quote_ident(cl.relname) AS table_from, conname, pg_get_constraintdef(c.oid) AS condef, conrelid AS conn_table
    FROM pg_constraint c
    JOIN pg_class cl on c.conrelid = cl.oid
    JOIN pg_namespace n ON n.oid = c.connamespace
    WHERE contype = 'f' AND conparentid = 0
      AND conname IN (SELECT constraint_name FROM information_schema.constraint_table_usage WHERE table_schema = LOWER(v_schemaname) AND table_name = LOWER(v_tablename || '_' || v_suffix));

    FOR v_references IN
        SELECT table_from, condef, conname
        FROM tmp_fks
    LOOP
        RAISE DEBUG 'Dropping constraint % FROM % AND add it to the (new) parent table', v_references.conname, v_references.table_from;
        v_references.condef := regexp_replace(v_references.condef, LOWER(v_tablename || '_' || v_suffix), LOWER(v_tablename));
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', v_references.table_from, v_references.conname);

        --If table is being referenced from another partition table, we can not add NOT VALID FKs
        IF EXISTS (SELECT 1 from pg_partitioned_table where partrelid = v_references.table_from::regclass) THEN
            -- We have to first create FK on all partitions, validate them and then create FK on partitioned table
            FOR v_rows IN (SELECT n.nspname AS partition_schema, c.relname AS partition_name
                           FROM   pg_catalog.pg_inherits i
                           INNER JOIN pg_catalog.pg_class c on i.inhrelid = c.oid
                           INNER JOIN pg_catalog.pg_namespace n on c.relnamespace = n.oid
                           WHERE  inhparent = v_references.table_from::regclass)
            LOOP
                RAISE DEBUG 'Adding constraint % FROM %', v_rows.partition_name || '_' || v_tablename || '_fkey', v_rows.partition_schema || '.' || v_rows.partition_name;
                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s NOT VALID', v_rows.partition_schema, v_rows.partition_name, v_rows.partition_name || '_' || v_tablename || '_fkey', v_references.condef);

                RAISE DEBUG 'Validating constraint % FROM %', v_rows.partition_name || '_' || v_tablename || '_fkey', v_rows.partition_schema || '.' || v_rows.partition_name;
                EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true WHERE conname=%L AND conrelid = %L::regclass', v_rows.partition_name || '_' || v_tablename || '_fkey', format('%I.%I', v_rows.partition_schema, v_rows.partition_name));
            END LOOP;
            RAISE DEBUG 'Adding constraint % FROM %', v_references.conname, v_references.table_from;
            EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', v_references.table_from, v_references.conname, v_references.condef);
        ELSE
           RAISE DEBUG 'Adding constraint % FROM %', v_references.conname, v_references.table_from;
           EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s NOT VALID', v_references.table_from, v_references.conname, v_references.condef);
        END IF;
    END LOOP;

    RAISE DEBUG 'Adding CHECK constraint on % called %_check, for % BETWEEN % AND %',
        v_schemaname || '.' || v_tablename || '_' || v_suffix, v_tablename || '_' ||  v_suffix, v_keycolumn, v_startkey, v_endkey;
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK ((%I IS NOT NULL) AND (%I >= %L AND %I < %L)) NOT VALID',
        v_schemaname, v_tablename || '_' || v_suffix, v_tablename || '_' || v_suffix || '_check', v_keycolumn, v_keycolumn, v_startkey, v_keycolumn, v_endkey);

    RAISE DEBUG 'Setting the % CHECK as VALID', v_tablename || '_'|| v_suffix || '_check';
    EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true FROM pg_namespace n WHERE c.connamespace=n.oid AND conname=%L AND nspname=%L',
        v_tablename || '_' || v_suffix || '_check', v_schemaname);

    -- Use the mammoth as the template for new child partitions so that all original indexes
    -- (including unique indexes that were excluded from the parent) are propagated to children.
    v_tablesource := v_tablename || '_' || v_suffix;

    RAISE DEBUG 'Attaching % as a child of %.', v_schemaname || '.' || v_tablename || '_' || v_suffix, v_schemaname || '.' || v_tablename;
    IF v_coltype ~ 'int' THEN
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename || '_' || v_suffix, v_startkey, v_endkey);
    ELSE
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename || '_' || v_suffix, v_startkey, v_endkey);
    END IF;

    -- Drop the CHECK constraint from mammoth immediately after attach. It only exists to let
    -- ATTACH PARTITION skip a full table scan; keeping it would cause LIKE ... INCLUDING ALL
    -- to copy it to every new partition created from mammoth as a template.
    RAISE DEBUG 'Dropping CHECK constraint on % called %_check',
            v_schemaname || '.' || v_tablename || '_' || v_suffix, v_tablename || '_' || v_suffix;
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
            v_schemaname, v_tablename || '_' || v_suffix, v_tablename || '_' || v_suffix || '_check');

    RAISE DEBUG 'Creating new partition % based on %.', v_schemaname || '.' || v_tablename || '_' || v_partitionname, v_schemaname || '.' || v_tablesource;
    PERFORM dba.create_optimized_table_copy(v_schemaname, v_tablesource, v_schemaname, v_tablename || '_' || v_partitionname);

    FOR v_rows IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = LOWER(v_schemaname)
            AND tablename = LOWER(v_tablename || '_' || v_partitionname)
            AND indexname LIKE '%_mammoth%'
    LOOP
        v_newindexname := replace(v_rows.indexname, '_mammoth', '');
        RAISE DEBUG 'Renaming index % to %', v_rows.indexname, v_newindexname;
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', LOWER(v_schemaname), v_rows.indexname, v_newindexname);
    END LOOP;

    IF v_coltype ~ 'int' THEN
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename || '_' || v_partitionname, v_newstart, v_newend);
    ELSE
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename || '_' || v_partitionname, v_newstart, v_newend);
    END IF;

    RAISE DEBUG 'Validating FKs which are referencing to the new partitioned table';
    FOR v_references IN
        SELECT conn_table, conname, table_from
        FROM tmp_fks
    LOOP
        RAISE DEBUG 'Validating constraint % FROM %', v_references.conname, v_references.table_from;
        EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true WHERE conname=%L AND conrelid = %L',
            v_references.conname, v_references.conn_table );
    END LOOP;


    RAISE DEBUG 'Running analyze with default_statistics_target = 1 on partitioned table: %', v_schemaname || '.' || v_tablename;
    SET default_statistics_target = 1;

    RAISE DEBUG 'Collect non default column level statistics for a table: %', v_schemaname || '.' || v_tablename || '_' || v_suffix;
    CREATE TEMP TABLE tmp_attributes AS
    SELECT attname, attstattarget
    FROM pg_attribute
    WHERE attrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename || '_' || v_suffix))::regclass and attstattarget not in (0,-1);

    -- Setting column level statistics to 1 for a table: %', v_schemaname || '.' || v_tablename;
    FOR v_rows IN
        SELECT attname
        FROM  tmp_attributes
    LOOP
        RAISE DEBUG 'Setting statistics to 1 for a column: % in a table: %', v_schemaname || '.' || v_tablename, v_rows.attname;
        EXECUTE format('ALTER TABLE %I.%I ALTER %I SET statistics 1', v_schemaname, v_tablename, v_rows.attname);
    END LOOP;

    RAISE DEBUG 'Running actual analyze for a table: %', v_schemaname || '.' || v_tablename;
    EXECUTE format('ANALYZE (VERBOSE) %I.%I', v_schemaname, v_tablename);

    RESET default_statistics_target;

    RAISE DEBUG 'Reverting column level statistics for a table: %', v_schemaname || '.' || v_tablename;
    FOR v_rows IN
        SELECT attname, attstattarget
        FROM  tmp_attributes
    LOOP
        RAISE DEBUG 'Reverting statistics for a column: % in a table: %', v_schemaname || '_' || v_tablename, v_rows.attname;
        EXECUTE format('ALTER TABLE %I.%I ALTER %I SET statistics %s', v_schemaname, v_tablename, v_rows.attname, v_rows.attstattarget);
    END LOOP;

    -- Drop statistical objects from the new partition AFTER the ANALYZE section.
    -- This block must run last because ALTER TABLE parent ALTER COLUMN SET STATISTICS propagates
    -- to all child partitions in PostgreSQL. The ANALYZE section above temporarily sets
    -- statistics=1 on the parent (and thereby all children) for a fast analyze, then reverts
    -- to the original value -- which would overwrite any earlier reset on the new partition.
    -- Placing the drop block here ensures the partition ends up with the correct final state.
    IF p_copy_statistics_to_children IS NOT TRUE THEN
        RAISE DEBUG 'Dropping extended statistics from new partition %', v_schemaname || '.' || v_tablename || '_' || v_partitionname;
        FOR v_rows IN
            SELECT s.stxname
            FROM pg_statistic_ext s
            WHERE s.stxrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename || '_' || v_partitionname))::regclass
        LOOP
            EXECUTE format('DROP STATISTICS %I.%I', v_schemaname, v_rows.stxname);
        END LOOP;

        RAISE DEBUG 'Resetting attstattarget on new partition %', v_schemaname || '.' || v_tablename || '_' || v_partitionname;
        FOR v_rows IN
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = format('%I.%I', lower(v_schemaname), lower(v_tablename || '_' || v_partitionname))::regclass
                AND attnum > 0
                AND NOT attisdropped
                AND attstattarget != -1
        LOOP
            EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I SET STATISTICS -1',
                v_schemaname, v_tablename || '_' || v_partitionname, v_rows.attname);
        END LOOP;
    END IF;

    DROP TABLE IF EXISTS tmp_trgs;
    DROP TABLE IF EXISTS tmp_fks;
    DROP TABLE IF EXISTS tmp_attributes;

    RETURN TRUE;

END
$func$;
