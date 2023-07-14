/*
Converting traditional/normal table to a partitioned table with declarative/native partitioning.
Supporting partitioning ONLY by RANGE.

Unlike dba.partition_inheritance function, where it will not work if the table is being
referenced by other tables, native partitioning is working fine with it being referenced by others.

The function will rename original table to $TABLE_mammoth, create an empty table
called $TABLE and put it as main table and create another partition $TABLE_$v_endkey$v_interval E.g.:

From: test_partition
to:
    - test_partition (parent table)
      - test_partition_mammoth
      - test_partition_20220401_20220430
    - test_partition_template (if the column is not in PK/unique index)

    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the normal table name
    v_keycolumn     TEXT    column name which the table will be partitioned based on
    v_startkey      TEXT    starting value for the the column in the original table;
                            supports date & timestamp(tz) in YYYY-MM-DD format, and integers
    v_endkey        TEXT    new value for the new partition *)
    v_interval      TEXT    length for the new partition table, e.g.: 1 month, 1 week, 1000000000, and so on
    v_nopk          BOOLEAN set to true if the partition column is not in primary key or unique index;
                            discouraged, unless application can ensure the data validity
    v_move_trg      BOOLEAN set to true if you want to move triggers to newly partitioned table

Example:
    SELECT dba.partition_native('public','test_partition','id','1','1000000','1000');
    SELECT dba.partition_native('public','test_partition_date','creation_date','1970-01-01','2023-01-01','1 month',TRUE,TRUE);

Caveats:
    Index names from the original $TABLE are not carried to any new tables, instead the names will follow
    postgres design, e.g.: $TABLE_col1_col2_col2_idx and so on

WARNING:
    If the table is being referenced by other tables, YOU HAVE TO RUN validate constraint; we set them as not valid
    to make this function executed faster no matter the table size

Notes:
    *) Postgres native partitioning where the last value is exclusive
*/

CREATE OR REPLACE FUNCTION dba.partition_native(v_schemaname TEXT, v_tablename TEXT, v_keycolumn TEXT, v_startkey TEXT, v_endkey TEXT, v_interval TEXT, v_nopk BOOLEAN DEFAULT FALSE, v_move_trg BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN LANGUAGE plpgsql AS $func$

DECLARE
    v_suffix        TEXT := 'mammoth';
    v_references    RECORD;
    v_rows          RECORD;
    v_options       TEXT;
    v_partitionname TEXT;
    v_coltype       TEXT;
    v_newend        TEXT;
    v_newstart      TEXT;
    v_newindexname  TEXT;
    v_statement     TEXT;
    v_tablesource   TEXT;

BEGIN
    SELECT LOWER(typname::text) AS type INTO v_coltype
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = LOWER(v_schemaname::name)
        AND c.relname = LOWER(v_tablename::name)
        AND a.attname = LOWER(v_keycolumn::name);

    IF v_nopk IS TRUE THEN
        v_tablesource := v_tablename || '_template';
    ELSE
        v_tablesource := v_tablename || '_' || v_suffix;
    END IF;

    IF v_coltype ~ 'timestamp' THEN
        v_startkey := v_startkey || ' 00:00:00';
        v_endkey   := v_endkey::date + 1 || ' 00:00:00';
        v_newstart := v_endkey;
        EXECUTE format($sel$SELECT (%L::%I + %L::INTERVAL)$sel$, v_newstart, v_coltype, v_interval) INTO v_newend;
    ELSE
        IF v_coltype ~ 'int' THEN
            EXECUTE format($sel$SELECT %I(%L::%I)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSIF v_coltype = 'date' THEN
            EXECUTE format($sel$SELECT %I(%L::%I)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L::INTERVAL)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype USING ERRCODE='ADYEN';
        END IF;
    END IF;

    v_partitionname := replace(regexp_replace(v_newstart::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(v_newend::TEXT, '\ .*', ''), '-', '');
    RAISE DEBUG 'Beginning value: %, new partition value start: % and end: %, column type: %, interval: %, partition name: %',
        v_startkey, v_newstart, v_newend, v_coltype, v_interval, v_partitionname USING ERRCODE='ADYEN';

    IF v_move_trg IS TRUE THEN
        CREATE TEMP TABLE tmp_trgs AS
        SELECT tgname, pg_get_triggerdef(oid) triggerdef
        FROM pg_trigger
        WHERE tgrelid = (v_schemaname||'.'||v_tablename)::regclass AND tgtype = 21;

        FOR v_rows in
            SELECT tgname FROM tmp_trgs
        LOOP
            RAISE DEBUG 'Dropping trigger %  on original table %', v_rows.tgname, v_schemaname||'.'||v_tablename USING ERRCODE='ADYEN';
            EXECUTE format('DROP TRIGGER %I ON %I.%I', v_rows.tgname, v_schemaname, v_tablename);
        END LOOP;
    END IF;

    RAISE DEBUG 'original table name: %, new table name: %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I_%I', v_schemaname, v_tablename, v_tablename, v_suffix);

    RAISE DEBUG 'Renaming indexes ON %', v_schemaname ||'.'|| v_tablename || v_suffix USING ERRCODE='ADYEN';

    FOR v_rows IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = LOWER(v_schemaname)
            AND tablename = LOWER(v_tablename||'_'||v_suffix)
    LOOP
        IF LOWER(v_rows.indexname) ~ LOWER(v_tablename) THEN
            v_newindexname := regexp_replace(v_rows.indexname, LOWER(v_tablename), LOWER(v_tablename||'_'||v_suffix));
            IF length(v_newindexname) > 64 THEN
                v_newindexname := substring(LOWER(v_newindexname), 1, 57) || '_idx' || trunc(random() * 9 + 1);
            END IF;
        ELSE
            v_newindexname := substring(LOWER(v_tablename||'_'||v_suffix), 1, 57) || '_idx_' || trunc(random() * 9 + 1);
        END IF;
        RAISE DEBUG 'Renaming index from % to: %', v_rows.indexname, v_newindexname USING ERRCODE='ADYEN';
        EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', LOWER(v_schemaname), v_rows.indexname, v_newindexname);
    END LOOP;

    IF v_nopk IS TRUE THEN
        RAISE DEBUG 'Creating new template table % based on %', v_schemaname || '.' || v_tablesource, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I_%I INCLUDING ALL)',
            v_schemaname, v_tablesource, v_schemaname, v_tablename, v_suffix, v_keycolumn
            );

        RAISE DEBUG 'Creating partitioned table % based on %', v_schemaname || '.' || v_tablename, v_tablesource USING ERRCODE='ADYEN';
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL EXCLUDING INDEXES) PARTITION BY RANGE (%I)',
            v_schemaname, v_tablename, v_schemaname, v_tablesource, v_keycolumn
            );

        RAISE DEBUG 'Copying indexes FROM % TO %', v_schemaname ||'.'|| v_tablesource, v_schemaname ||'.'|| v_tablename USING ERRCODE='ADYEN';
        FOR v_rows IN
            WITH indexes AS (
                SELECT indexdef, indexname FROM pg_indexes
                WHERE schemaname = LOWER(v_schemaname)
                    AND tablename = LOWER(v_tablesource)
            )
            SELECT indexdef, indisprimary, indexname FROM indexes
            JOIN pg_class ON pg_class.relname = indexes.indexname
            JOIN pg_index ON pg_class.oid = pg_index.indexrelid
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_namespace.nspname = LOWER(v_schemaname)
        LOOP
            IF v_rows.indisprimary IS FALSE THEN
                IF LOWER(v_rows.indexname) ~ LOWER(v_tablesource) THEN
                    v_newindexname := regexp_replace(v_rows.indexname, LOWER(v_tablesource), LOWER(v_tablename));
                    IF length(v_newindexname) > 64 THEN
                        v_newindexname := substring(LOWER(v_newindexname), 1, 57) || '_idx' || trunc(random() * 9 + 1);
                    END IF;
                ELSE
                    -- this is forced because we need table name in indexname
                    -- otherwise it can cause further break down
                    v_newindexname := substring(LOWER(v_tablename), 1, 57) || '_idx_' || trunc(random() * 9 + 1);
                END IF;
                v_statement := regexp_replace(v_rows.indexdef, LOWER(v_schemaname) || '.' || LOWER(v_tablesource), LOWER(v_schemaname) || '.' || LOWER(v_tablename));
                v_statement := regexp_replace(v_statement, LOWER(v_rows.indexname), LOWER(v_newindexname));
                RAISE DEBUG 'Creating an index using: %', v_statement USING ERRCODE='ADYEN';
                EXECUTE format(v_statement);
            END IF;
        END LOOP;
    ELSE
        RAISE DEBUG 'Creating partitioned table % based on %', v_schemaname || '.' || v_tablename, v_tablesource USING ERRCODE='ADYEN';
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) PARTITION BY RANGE (%I)',
            v_schemaname, v_tablename, v_schemaname, v_tablesource, v_keycolumn
            );
    END IF;

    IF v_move_trg IS TRUE THEN
        FOR v_rows in
            SELECT triggerdef, tgname FROM tmp_trgs
        LOOP
            RAISE DEBUG 'Creating trigger % on new table %', v_rows.tgname, v_schemaname||'.'||v_tablename USING ERRCODE='ADYEN';
            EXECUTE format(v_rows.triggerdef);
        END LOOP;
    END IF;

    RAISE DEBUG 'Copying FK % based on %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
    FOR v_references IN
        SELECT conname, pg_get_constraintdef(oid) as statement from pg_constraint where conrelid=(LOWER(v_schemaname || '.' || v_tablename || '_' || v_suffix))::regclass and contype='f' and conparentid = 0
    LOOP
        EXECUTE format('ALTER TABLE %s.%s ADD CONSTRAINT %s %s;', v_schemaname, v_tablename,
            regexp_replace(v_references.conname, LOWER(v_tablename || '_' || v_suffix), LOWER(v_tablename), 'g'), v_references.statement);
    END LOOP;

    RAISE DEBUG 'Finding constraint from other tables connected to %', v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    CREATE TEMP TABLE tmp_fks AS
    SELECT n.nspname || '.' || cl.relname AS table_from, conname, pg_get_constraintdef(c.oid) AS condef, conrelid AS conn_table
    FROM pg_constraint c
    JOIN pg_class cl on c.conrelid = cl.oid
    JOIN pg_namespace n ON n.oid = c.connamespace
    WHERE contype = 'f' AND n.nspname = LOWER(v_schemaname)
        AND conname IN (SELECT constraint_name FROM information_schema.constraint_table_usage WHERE table_schema = LOWER(v_schemaname) AND table_name = LOWER(v_tablename || '_' || v_suffix));

    FOR v_references IN
        SELECT table_from, condef, conname
        FROM tmp_fks
    LOOP
        RAISE DEBUG 'Dropping constraint % FROM % AND add it to the (new) parent table', v_references.conname, v_references.table_from USING ERRCODE='ADYEN';
        v_references.condef := regexp_replace(v_references.condef, LOWER(v_tablename || '_' || v_suffix), LOWER(v_tablename));
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', v_references.table_from, v_references.conname);
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s NOT VALID', v_references.table_from, v_references.conname, v_references.condef);
    END LOOP;

    SELECT btrim(reloptions::text,'{}')
    INTO v_options
    FROM pg_class AS c
    JOIN pg_namespace AS ns ON c.relnamespace=ns.oid
    WHERE relname = LOWER(v_tablename || '_' || v_suffix);
    IF v_options IS NOT NULL THEN
        RAISE DEBUG 'Setting table options: %', v_options USING ERRCODE='ADYEN';
        RAISE NOTICE 'XXXXXXXXX IMPORTANT XXXXXXXXX' USING ERRCODE='ADYEN';
        RAISE NOTICE 'Run this command after you are done' USING ERRCODE='ADYEN';
        RAISE NOTICE 'ALTER TABLE % SET (%);', v_schemaname || '.' || v_tablename || '_' || v_partitionname, v_options USING ERRCODE='ADYEN';
        RAISE NOTICE 'XXXXXXXXX IMPORTANT XXXXXXXXX' USING ERRCODE='ADYEN';
        -- EXECUTE format('ALTER TABLE ' || v_schemaname || '.' || v_tablename || ' SET (' || v_options || ');');
    END IF;

    RAISE DEBUG 'Adding CHECK constraint on % called %_check, for % BETWEEN % AND %',
        v_schemaname || '.' || v_tablename || '_' || v_suffix, v_tablename || '_' ||  v_suffix, v_keycolumn, v_startkey, v_endkey USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_%I_check CHECK ((%I IS NOT NULL) AND (%I >= %L AND %I < %L)) NOT VALID',
        v_schemaname, v_tablename || '_' || v_suffix, v_tablename, v_suffix, v_keycolumn, v_keycolumn, v_startkey, v_keycolumn, v_endkey);

    RAISE DEBUG 'Setting the % CHECK as VALID', v_tablename || '_'|| v_suffix || '_check' USING ERRCODE='ADYEN';
    EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true FROM pg_namespace n WHERE c.connamespace=n.oid AND conname=%L AND nspname=%L',
        v_tablename || '_' || v_suffix || '_check', v_schemaname);

    IF v_nopk IS TRUE THEN
        v_tablesource := v_tablename || '_template';
    ELSE
        v_tablesource := v_tablename;
    END IF;

    RAISE DEBUG 'Attaching % as a child of %.', v_schemaname || '.' || v_tablename || '_' || v_suffix, v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    IF v_coltype ~ 'int' THEN
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I_%I FOR VALUES FROM (%s) TO (%s)',
            v_schemaname, v_tablename, v_schemaname, v_tablename, v_suffix, v_startkey, v_endkey);

        RAISE DEBUG 'Creating new partition % based on %.', v_schemaname || '.' || v_tablename || '_' || v_partitionname, v_schemaname || '.' || v_tablesource USING ERRCODE='ADYEN';
        EXECUTE format('CREATE TABLE %I.%I_%s (LIKE %I.%I INCLUDING ALL)',
            v_schemaname, v_tablename, v_partitionname, v_schemaname, v_tablesource
            );
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I_%s FOR VALUES FROM (%s) TO (%s)',
            v_schemaname, v_tablename, v_schemaname, v_tablename, v_partitionname, v_newstart, v_newend);
    ELSE
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I_%I FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename, v_suffix, v_startkey, v_endkey);

        RAISE DEBUG 'Creating new partition % based on %.', v_schemaname || '.' || v_tablename || '_' || v_partitionname, v_schemaname || '.' || v_tablesource USING ERRCODE='ADYEN';
        EXECUTE format('CREATE TABLE %I.%I_%s (LIKE %I.%I INCLUDING ALL)',
            v_schemaname, v_tablename, v_partitionname, v_schemaname, v_tablesource
            );
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I_%s FOR VALUES FROM (%L) TO (%L)',
            v_schemaname, v_tablename, v_schemaname, v_tablename, v_partitionname, v_newstart, v_newend);
    END IF;

    RAISE DEBUG 'Validating FKs which are referencing to the new partitioned table';
    FOR v_references IN
        SELECT conn_table, conname, table_from
        FROM tmp_fks
    LOOP
        RAISE DEBUG 'Validating constraint % FROM %', v_references.conname, v_references.table_from USING ERRCODE='ADYEN';
        EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true WHERE conname=%L AND conrelid = %L',
            v_references.conname, v_references.conn_table );
    END LOOP;

    DROP TABLE IF EXISTS tmp_trgs;
    DROP TABLE IF EXISTS tmp_fks;

    RETURN TRUE;

END
$func$;
