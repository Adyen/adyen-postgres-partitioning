/*
Converting traditional/normal table to a partitioned table with inheritance.
The function will rename original table to $TABLE_mammoth, create an empty table
called $TABLE and put it as a parent, create another child $TABLE_$v_endkey$v_interval
and another one $TABLE_overflow. E.g.:

From: test_partition
to:
    - test_partition (parent table)
      - test_partition_mammoth
      - test_partition_overflow
      - test_partition_20230101_20230201

    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the normal table name
    v_keycolumn     TEXT    column name which the table will be partitioned based on
    v_startkey      TEXT    starting value for the the column in the original table;
                            supports date & timestamp(tz) in YYYY-MM-DD format, and integers
    v_endkey        TEXT    end value for the column of the original table
    v_interval      TEXT    length for the new partition table, e.g.: 1 month, 1 week, 1000000000, and so on

Example:
    SELECT dba.partition_inheritance('public','test_partition','creation_date','2000-01-01','2023-01-01','1 month');
    SELECT dba.partition_inheritance('public','test_partition','id','1','70000000000','1000000000');

Caveats:
    Index names from the original $TABLE are not carried to any new tables, instead the names will follow postgres design
    e.g.: $TABLE_col1_col2_col2_idx and so on

Limitations:
    Due to the complexity of table references, this function will not work if other tables are referencing to it.
    On the other hand, foreign keys on the table will be copied over to the new tables just fine.
*/

CREATE OR REPLACE FUNCTION dba.partition_inheritance(v_schemaname TEXT, v_tablename TEXT, v_keycolumn TEXT, v_startkey TEXT, v_endkey TEXT, v_interval TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql AS $func$

DECLARE
    v_suffix        TEXT := 'mammoth';
    v_options       TEXT;
    v_referenced    RECORD;
    v_partitionname TEXT;
    v_coltype       TEXT;
    v_newend        TEXT;
    v_newstart      TEXT;

BEGIN
    SELECT LOWER(typname::text) AS type INTO v_coltype
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = LOWER(v_schemaname::name)
        AND c.relname = LOWER(v_tablename::name)
        AND a.attname = LOWER(v_keycolumn::name);

    IF v_coltype ~ 'timestamp' THEN
        IF v_startkey SIMILAR TO '[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9]' THEN
            v_startkey := v_startkey || ' 00:00:00';
        ELSE
            v_startkey := v_startkey;
        END IF;
        IF v_endkey SIMILAR TO '[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9]' THEN
            v_endkey   := v_endkey || ' 23:59:59.999999';
            v_newstart := v_endkey::date + 1;
        ELSE
            v_endkey   := v_endkey;
            v_newstart := v_endkey::timestamp + '00:00:00.000001';
        END IF;
        EXECUTE format($sel$SELECT (%L::%I + %L::INTERVAL - '1 day'::INTERVAL)$sel$, v_newstart::date || ' 23:59:59.999999'::TEXT, v_coltype, v_interval) INTO v_newend;
    ELSE
        IF v_coltype ~ 'int' THEN
            EXECUTE format($sel$SELECT %I(%L::%I + 1)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L - 1)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSIF v_coltype = 'date' THEN
            EXECUTE format($sel$SELECT %I(%L::%I + 1)$sel$, v_coltype, v_endkey, v_coltype) INTO v_newstart;
            EXECUTE format($sel$SELECT %I(%L::%I + %L::INTERVAL - '1 day'::INTERVAL)$sel$, v_coltype, v_newstart, v_coltype, v_interval) INTO v_newend;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype USING ERRCODE='ADYEN';
        END IF;
    END IF;

    RAISE DEBUG 'Finding constraint from other tables connected to %', v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    SELECT n.nspname || '.' || conrelid::regclass
    INTO v_referenced
    FROM pg_constraint c
    JOIN pg_namespace n ON n.oid = c.connamespace
    WHERE contype = 'f' AND n.nspname = LOWER(v_schemaname)
            AND conname IN (SELECT constraint_name FROM information_schema.constraint_table_usage WHERE table_schema = LOWER(v_schemaname) AND table_name = LOWER(v_tablename));
    IF v_referenced IS NOT NULL THEN
        RAISE EXCEPTION 'FAILING: table % is being referenced by other tables, partitioning using inheritance IS NOT possible', v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    END IF;

    v_partitionname := replace(regexp_replace(v_newstart::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(v_newend::TEXT, '\ .*', ''), '-', '');
    RAISE DEBUG 'Beginning value: %, new partition value start: % and end: %, column type: %, interval: %, partition name: %',
        v_startkey, v_newstart, v_newend, v_coltype, v_interval, v_partitionname USING ERRCODE='ADYEN';

    RAISE DEBUG 'original table name: %, new table name: %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I_%I', v_schemaname, v_tablename, v_tablename, v_suffix);

    RAISE DEBUG 'Creating new table % based on %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I_%I INCLUDING ALL)',
        v_schemaname, v_tablename, v_schemaname, v_tablename, v_suffix
        );

    RAISE DEBUG 'Copying FK % based on %', v_schemaname || '.' || v_tablename, v_tablename || '_' || v_suffix USING ERRCODE='ADYEN';
    PERFORM dba.partition_copy_fk_to_new_table(v_schemaname, v_tablename || '_' || v_suffix, v_tablename);

    SELECT btrim(reloptions::text,'{}')
    INTO v_options
    FROM pg_class AS c
    JOIN pg_namespace AS ns ON c.relnamespace=ns.oid
    WHERE relname = LOWER(v_tablename || '_' || v_suffix);
    IF v_options IS NOT NULL THEN
        RAISE DEBUG 'Setting table options: %', v_options USING ERRCODE='ADYEN';
        EXECUTE format('ALTER TABLE ' || v_schemaname || '.' || v_tablename || ' SET (' || v_options || ');');
    END IF;

    RAISE DEBUG 'Putting % as a child of %.', v_schemaname || '.' || v_tablename || '_' || v_suffix, v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I_%I INHERIT %I.%I',
        v_schemaname, v_tablename, v_suffix, v_schemaname, v_tablename);

    RAISE DEBUG 'Adding CHECK constraint on % called %_check, for % BETWEEN % AND %',
        v_schemaname || '.' || v_tablename || '_' || v_suffix, v_tablename || '_' ||  v_suffix, v_keycolumn, v_startkey, v_endkey USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_%I_check CHECK (%I BETWEEN %L AND %L) NOT VALID',
        v_schemaname, v_tablename || '_' || v_suffix, v_tablename, v_suffix, v_keycolumn, v_startkey, v_endkey);

    RAISE DEBUG 'Setting the % CHECK as VALID', v_tablename || '_'|| v_suffix || '_check' USING ERRCODE='ADYEN';
    EXECUTE FORMAT('UPDATE pg_constraint AS c SET convalidated=true FROM pg_namespace n WHERE c.connamespace=n.oid AND conname=%L AND nspname=%L',
        v_tablename || '_' || v_suffix || '_check', v_schemaname);

    PERFORM dba.partition_create_table_inherits_from_template(v_schemaname, v_tablename, v_partitionname),
        dba.partition_copy_indexes_to_new_table(v_schemaname, v_tablename, v_tablename || '_' || v_partitionname),
        dba.partition_copy_fk_to_new_table(v_schemaname, v_tablename, v_tablename || '_' || v_partitionname);

    RAISE DEBUG 'Adding CHECK constraint on % called %_check, for % BETWEEN % AND %',
        v_schemaname || '.' || v_tablename || '_' || v_partitionname, v_tablename || '_' || v_partitionname, v_keycolumn, v_newstart, v_newend USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_check CHECK (%I BETWEEN %L AND %L)',
        v_schemaname, v_tablename || '_' || v_partitionname, v_tablename || '_' || v_partitionname, v_keycolumn, v_newstart, v_newend);

    RAISE DEBUG 'Creating overflow table % based on %', v_schemaname || '.' || v_tablename || '_overflow', v_tablename USING ERRCODE='ADYEN';
    EXECUTE format(
            'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)',
            v_schemaname, v_tablename || '_overflow', v_schemaname, v_tablename
        );

    RAISE DEBUG 'Putting % as a child of %', v_schemaname || '.' || v_tablename || '_overflow', v_schemaname || '.' || v_tablename USING ERRCODE='ADYEN';
    EXECUTE format('ALTER TABLE %I.%I INHERIT %I.%I',
        v_schemaname, v_tablename || '_overflow', v_schemaname, v_tablename);

    PERFORM dba.partition_copy_fk_to_new_table(v_schemaname, v_tablename, v_tablename || '_overflow');

    RETURN TRUE;

END
$func$;
