/*
This function creates new partitions for all native partitioned tables in the database untill there are at least
number_of_additional_partitions available, unused partitions. When the number of requested, free partitions already
exits the function does nothing.

When the table is partitioned based on a date or timestamp the function will create new partitions untill there are
number_of_additional_partitions partitions where the starting date/timestamp of the partition is larger than the current date.

When the table is partitioned based on an integer the function will create new partitions untill there are number_of_additional_partitions
partitions where the lower boundary of the partition is larger than the current maximum value from the table.

When the table is partitioned based on any other column type the function will return an error.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname                           TEXT    the normal table name
    v_number_of_additional_partitions   TEXT    the number of additional, unused partitions

Example:
    SELECT dba.partition_add_up_to_nr_of_free_partitions('public','test_partition', 3);
*/

CREATE OR REPLACE FUNCTION dba.partition_add_up_to_nr_of_free_partitions(v_schema TEXT, v_relname TEXT, v_number_of_additional_partitions INT)
RETURNS BOOLEAN LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $func$

DECLARE
    v_is_partitioned                BOOLEAN;
    v_column_name           	    TEXT;
    v_lastrange             	    TEXT ARRAY;
    v_lastrange_size        	    TEXT;
    v_lastpartitionname     	    TEXT;
    v_coltype               	    TEXT;
    v_newstart              	    TEXT;
    v_newend                	    TEXT;
    v_partition_suffix      	    TEXT;
    v_current_additional_partitions INT;
    v_range                         TEXT;
    v_is_range                      BOOLEAN;
    v_new_partition_name            TEXT;
    v_table_owner                   NAME;
    v_reloptions                    TEXT;
    V_ATTACH_LOCK_TIMEOUT           CONSTANT INT := 1000 ; -- ms
    V_ATTACH_RETRIES                CONSTANT INT := 3;
    V_ATTACH_RETRY_SLEEP            CONSTANT INT := 10; -- seconds

BEGIN
    -- Set the statement timeout. We don't want to block the application for too long. We need a lock to retrieve partition
    -- details and for attaching the partition.
    EXECUTE FORMAT('SET local lock_timeout TO %L', V_ATTACH_LOCK_TIMEOUT);

    v_schema:=LOWER(v_schema);
    v_relname:=LOWER(v_relname);

    RAISE DEBUG 'Checking number of free available partitions for table %', v_relname USING ERRCODE='ADYEN';

    -- Do a check if the table is actually partitioned
    EXECUTE format($sel$
        SELECT count(*) > 0
        FROM pg_partitioned_table pt
        JOIN pg_class par on par.oid = pt.partrelid
        WHERE
            LOWER(relnamespace::regnamespace::text) = LOWER('%I')
            AND LOWER(par.relname) = LOWER('%I')
    $sel$, v_schema, v_relname)
    INTO v_is_partitioned;

    IF NOT v_is_partitioned THEN
        RAISE EXCEPTION 'Table % is not a partitioned table.', v_schema || '.' || v_relname USING ERRCODE='ADYEN';
    END IF;

    -- Determine the name and type of the column used for partitioning
    EXECUTE format($sel$
        SELECT
            LOWER(col.column_name),
            t.typname
        FROM
            (SELECT
                partrelid,
                unnest(partattrs) column_index
             FROM
                 pg_partitioned_table) pt
        JOIN pg_class c on c.oid = pt.partrelid
        JOIN information_schema.columns col ON
            col.table_schema = c.relnamespace::regnamespace::text
            AND col.table_name = c.relname
            AND ordinal_position = pt.column_index
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        WHERE
            c.relname = '%I'
            and relnamespace::regnamespace::text='%I'
    $sel$, v_relname, v_schema)
    INTO v_column_name, v_coltype;

    RAISE DEBUG 'Table % is partitioned on column % of type %', v_relname, v_column_name, v_coltype USING ERRCODE='ADYEN';

    -- A table might have multiple ranges with partitions. We need to create new partitions for every range.
    -- When the table does not have multiple ranges, we only consider the single set of partitions.
    FOR v_range IN
        EXECUTE format($sel$
            SELECT  (regexp_match(child.relname, '%I_(r\d+)_.*'))[1]
            FROM pg_partitioned_table pt
            JOIN pg_class parent on pt.partrelid = parent.oid
            JOIN pg_inherits i on pt.partrelid = i.inhparent
            JOIN pg_class child on i.inhrelid = child.oid
            WHERE parent.relname = '%I'
                AND parent.relnamespace::regnamespace::text='%I'
                AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
            GROUP by (regexp_match(child.relname, '%I_(r\d+)_.*'))[1]
        $sel$, v_relname, v_relname, v_schema, v_relname)

    LOOP
        v_is_range := v_range IS NOT NULL;

        RAISE DEBUG 'is range: %', v_is_range;
        RAISE DEBUG 'current range: %', v_range;

        -- Calculate the number of free partitions for this table
        EXECUTE format($sel$  select dba.partition_calculate_free_partitions('%I', '%I', '%I', '%I', %L) $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range) INTO v_current_additional_partitions;

        RAISE DEBUG 'Number of additional partitions: %', v_current_additional_partitions USING ERRCODE='ADYEN';

        -- Check if we already have the requested number of free additional partitions
        IF v_current_additional_partitions >= v_number_of_additional_partitions THEN
            -- Nothing to do, continue to the next range
            CONTINUE;
        END IF;

        -- We need to create at least one partition

        -- Create new partitions for the table until number of desired partitions has been reached
        WHILE v_current_additional_partitions < v_number_of_additional_partitions
        LOOP
            -- Determine boundaries for the latest existing partition
            EXECUTE FORMAT($sel$
                SELECT v_childrelname, v_range FROM dba.partition_get_last_partition_details('%I', '%I', %L)
            $sel$, v_schema, v_relname, v_range )
            INTO v_lastpartitionname, v_lastrange;

            RAISE DEBUG 'schema %', v_schema USING ERRCODE='ADYEN';
            RAISE DEBUG 'Last partition %', v_lastpartitionname USING ERRCODE='ADYEN';
            RAISE DEBUG 'Last lower bound %', v_lastrange[1] USING ERRCODE='ADYEN';
            RAISE DEBUG 'Last upper bound %', v_lastrange[2] USING ERRCODE='ADYEN';

            CASE
                WHEN  v_coltype ~ 'int' THEN
                    -- Calculate the range based on latest boundaries
                    SELECT v_lastrange[2]::bigint - v_lastrange[1]::bigint INTO v_lastrange_size;
                    RAISE DEBUG 'Last range size %', v_lastrange_size USING ERRCODE='ADYEN';

                    -- Calculate boundaries for the new partition
                    SELECT v_lastrange[2] INTO v_newstart;
                    SELECT v_lastrange[2]::bigint + v_lastrange_size::bigint INTO v_newend;

                WHEN v_coltype ~ 'date' THEN
                    -- Calculate the range based on latest boundaries
                    SELECT age(v_lastrange[2]::date, v_lastrange[1]::date) INTO v_lastrange_size;
                    RAISE DEBUG 'Last range size %', v_lastrange_size USING ERRCODE='ADYEN';

                    -- Calculate boundaries for the new partition
                    SELECT v_lastrange[2] INTO v_newstart;
                    SELECT v_lastrange[2]::date + v_lastrange_size::interval INTO v_newend;
                WHEN v_coltype ~ 'timestamp' THEN
                    -- Calculate the range based on latest boundaries
                    SELECT age(v_lastrange[2]::date, v_lastrange[1]::date) INTO v_lastrange_size;
                    RAISE DEBUG 'Last range size %', v_lastrange_size USING ERRCODE='ADYEN';

                    -- Calculate boundaries for the new partition
                    SELECT v_lastrange[2] INTO v_newstart;
                    SELECT v_lastrange[2]::timestamp + v_lastrange_size::interval INTO v_newend;
                ELSE
                    RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype USING ERRCODE='ADYEN';
            END CASE;

            RAISE DEBUG 'New lower bound %', v_newstart USING ERRCODE='ADYEN';
            RAISE DEBUG 'New upper bound %', v_newend USING ERRCODE='ADYEN';

            -- Determine the suffix for the new partition in format <lower_boundary>_<upper_boundary>
            v_partition_suffix := replace(regexp_replace(v_newstart::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(v_newend::TEXT, '\ .*', ''), '-', '');

            RAISE DEBUG 'New partition suffix %', v_partition_suffix USING ERRCODE='ADYEN';

            -- Create the new partition
            v_new_partition_name := REPLACE(CONCAT(v_relname, '_', v_range, '_', v_partition_suffix), '__', '_');

            RAISE NOTICE 'Adding new partition % to table %', v_schema || '.' || v_new_partition_name, v_schema || '.' || v_relname;

            EXECUTE FORMAT('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)',
                    v_schema, v_new_partition_name, v_schema, v_lastpartitionname
                    );

            -- When the constraint on the to be attached partition doesn't overlap with the constraint on the possible
            -- available default partition we don't required an ACCESS EXCLUSIVE lock on the table.
            EXECUTE format('ALTER TABLE %I.%I add constraint partition_constraint check ((%I IS NOT NULL) AND (%I >= %L::%I) AND (%I < %L::%I))',
                    v_schema, v_new_partition_name, v_column_name, v_column_name, v_newstart, v_coltype, v_column_name, v_newend, v_coltype
                    );

            -- Copy the table storage parameters to the new partition
            EXECUTE FORMAT($sel$
                SELECT array_to_string(reloptions, ',')
                FROM pg_class
                WHERE relname = '%I' and relnamespace::regnamespace::text='%I';
            $sel$, v_lastpartitionname, v_schema)
            INTO v_reloptions;

            IF v_reloptions IS NOT NULL THEN
                EXECUTE FORMAT('ALTER TABLE %I.%I set (%s)',
                        v_schema, v_new_partition_name, v_reloptions);
            END IF;

            -- Try to attach the new table to the parent. We need a AccessExclusiveLock when a default partition exists. We
            -- try to get a lock for  V_ATTACH_LOCK_TIMEOUT ms. If we can't get the lock, we wait V_ATTACH_RETRY_SLEEP seconds
            -- and try again for a maximum of V_ATTACH_RETRIES times. If we didn't succeed in attaching the partition we drop the
            -- latest created table and exit the function with 'false'.
            FOR loop_cnt in 1..V_ATTACH_RETRIES LOOP
                BEGIN
                    -- Add the new table as partition to the parent table
                    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
                            v_schema, v_relname, v_schema, v_new_partition_name, v_newstart, v_newend);

                    SELECT tableowner FROM pg_tables WHERE schemaname = v_schema AND tablename = v_relname
                    INTO v_table_owner;

                    EXECUTE FORMAT('ALTER TABLE %I.%I OWNER TO %I',
                            v_schema, v_new_partition_name, v_table_owner);

                    -- Drop the partition constraint. This constraint is now implicitly added by the database and the one
                    -- we created is no longer required for any reason.
                    EXECUTE format('ALTER TABLE %I.%I drop constraint partition_constraint',
                            v_schema, v_new_partition_name);

                    -- Attaching succeeded. Exit the loop.
                    EXIT;

                    EXCEPTION
                        WHEN lock_not_available THEN
                            RAISE NOTICE 'Lock not available %', loop_cnt;

                            IF loop_cnt = V_ATTACH_RETRIES THEN
                                RAISE NOTICE 'Attaching table failed';

                                -- Drop the newly created table and exit
                                EXECUTE format('DROP TABLE %I.%I', v_schema, v_new_partition_name);

                                RETURN FALSE;
                            END IF;

                            PERFORM PG_SLEEP(V_ATTACH_RETRY_SLEEP);
                END;
            END LOOP;

            -- Recalculate the amount of free partitions
            EXECUTE FORMAT($sel$
                SELECT dba.partition_calculate_free_partitions('%I', '%I', '%I', '%I', %L)
            $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range)
            INTO v_current_additional_partitions;
        END LOOP;
    END LOOP;

    RETURN TRUE;
END 
$func$;
