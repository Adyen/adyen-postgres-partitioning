/*
This function creates new partitions for all native partitioned tables in the database until there are at least
number_of_additional_partitions available, unused partitions. When the number of requested, free partitions already
exits the function does nothing.

When the table is partitioned based on a date, timestamp or uuid the function will create new partitions until there are
number_of_additional_partitions partitions where the starting date/timestamp of the partition is larger than the current date.

When the table is partitioned based on an integer the function will create new partitions untill there are number_of_additional_partitions
partitions where the lower boundary of the partition is larger than the current maximum value from the table.

When the table is partitioned based on any other column type the function will return an error.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname                           TEXT    the normal table name
    v_number_of_additional_partitions   TEXT    the number of additional, unused partitions

Example:
    SELECT dba.partition_add_up_to_nr_of_free_partitions('public','orders', 3);
*/
CREATE OR REPLACE FUNCTION dba.partition_add_up_to_nr_of_free_partitions(v_schema TEXT, v_relname TEXT, v_number_of_additional_partitions INT)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
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
    V_ATTACH_LOCK_TIMEOUT           CONSTANT INT := 1000 ; -- ms
    V_ATTACH_RETRIES                CONSTANT INT := 3;
    V_ATTACH_RETRY_SLEEP            CONSTANT INT := 10; -- seconds
BEGIN

-- Set the statement timeout. We don't want to block the application for too long. We need a lock to retrieve partition
-- details and for attaching the partition.
EXECUTE FORMAT('SET local lock_timeout TO %L', V_ATTACH_LOCK_TIMEOUT);

v_schema:=LOWER(v_schema);
v_relname:=LOWER(v_relname);

RAISE DEBUG 'Checking number of free available partitions for table %', v_relname;

-- Do a check if the table is actually partitioned
EXECUTE format($sel$
    SELECT count(*) > 0
    FROM pg_partitioned_table pt
    JOIN pg_class par on par.oid = pt.partrelid
    WHERE
        LOWER(relnamespace::regnamespace::text) = LOWER(quote_ident(%L))
        AND LOWER(par.relname) = LOWER(%L)
$sel$, v_schema, v_relname)
INTO v_is_partitioned;

IF NOT v_is_partitioned THEN
    RAISE EXCEPTION 'Table % is not a partitioned table.', v_schema || '.' || v_relname;
END IF;

-- Determine the name and type of the column used for partitioning
SELECT pci.v_column_name, pci.v_column_type
INTO v_column_name, v_coltype
FROM dba.partition_get_partition_column_info(v_schema, v_relname) AS pci;

RAISE DEBUG 'Table % is partitioned on column % of type %', v_relname, v_column_name, v_coltype;

-- A table might have multiple ranges with partitions. We need to create new partitions for every range.
-- When the table does not have multiple ranges, we only consider the single set of partitions.
FOR v_range IN
    EXECUTE format($sel$
        SELECT  (regexp_match(child.relname, %L || '_(r\d+)_.*'))[1]
        FROM pg_partitioned_table pt
        JOIN pg_class parent on pt.partrelid = parent.oid
        JOIN pg_inherits i on pt.partrelid = i.inhparent
        JOIN pg_class child on i.inhrelid = child.oid
        WHERE parent.relname = %L
            AND parent.relnamespace::regnamespace::text=quote_ident(%L)
            AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
        GROUP by (regexp_match(child.relname, %L || '_(r\d+)_.*'))[1]
    $sel$, v_relname, v_relname, v_schema, v_relname)

LOOP

    v_is_range := v_range IS NOT NULL;

    RAISE DEBUG 'is range: %', v_is_range;
    RAISE DEBUG 'current range: %', v_range;

    -- Calculate the number of free partitions for this table
    EXECUTE format($sel$  select dba.partition_calculate_free_partitions(%L, %L, %L, %L, %L) $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range) INTO v_current_additional_partitions;

    RAISE DEBUG 'Number of additional partitions: %', v_current_additional_partitions;

    -- Check if we already have the requested number of free additional partitions
    IF v_current_additional_partitions >= v_number_of_additional_partitions THEN
        -- Nothing to do, continue to the next range
        CONTINUE;
    END IF;

    -- We need to create at least one more partition

    -- Create new partitions for the table until number of desired partitions has been reached
    WHILE v_current_additional_partitions < v_number_of_additional_partitions
    LOOP
        -- Determine boundaries for the latest existing partition
        EXECUTE format( $sel$ select v_childrelname, v_range from dba.partition_get_last_partition_details(%L, %L, %L) $sel$, v_schema, v_relname, v_range )
        INTO v_lastpartitionname, v_lastrange;

        RAISE DEBUG 'schema %', v_schema;
        RAISE DEBUG 'Last partition %', v_lastpartitionname;
        RAISE DEBUG 'Last lower bound %', v_lastrange[1];
        RAISE DEBUG 'Last upper bound %', v_lastrange[2];

        CASE
            WHEN  v_coltype ~ 'int' THEN
                -- Calculate the range based on latest boundaries
                SELECT v_lastrange[2]::bigint - v_lastrange[1]::bigint INTO v_lastrange_size;
                RAISE DEBUG 'Last range size %', v_lastrange_size;

                -- Calculate boundaries for the new partition
                SELECT v_lastrange[2] INTO v_newstart;
                SELECT v_lastrange[2]::bigint + v_lastrange_size::bigint INTO v_newend;

            WHEN v_coltype ~ 'date' THEN
                -- Calculate the range based on latest boundaries
                SELECT age(v_lastrange[2]::date, v_lastrange[1]::date) INTO v_lastrange_size;
                RAISE DEBUG 'Last range size %', v_lastrange_size;

                -- Calculate boundaries for the new partition
                SELECT v_lastrange[2] INTO v_newstart;
                SELECT v_lastrange[2]::date + v_lastrange_size::interval INTO v_newend;
            WHEN v_coltype ~ 'timestamp' THEN
                -- Calculate the range based on latest boundaries
                SELECT age(v_lastrange[2]::date, v_lastrange[1]::date) INTO v_lastrange_size;
                RAISE DEBUG 'Last range size %', v_lastrange_size;

                -- Calculate boundaries for the new partition
                SELECT v_lastrange[2] INTO v_newstart;
                SELECT v_lastrange[2]::timestamp + v_lastrange_size::interval INTO v_newend;
            WHEN v_coltype ~ 'uuid' THEN
                -- Calculate the range based on latest boundaries
                SELECT age(dba.uuid_v7_to_timestamptz(v_lastrange[2]::uuid), dba.uuid_v7_to_timestamptz(v_lastrange[1]::uuid)) INTO v_lastrange_size;
                RAISE DEBUG 'Last range size %', v_lastrange_size;

                -- Calculate boundaries for the new partition
                SELECT v_lastrange[2] INTO v_newstart;
                SELECT dba.uuid_timestamptz_to_v7(dba.uuid_v7_to_timestamptz(v_lastrange[2]::uuid) + v_lastrange_size::interval, true) INTO v_newend;
            ELSE
                RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype;
        END CASE;

        RAISE DEBUG 'New lower bound %', v_newstart;
        RAISE DEBUG 'New upper bound %', v_newend;

        -- Determine the suffix for the new partition in format <lower_boundary>_<upper_boundary>
        -- In case of uuid's we use the date to improve readability.
        CASE
            WHEN v_coltype ~ 'uuid' THEN
                v_partition_suffix := replace(regexp_replace(dba.uuid_v7_to_timestamptz(v_newstart::uuid)::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(dba.uuid_v7_to_timestamptz(v_newend::uuid)::TEXT, '\ .*', ''), '-', '');
            ELSE
                v_partition_suffix := replace(regexp_replace(v_newstart::TEXT, '\ .*', ''), '-', '') || '_' || replace(regexp_replace(v_newend::TEXT, '\ .*', ''), '-', '');
        END CASE;

        RAISE DEBUG 'New partition suffix %', v_partition_suffix;

        -- Create the new partition
        v_new_partition_name := replace(concat(v_relname, '_', v_range, '_', v_partition_suffix), '__', '_');

        RAISE LOG 'Partition maintenance: Adding new partition % to table %', v_schema || '.' || v_new_partition_name, v_schema || '.' || v_relname;

        PERFORM dba.create_optimized_table_copy(
                v_schema, v_lastpartitionname, v_schema, v_new_partition_name
                );

        -- When the constraint on the to be attached partition doesn't overlap with the constraint on the possible
        -- available default partition we don't required an ACCESS EXCLUSIVE lock on the table.
        EXECUTE format('ALTER TABLE %I.%I add constraint partition_constraint check ((%I IS NOT NULL) AND (%I >= %L::%I) AND (%I < %L::%I))',
                v_schema, v_new_partition_name, v_column_name, v_column_name, v_newstart, v_coltype, v_column_name, v_newend, v_coltype
                );

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

                EXECUTE format('ALTER TABLE %I.%I OWNER TO %I',
                        v_schema, v_new_partition_name, v_table_owner);

                -- Drop the partition constraint. This constraint is now implicitly added by the database and the one
                -- we created is no longer required for any reason.
                EXECUTE format('ALTER TABLE %I.%I drop constraint partition_constraint',
                        v_schema, v_new_partition_name);

                -- Attaching succeeded. Exit the loop.
                EXIT;

                EXCEPTION
                    WHEN lock_not_available THEN
                        RAISE LOG 'Partition maintenance: Lock not available %', loop_cnt;

                        IF loop_cnt = V_ATTACH_RETRIES THEN
                            RAISE LOG 'Partition maintenance: Attaching table failed';

                            -- Drop the newly created table and exit
                            EXECUTE format('DROP TABLE %I.%I', v_schema, v_new_partition_name);

                            RETURN FALSE;
                        END IF;

                        perform pg_sleep(V_ATTACH_RETRY_SLEEP);
            END;
        END LOOP;

        -- Recalculate the amount of free partitions
        EXECUTE format($sel$  select dba.partition_calculate_free_partitions(%L, %L, %L, %L, %L) $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range) INTO v_current_additional_partitions;
    END LOOP;
END LOOP;

RETURN TRUE;

END 
$func$;
