/*
This is a wrapper function to partition tables. It executes the following basis steps:
  - Lock the table in AccessExclusiveMode
  - Partition the table natively
  - Add two more partitions
  - Run regular statistics
  - Add a row to the table dba.partition_configuration to enable automatic maintenance for this table

The function will rename original table to $TABLE_mammoth, create an empty table
called $TABLE and put it as main table and create another partition $TABLE_$v_endkey_$v_endkey+$v_interval.
It will also run analyze with default_statistics_target = 1 on newly partitioned table. By default it will run
regular analyze after adding additional partitions, but this step can be skipped.

    PARAMETER                   TYPE                    DESCRIPTION
    v_schemaname                TEXT                    schema location for the table
    v_tablename                 TEXT                    the normal table name
    v_keycolumn                 TEXT                    column name which the table will be partitioned based on
    v_startkey                  TEXT                    starting value for the the column in the original table;
                                                        supports date & timestamp(tz) in YYYY-MM-DD format, and integers
    v_endkey                    TEXT                    exclusive upper boundary for the mammoth partition;
                                                        for integer columns pass the rounded exclusive boundary (e.g. 10000);
                                                        for date/timestamp columns pass the last inclusive date (e.g. 2022-04-01)
    v_interval                  TEXT                    length for the new partition table, e.g.: 1 month, 1 week, 1000000000, and so on

    OPTIONAL PARAMETERS
    v_move_trg                  BOOLEAN DEFAULT TRUE    set to false if you do not want to move triggers to newly partitioned table, default true
    v_detach_lock_timeout_ms    INT DEFAULT 1000        maximum time in ms to try to get locks
    v_detach_retry_sleep_sec    INT DEFAULT 20          time in seconds between execution attempts
    v_max_retries               INT DEFAULT 10          maximum number of attempts to execute the input
    v_skip_statistics           BOOLEAN DEFAULT FALSE   to limit the total lock time you can skip this step. This is not recommended
    p_allow_skipping_unique_indexes
                                BOOLEAN DEFAULT FALSE   allow unique indexes that do not include the partition key to be silently
                                                        skipped on the parent table. When FALSE (the default), an exception is raised
                                                        if such indexes are detected, because uniqueness will only be enforced per
                                                        individual partition, not across the entire table. Set to TRUE only if you
                                                        accept that cross-partition uniqueness is not guaranteed.
    p_copy_statistics_to_children
                                BOOLEAN DEFAULT FALSE   when TRUE, extended statistics objects and per-column statistics targets
                                                        (attstattarget) are kept on the first new partition so they propagate to
                                                        future partitions. When FALSE (default), those objects are dropped from
                                                        the new partition; the mammoth always retains its own statistics.

Example:
    SELECT dba.partition_table_native_wrapper('public','orders', 'date_order', '2024-01-01','2024-02-01','1 month');
    SELECT dba.partition_table_native_wrapper('public','orders', 'orderId',  '0', '10000', '10000');
    SELECT dba.partition_table_native_wrapper('public','orders', 'orderId',  '0', '10000', '10000', FALSE, 500, 10, 100, TRUE);
    SELECT dba.partition_table_native_wrapper('public','orders', 'orderId',  '0', '10000', '10000', v_max_retries => 500);
*/

CREATE OR REPLACE FUNCTION dba.partition_table_native_wrapper(
    v_schemaname TEXT,
    v_tablename TEXT,
    v_keycolumn TEXT,
    v_startkey TEXT,
    v_endkey TEXT,
    v_interval TEXT,
    v_move_trg BOOLEAN DEFAULT TRUE,
    v_detach_lock_timeout_ms int default 1000,
    v_detach_retry_sleep_sec int default 20,
    v_max_retries int default 10,
    v_skip_statistics boolean default false,
    p_allow_skipping_unique_indexes boolean default false,
    p_copy_statistics_to_children boolean default false
)
     RETURNS VOID
     LANGUAGE plpgsql
     SET search_path = pg_catalog, dba, pg_temp
     AS $func$
     DECLARE
         has_lock        boolean;
         v_coltype       text;
         v_effective_upper text;
         v_has_violations  boolean;
     BEGIN

         -- Normalize the identifiers so names with uppercase letters are matched
         -- case-insensitively, used consistently in the generated DDL, and stored
         -- lowercase in dba.partition_configuration.
         v_schemaname := LOWER(v_schemaname);
         v_tablename := LOWER(v_tablename);
         v_keycolumn := LOWER(v_keycolumn);

         -- Detect the partition key column type
         SELECT LOWER(typname::text) INTO v_coltype
             FROM pg_catalog.pg_type t
             JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
             JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
             JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = LOWER(v_schemaname::name)
             AND c.relname = LOWER(v_tablename::name)
             AND a.attname = LOWER(v_keycolumn::name);

         -- Compute the effective exclusive upper boundary for boundary checks.
         -- For timestamp types we round the date up
        IF v_coltype ~ 'timestamp' THEN
             v_effective_upper := (v_endkey::date + 1)::text;
         ELSE
             v_effective_upper := v_endkey;
         END IF;

         -- Pre-lock boundary check: fast check before acquiring the lock
         EXECUTE format('SELECT count(1) > 0 FROM %I.%I WHERE %I < %L::%s OR %I >= %L::%s',
                        v_schemaname, v_tablename,
                        v_keycolumn, v_startkey, v_coltype,
                        v_keycolumn, v_effective_upper, v_coltype)
         INTO v_has_violations;

         IF v_has_violations THEN
             RAISE EXCEPTION 'Table %.% contains data outside the partition boundaries [%, %)',
                 v_schemaname, v_tablename, v_startkey, v_effective_upper;
         END IF;

         -- Lock the table outside of the partitioning function for more control
         BEGIN
            execute format($sql$ CALL dba.lock_safe_execute(%L, null, %s, %s, %s) $sql$, format('lock table %I.%I in access exclusive mode', v_schemaname, v_tablename), v_detach_lock_timeout_ms, v_detach_retry_sleep_sec, v_max_retries);
            
            EXCEPTION
                    WHEN OTHERS THEN   
                        RAISE NOTICE 'Failed get a lock on %.%  % (Code: %)', v_schemaname, v_tablename, SQLERRM, SQLSTATE;
                        RETURN;
         END;

         RAISE DEBUG 'Lock acquired';

         -- Post-lock boundary check: authoritative check under the lock
         EXECUTE format('SELECT count(1) > 0 FROM %I.%I WHERE %I < %L::%s OR %I >= %L::%s',
                        v_schemaname, v_tablename,
                        v_keycolumn, v_startkey, v_coltype,
                        v_keycolumn, v_effective_upper, v_coltype)
         INTO v_has_violations;

         IF v_has_violations THEN
             RAISE EXCEPTION 'Table %.% contains data outside the partition boundaries [%, %) (verified under lock)',
                 v_schemaname, v_tablename, v_startkey, v_effective_upper;
         END IF;

         -- Partition the table.
         -- For integer types partition_native adds +1 to its endkey internally, so pass v_endkey - 1
         -- to make the exclusive upper boundary of the mammoth land exactly at v_endkey.
         RAISE LOG 'Partitioning the table %.%', v_schemaname, v_tablename;
         IF v_coltype ~ 'int' THEN
             PERFORM dba.partition_native(v_schemaname, v_tablename, v_keycolumn, v_startkey, (v_endkey::bigint - 1)::text, v_interval, FALSE, v_move_trg, p_allow_skipping_unique_indexes, p_copy_statistics_to_children);
         ELSE
             PERFORM dba.partition_native(v_schemaname, v_tablename, v_keycolumn, v_startkey, v_endkey, v_interval, FALSE, v_move_trg, p_allow_skipping_unique_indexes, p_copy_statistics_to_children);
         END IF;

         -- Add up to three partitions for this table
         RAISE NOTICE 'Adding two more partitions to table %.%', v_schemaname, v_tablename;
         PERFORM dba.partition_add_up_to_nr_of_free_partitions(v_schemaname, v_tablename, 3);

         -- Gather statistics with default statistics target
         -- Collecting more detailed statistics
         IF (v_skip_statistics) THEN
             RAISE LOG '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
             RAISE LOG '!!!';
             RAISE LOG '!!! Skipping statistics for table %.%. This reduces the lock time, but increases performance risks', v_schemaname, v_tablename;
             RAISE LOG '!!! Please run the command "ANALYZE (VERBOSE) %.%" as quickly as possible', v_schemaname, v_tablename ;
             RAISE LOG '!!!';
             RAISE LOG '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
         ELSE
             RAISE LOG 'Collecting default statistics for table %.%', v_schemaname, v_tablename;
             EXECUTE format('ANALYZE (VERBOSE) %I.%I', v_schemaname, v_tablename);
         END IF;

         -- Add this table for partition maintenance with all default values
         INSERT INTO dba.partition_configuration  values (v_schemaname,v_tablename,'{"auto-maintenance" : true}')
         ON CONFLICT (schema_name, table_name) DO NOTHING;

         RAISE LOG '';
         RAISE LOG 'Table %.% succesfully partitioned', v_schemaname, v_tablename;
         RAISE LOG '';

         RAISE LOG 'A line has been added to table dba.partition_configuration';
         RAISE LOG 'The partitioning framework will keep up to 3 empty partitions available at all times';
         RAISE LOG 'If you need any additional configuration, please add this manually to the configuration table';
     END
$func$;
