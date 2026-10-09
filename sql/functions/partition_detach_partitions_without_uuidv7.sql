/*
This procedure tries to detach partitions from the parent table based on the entries from
dba.partition_configuration. If the upper boundary of a partition is older than the
specified detach interval the procedure will attempt to detach the child using lock_safe_execute.

This procedure only works on tables with range partitioning on a date or timestamp type.

After detaching the child the child name is added to the table dba.detached_partitions including
original boundaries, detach date and parent.

    PARAMETER                           TYPE    DESCRIPTION
    (none)

Example:
    CALL dba.partition_detach_partitions_without_uuidv7();
*/

CREATE OR REPLACE PROCEDURE dba.partition_detach_partitions_without_uuidv7()
LANGUAGE PLPGSQL
AS $proc$
DECLARE
    v_partitioned_table RECORD;
BEGIN
    -- A procedure that commits cannot have a SET search_path clause, so pin the search_path locally.
    -- COMMIT resets a local setting, so it is applied again after every COMMIT.
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);

    FOR v_partitioned_table IN

        -- Detach date and timestamp partitions (not uuidv7)
        WITH config AS (
            SELECT q.schema_name, q.table_name, d.key, d.value::text
            FROM dba.partition_configuration q
            JOIN json_each_text(configuration) d ON true
            ORDER BY 1, 2
        ),
        detach_set AS (
            SELECT *
            FROM config
            WHERE key = 'detach'
        ),
        timeout_set AS (
            SELECT schema_name, table_name, value::int AS lock_timeout_ms
            FROM config
            WHERE key = 'detach_lock_timeout_ms'
        ),
        -- Only process tables partitioned on a date or timestamp
        detach_date_set AS MATERIALIZED (
        SELECT
            schema_name,
            detach_set.table_name,
            LOWER(child.relname) AS partition_name,
            detach_set.value::interval AS detach_interval,
            (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[2]::date AS upper_boundary
        FROM detach_set
        JOIN pg_class parent ON LOWER(parent.relname) = LOWER(detach_set.table_name)
        JOIN pg_inherits ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
        JOIN LATERAL dba.partition_get_partition_column_info(
                detach_set.schema_name, parent.relname) AS pci ON TRUE
        WHERE
            (pci.v_column_type ~ 'timestamp' OR pci.v_column_type ~ 'date')
            AND LOWER(nmsp_parent.nspname)=LOWER(detach_set.schema_name)
            AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
            AND NOT LOWER(child.relname) ~ 'mammoth'
        )
        SELECT
            schema_name,
            detach_date_set.table_name,
            partition_name,
            COALESCE(timeout_set.lock_timeout_ms, 1000) AS lock_timeout_ms
        FROM
            detach_date_set
        LEFT JOIN timeout_set USING (schema_name, table_name)
        WHERE
           detach_date_set.upper_boundary::DATE < ( CURRENT_DATE - detach_interval )::DATE
        ORDER BY schema_name, detach_date_set.table_name, upper_boundary::DATE ASC
    LOOP
        -- Call partition_detach_partition via lock_safe_execute with a maximum of 10 attempts.
        -- If the lock cannot be acquired in 10 attempts the detachment fails and processing
        -- moves on to the next partition. v_detach_last is false so that a lock failure on one
        -- partition does not prevent detaching subsequent partitions of the same table.
        BEGIN
            EXECUTE pg_catalog.format($sql$
                CALL dba.lock_safe_execute('dba.partition_detach_partition', %L, v_max_retries => 10) $sql$,
                pg_catalog.format('%L, %L, %L, false, %s',
                    v_partitioned_table.schema_name, v_partitioned_table.table_name, v_partitioned_table.partition_name, v_partitioned_table.lock_timeout_ms));
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE LOG 'Failed to detach partition %.  % (Code: %)', v_partitioned_table.partition_name, SQLERRM, SQLSTATE;
        END;

        COMMIT;
        PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);

    END LOOP;

END;
$proc$;
