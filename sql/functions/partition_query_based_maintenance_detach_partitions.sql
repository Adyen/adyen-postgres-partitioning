/*
This procedure retrieves all rows from dba.partition_configuration where the configuration contains
the element 'detach_query'. For all these records it calls the procedure
partition_query_based_detach_partitions to detach eligible partitions from the parent table.

After every processed table it commits the transaction.

    PARAMETER                           TYPE    DESCRIPTION
    (none)

Example:
    CALL dba.partition_query_based_maintenance_detach_partitions();
*/

CREATE OR REPLACE PROCEDURE dba.partition_query_based_maintenance_detach_partitions()
LANGUAGE PLPGSQL
AS $proc$
DECLARE
    v_schemas       text[];
    v_tables        text[];
    v_queries       text[];
    v_intervals     text[];
    v_timeouts      int[];
    v_idx           int;
BEGIN
    -- A procedure that commits cannot have a SET search_path clause, so pin the search_path locally.
    -- COMMIT resets a local setting, so it is applied again after every COMMIT.
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);

    -- Collect all configuration into arrays so no cursor is held open during the loop.
    -- A cursor-based FOR loop prevents COMMIT in called procedures; a numeric FOR loop does not.
    SELECT
        array_agg(schema_name ORDER BY configuration ->> 'order' NULLS LAST),
        array_agg(table_name ORDER BY configuration ->> 'order' NULLS LAST),
        array_agg(configuration ->> 'detach_query' ORDER BY configuration ->> 'order' NULLS LAST),
        array_agg(configuration ->> 'detach' ORDER BY configuration ->> 'order' NULLS LAST),
        array_agg((configuration ->> 'detach_lock_timeout_ms')::int ORDER BY configuration ->> 'order' NULLS LAST)
    INTO v_schemas, v_tables, v_queries, v_intervals, v_timeouts
    FROM dba.partition_configuration
    WHERE configuration::jsonb ? 'detach_query';

    -- Use CALL (not EXECUTE) so the callee is not placed in an atomic execution context,
    -- which allows it to use COMMIT to release locks after each individual partition detach.
    FOR v_idx IN 1..COALESCE(array_length(v_schemas, 1), 0)
    LOOP
        CALL dba.partition_query_based_detach_partitions(
            v_schemas[v_idx], v_tables[v_idx], v_queries[v_idx], v_intervals[v_idx]::interval,
            COALESCE(v_timeouts[v_idx], 1000));

        COMMIT;
        PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);
    END LOOP;

END;
$proc$;
