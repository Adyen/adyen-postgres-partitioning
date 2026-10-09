/*
This procedure loops over all children of the parent table and detaches the partitions when the
interval between the query result and the current date is more than the provided interval.

The query can reference the partition name using the placeholder <<partition>>, which is replaced
with the schema-qualified partition name at runtime (or only the quoted partition name when the query
writes <schema>.<<partition>>). The query runs with search_path
pg_catalog, dba, pg_temp, so any other table or function it references must be schema-qualified.

    PARAMETER                           TYPE        DESCRIPTION
    v_schema                            TEXT        The schema location for the table
    v_parent                            TEXT        The name of the parent table
    v_query                             TEXT        The query to run; return type must be date.
                                                    Use <<partition>> as placeholder for the partition name.
    v_interval                          INTERVAL    The interval between the query result and the current date
                                                    used to determine whether the partition can be detached
    v_lock_timeout_ms                   INT         default 1000. Lock timeout in milliseconds per lock attempt.

Example:
    CALL dba.partition_query_based_detach_partitions('public', 'orders', 'SELECT someDate FROM <<partition>>', '2 years');
*/

CREATE OR REPLACE PROCEDURE dba.partition_query_based_detach_partitions(v_schema text, v_parent text, v_query text, v_interval interval, v_lock_timeout_ms int DEFAULT 1000)
LANGUAGE plpgsql
AS $proc$
DECLARE
    v_detach_partition              boolean;
    v_child_name                    text;
    v_partition_query               text;
    v_eligible_partitions           text[] := '{}';
BEGIN
    -- A procedure that commits cannot have a SET search_path clause, so pin the search_path locally.
    -- COMMIT resets a local setting, so it is applied again after every COMMIT.
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);
    -- Phase 1: Collect eligible partitions without holding any locks.
    -- Running the user-provided query here (potentially slow) is safe because
    -- no ACCESS EXCLUSIVE lock is held yet.
    FOR v_child_name IN
        EXECUTE pg_catalog.format($sql$
            SELECT c.relname
            FROM pg_class p
            JOIN pg_inherits i ON p.oid = i.inhparent
            JOIN pg_class c ON i.inhrelid = c.oid
            WHERE LOWER(p.relname) = LOWER(%L)
              AND LOWER(p.relnamespace::regnamespace::text) = LOWER(%L)
        $sql$, v_parent, v_schema)
    LOOP
        RAISE DEBUG 'table: %', v_child_name;

        -- Replace the partition placeholder in the original query with the actual partition name
        -- A placeholder the query already prefixes with a schema gets only the quoted name; a bare one is schema-qualified.
        v_partition_query := pg_catalog.replace(v_query, '.<<partition>>', '.' || pg_catalog.quote_ident(v_child_name));
        v_partition_query := pg_catalog.replace(v_partition_query, '<<partition>>', pg_catalog.format('%I.%I', v_schema, v_child_name));

        RAISE DEBUG 'Partition query: %', v_partition_query;

        -- Calculate the interval between the query result and the current date
        SELECT dba.calculate_query_date_interval(v_partition_query) > v_interval INTO v_detach_partition;

        IF (v_detach_partition) THEN
            v_eligible_partitions := array_append(v_eligible_partitions, v_child_name);
        END IF;

    END LOOP;

    RAISE LOG 'Partition maintenance: %.% - partitions to detach: %', v_schema, v_parent, v_eligible_partitions;

    -- Phase 2: Detach each eligible partition and COMMIT immediately after each one.
    -- This keeps the ACCESS EXCLUSIVE lock duration to the minimum (one DDL statement),
    -- preventing the lock from being held across multiple partition detaches.
    FOREACH v_child_name IN ARRAY v_eligible_partitions
    LOOP
        BEGIN
            EXECUTE pg_catalog.format($sql$
                CALL dba.lock_safe_execute('dba.partition_detach_partition', %L, v_max_retries => 10) $sql$,
                pg_catalog.format('%L, %L, %L, false, %s', v_schema, v_parent, v_child_name, v_lock_timeout_ms));
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE WARNING 'Failed to detach partition %.  % (Code: %)', v_child_name, SQLERRM, SQLSTATE;
                    RAISE LOG 'Failed to detach partition %.  % (Code: %)', v_child_name, SQLERRM, SQLSTATE;
        END;
        COMMIT;
        PERFORM pg_catalog.set_config('search_path', 'pg_catalog, dba, pg_temp', true);
    END LOOP;
END;
$proc$;
