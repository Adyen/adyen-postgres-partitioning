/*
This function tries to drop a detached partition. The table must exits and be registered in dba.detached_partitions.
After dropping the table, the row is removed from dba.detached_partitions.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname                           TEXT    the table name of the parent table
    v_partition_name                    TEXT    the name of the partition you would like to drop

Example:
    SELECT dba.partition_drop_detached_partition('public','parent_table', 'child_table');
*/
CREATE OR REPLACE FUNCTION dba.partition_drop_detached_partition(v_schema TEXT, v_relname_parent TEXT, v_partition_name TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    V_ATTACH_LOCK_TIMEOUT           CONSTANT INT := 1000 ; -- ms
BEGIN

v_schema:=LOWER(v_schema);
v_relname_parent:=LOWER(v_relname_parent);
v_partition_name:=LOWER(v_partition_name);

-- Set a lock timeout for all statements in this function
EXECUTE FORMAT('SET local lock_timeout TO %L', V_ATTACH_LOCK_TIMEOUT);

-- Parent and child tables exists and combination is part of dba.detached_partitions
PERFORM 1 FROM dba.detached_partitions dp
JOIN pg_class parent ON parent.relname = parent_relname
JOIN pg_class child ON child.relname = partition_relname
WHERE
    schema = v_schema
    AND parent_relname = v_relname_parent
    AND partition_relname = v_partition_name
    AND parent.relnamespace::regnamespace::text = v_schema;

    IF NOT FOUND THEN
        -- Check if a table with the partition name exists within this schema
        PERFORM 1 FROM pg_class where relname = v_partition_name and relnamespace::regnamespace::text = v_schema;
        IF NOT FOUND THEN
            -- This table doesn't exist anymore. Remove from list of detached partitions
            RAISE LOG 'Partition maintenance: Combination of parent % and child % not found in the catalog. Removing from dba.detached_partitions', v_relname_parent, v_partition_name;
            DELETE FROM dba.detached_partitions WHERE schema = v_schema AND parent_relname = v_relname_parent AND partition_relname = v_partition_name;
            RETURN TRUE;
        END IF;

        RAISE EXCEPTION 'Combination of parent % and child % not found', v_relname_parent, v_partition_name;
    END IF;

-- partition is not attached to any table
PERFORM  1
FROM pg_class child
JOIN pg_inherits on inhrelid = oid
WHERE
    child.relname = v_partition_name
    AND child.relnamespace::regnamespace::text = v_schema;

IF FOUND THEN
    RAISE EXCEPTION 'Partition is attached to a table';
END IF;
-- drop table

RAISE LOG 'Partition maintenance: dropping partition %.%', v_schema, v_partition_name;

-- drop table
BEGIN
    execute format($sql$
        call dba.lock_safe_execute(%L, v_max_retries => 10) $sql$,
            format('DROP TABLE %I.%I', v_schema, v_partition_name)) ;
    EXCEPTION
            WHEN OTHERS THEN   
                RAISE LOG 'Failed to drop partition %.%. % (Code: %)',v_schema, v_partition_name, SQLERRM, SQLSTATE;
                RETURN false;
END;

-- remove row from dba.detached_partitions
DELETE FROM dba.detached_partitions WHERE schema = v_schema AND parent_relname = v_relname_parent AND partition_relname = v_partition_name;

RETURN TRUE;

END
$func$;
