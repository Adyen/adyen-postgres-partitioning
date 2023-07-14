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
        RAISE EXCEPTION 'Table not found in dba.detached_partitions';
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
    RAISE NOTICE 'Dropping table';
    EXECUTE format('DROP TABLE %I.%I', v_schema, v_partition_name);

    -- remove row from dba.detached_partitions
    EXECUTE FORMAT('DELETE FROM dba.detached_partitions WHERE schema = ''%I'' AND parent_relname = ''%I'' AND partition_relname = ''%I''' , v_schema, v_relname_parent, v_partition_name);

    RETURN TRUE;

END
$func$;
