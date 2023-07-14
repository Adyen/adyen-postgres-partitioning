/*
This function tries to detach the partition from the parent table. After detaching the table the table name is added
to the table dba.detached_partitions.
If you want to drop another partition than the oldest one set v_detach_last to false.

Default it is only possible to detach the oldest available partition from a table. If you want to detach another partition
set the flag v_detach_last to false.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname                           TEXT    the table name of the parent table
    v_partition_name                    TEXT    the name of the partition you would like to detach
    v_detach_last                       BOOELAN default true. Set to false for detaching another partition than the last one

Example:
    SELECT dba.partition_detach_partition('public','parent_table', 'child_table');
    SELECT dba.partition_detach_partition('public','parent_table', 'child_table', false);

*/

CREATE OR REPLACE FUNCTION dba.partition_detach_partition(v_schema TEXT, v_relname_parent TEXT, v_partition_name TEXT, v_detach_last boolean DEFAULT TRUE)
RETURNS BOOLEAN LANGUAGE plpgsql
AS $func$

DECLARE
    v_last_range                    TEXT ARRAY;
    v_oldest_partition_relname      TEXT;
    v_coltype                       TEXT;
    V_ATTACH_LOCK_TIMEOUT           CONSTANT INT := 1000 ; -- ms
    V_ATTACH_RETRIES                CONSTANT INT := 3;
    V_ATTACH_RETRY_SLEEP            CONSTANT INT := 10; -- seconds

BEGIN
    v_schema:=LOWER(v_schema);
    v_relname_parent:=LOWER(v_relname_parent);
    v_partition_name:=LOWER(v_partition_name);

    -- Set a lock timeout for all statements in this function
    EXECUTE FORMAT('SET local lock_timeout TO %L', V_ATTACH_LOCK_TIMEOUT);

    IF ( v_detach_last ) THEN
        -- Get the type of the column used for partitioning
        EXECUTE format($sel$
            SELECT
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
        $sel$, v_relname_parent, v_schema)
        INTO v_coltype;

        -- Get the oldest child for the parent
        EXECUTE FORMAT($sel$
        SELECT
            LOWER(child.relname),
            regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*') as range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
        WHERE
            LOWER(nmsp_child.nspname)=LOWER('%I')
            AND LOWER(parent.relname)=LOWER('%I')
            AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
            AND NOT LOWER(child.relname) ~ 'mammoth'
        -- Order by the partition lower boundary limit, casted to the partition column type.
        ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[1]::%s asc
        LIMIT 1
        $sel$ , v_schema, v_relname_parent, v_coltype)
        into v_oldest_partition_relname, v_last_range;

        -- If the given partition name is not the name of the oldest partition we throw an exception
        -- If the given partition name doesn't exist, there is no match and we throw the same exception
        IF NOT v_oldest_partition_relname = v_partition_name THEN
            RAISE NOTICE 'Use select dba.partition_detach_partition(''%'', ''%'', ''%'', FALSE) to detach a partition other than the oldest one', v_schema, v_relname_parent, v_partition_name;

            RAISE EXCEPTION '% is not the oldest partition', v_partition_name;
        END IF;
    ELSE
        EXECUTE FORMAT($sel$
        SELECT
            regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*') as range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
        WHERE
            LOWER(nmsp_parent.nspname)=LOWER('%I')
            AND LOWER(parent.relname)=LOWER('%I')
            AND LOWER(child.relname) = LOWER('%I')
        $sel$ , v_schema, v_relname_parent, v_partition_name)
        INTO v_last_range;

        IF ( v_last_range IS NULL ) THEN
            RAISE EXCEPTION '% is not a partition of %', v_partition_name, v_relname_parent;
        END IF;

        RAISE NOTICE '!!! Detaching a non-oldest partition !!!';
    END IF;

    -- Detach the partition
    RAISE NOTICE 'Detaching partition % from table %', v_schema || '.' || v_partition_name, v_schema || '.' || v_relname_parent;

    -- Try to detach the partition from the parent. We need a AccessExclusiveLock when a default partition exists. We
    -- try to get a lock for V_ATTACH_LOCK_TIMEOUT ms. If we can't get the lock, we wait V_ATTACH_RETRY_SLEEP seconds
    -- and try again for a maximum of V_ATTACH_RETRIES times. If we didn't succeed in detaching the partition the
    -- function returns 'false'.
    FOR loop_cnt in 1..V_ATTACH_RETRIES LOOP
        BEGIN
            -- Detach the partition from the parent table
            EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',
                    v_schema, v_relname_parent, v_schema, v_partition_name);

            -- Detaching succeeded. Exit the loop.
            EXIT;

            EXCEPTION
                WHEN lock_not_available THEN
                    RAISE NOTICE 'Lock not available %', loop_cnt;

                    IF loop_cnt = V_ATTACH_RETRIES THEN
                        RAISE NOTICE 'Detaching table failed';

                        RETURN FALSE;
                    END IF;

                    perform pg_sleep(V_ATTACH_RETRY_SLEEP);
        END;
    END LOOP;

    -- Do some book keeping: old parent, old partition name, old range, date of detaching
    INSERT INTO dba.detached_partitions VALUES (v_schema, v_relname_parent, v_partition_name, v_last_range, CURRENT_DATE);

    RETURN TRUE;

END
$func$;
