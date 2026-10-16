/*
This function tries to detach the partition from the parent table. After detaching the table the table name is added
to the table dba.detached_partitions.
If you want to detach another partition than the oldest one set v_detach_last to false.

By default it is only possible to detach the oldest available partition from a table. If you want to detach another
partition set the flag v_detach_last to false.

    PARAMETER                           TYPE    DESCRIPTION
    v_schema                            TEXT    schema location for the table
    v_relname_parent                    TEXT    the table name of the parent table
    v_partition_name                    TEXT    the name of the partition you would like to detach
    v_detach_last                       BOOLEAN default true. Set to false for detaching another partition than the oldest one
    v_lock_timeout_ms                   INT     default 1000. Lock timeout in milliseconds per lock attempt.

Example:
    SELECT dba.partition_detach_partition('public','orders', 'orders_20240101_20240201');
    SELECT dba.partition_detach_partition('public','orders', 'orders_20240101_20240201', false);
*/

CREATE OR REPLACE FUNCTION dba.partition_detach_partition(v_schema TEXT, v_relname_parent TEXT, v_partition_name TEXT, v_detach_last boolean DEFAULT TRUE, v_lock_timeout_ms INT DEFAULT 1000)
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_last_range                    TEXT ARRAY;
    v_oldest_partition_relname      TEXT;
    v_coltype                       TEXT;
    v_ref_table                     RECORD;
BEGIN

v_schema:=LOWER(v_schema);
v_relname_parent:=LOWER(v_relname_parent);
v_partition_name:=LOWER(v_partition_name);

-- Get the type of the column used for partitioning
SELECT pci.v_column_type
INTO v_coltype
FROM dba.partition_get_partition_column_info(v_schema, v_relname_parent) AS pci;

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
    LOWER(nmsp_child.nspname)=LOWER(%L)
    AND LOWER(parent.relname)=LOWER(%L)
    AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    AND NOT LOWER(child.relname) ~ 'mammoth'
-- Order by the partition lower boundary limit, casted to the partition column type.
ORDER BY (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[1]::%s asc
LIMIT 1
$sel$ , v_schema, v_relname_parent, v_coltype)
into v_oldest_partition_relname, v_last_range;

-- If the given partition name is not the name of the oldest partition we throw an exception.
-- If the given partition name doesn't exist, there is no match and we throw the same exception.
IF NOT v_oldest_partition_relname = v_partition_name THEN
    IF ( v_detach_last ) THEN
        RAISE NOTICE 'Use select dba.partition_detach_partition(''%'', ''%'', ''%'', FALSE) to detach a partition other than the oldest partition', v_schema, v_relname_parent, v_partition_name;

        RAISE EXCEPTION '% is not the oldest partition', v_partition_name;
    ELSE
        RAISE NOTICE '!!! Detaching a non-oldest partition !!!';
    END IF;

    -- Retrieve the partition boundaries for the partition to detach
    EXECUTE FORMAT($sel$
        SELECT
            regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*') as range
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
        WHERE
            LOWER(nmsp_parent.nspname)=LOWER(%L)
            AND LOWER(parent.relname)=LOWER(%L)
            AND LOWER(child.relname) = LOWER(%L)
        $sel$ , v_schema, v_relname_parent, v_partition_name)
    INTO v_last_range;
END IF;

IF ( v_last_range IS NULL ) THEN
    RAISE EXCEPTION '% is not a partition of %', v_partition_name, v_relname_parent;
END IF;

-- Set lock_timeout to bound the wait on each lock attempt.
-- If any lock cannot be acquired within v_lock_timeout_ms, lock_not_available is raised
-- and the caller (typically lock_safe_execute) handles retry and releases all locks.
EXECUTE FORMAT('SET local lock_timeout TO %s', v_lock_timeout_ms);

-- Lock FK-referenced tables before detaching to prevent deadlocks.
-- DETACH PARTITION internally acquires SHARE ROW EXCLUSIVE on referenced tables;
-- pre-acquiring ensures consistent lock ordering.
FOR v_ref_table IN
    SELECT DISTINCT c.confrelid::regclass::text AS referenced_table
    FROM pg_catalog.pg_constraint c
    WHERE c.conrelid = pg_catalog.format('%I.%I', v_schema, v_relname_parent)::regclass
      AND c.contype = 'f'
LOOP
    RAISE LOG 'Partition maintenance: Locking referenced table % in SHARE ROW EXCLUSIVE MODE', v_ref_table.referenced_table;
    EXECUTE pg_catalog.format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', v_ref_table.referenced_table);
END LOOP;

RAISE LOG 'Partition maintenance: Detaching partition % from table %', v_schema || '.' || v_partition_name, v_schema || '.' || v_relname_parent;

-- Detach the partition from the parent table
EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',
        v_schema, v_relname_parent, v_schema, v_partition_name);

-- Record the detached partition: parent, partition name, boundaries, detach date.
-- If already recorded (e.g. re-detach after re-attach), update the detached date and range.
INSERT INTO dba.detached_partitions VALUES (v_schema, v_relname_parent, v_partition_name, v_last_range, CURRENT_DATE)
    ON CONFLICT (schema, parent_relname, partition_relname)
    DO UPDATE SET detached_date = EXCLUDED.detached_date, range = EXCLUDED.range;

RETURN TRUE;

END
$func$;
