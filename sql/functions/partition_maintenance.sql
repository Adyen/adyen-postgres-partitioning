-- Set client_min_messages to LOG to retrieve relevant log information about the maintenance run
set client_min_messages='LOG';

-- Run the maintenance with a fixed search_path so objects in schemas writable by other roles can't take over
-- function or operator calls. It is reset at the end of the script.
SET search_path TO pg_catalog, dba, pg_temp;

-- Create new partitions based on number
SELECT dba.partition_extend_all_partitioned_tables();

-- Add date constraints to partitions
with config as (
    SELECT q.schema_name, q.table_name, d.key, d.value::json
    FROM dba.partition_configuration q
    JOIN json_each_text(configuration) d ON true
    ORDER BY 1, 2
),
constraint_set as (
    select *
    from config
    where key = 'date_constraint'
)
select dba.partition_add_constraints(
    constraint_set.schema_name,
    constraint_set.table_name,
    x.marker,
    x.constraint_column)
from constraint_set, json_to_record(constraint_set.value) as x(constraint_column text, marker text);

-- Detach date, timestamp and uuidv7 partitions
DO $$
DECLARE
    uuid_partition_count INTEGER;
BEGIN
    SELECT count(*) INTO uuid_partition_count  FROM pg_class parent JOIN pg_namespace pn ON pn.oid = parent.relnamespace JOIN pg_partitioned_table pt ON pt.partrelid = parent.oid
    JOIN pg_attribute a ON a.attrelid = parent.oid AND a.attnum = ANY(pt.partattrs) JOIN pg_type t ON t.oid = a.atttypid
    WHERE parent.relkind = 'p' AND pn.nspname NOT IN ('pg_catalog', 'information_schema') AND t.typname ~ 'uuid';
    IF uuid_partition_count > 0 THEN
        RAISE DEBUG  'UUID-partitioned table found (Count: %): Calling dba.partition_detach_partitions.', uuid_partition_count;
        call dba.partition_detach_partitions();
    ELSE
        RAISE DEBUG  'NO UUID-partitioned table found: Calling dba.partition_detach_partitions_without_uuidv7';
        call dba.partition_detach_partitions_without_uuidv7();
    END IF;
END $$;

-- Detach partitions for integer range based partitioned tables
call dba.partition_query_based_maintenance_detach_partitions();

-- Drop detached tables
WITH config AS (
    SELECT q.schema_name, q.table_name, d.key, d.value::text
    FROM dba.partition_configuration q
    JOIN json_each_text(configuration) d ON true
    ORDER BY 1, 2
),
drop_detach_set AS (
    SELECT *
    FROM config
    WHERE key = 'drop_detached'
)
SELECT
    schema_name,
    table_name,
    partition_relname
     ,dba.partition_drop_detached_partition(schema_name, table_name, partition_relname) as is_dropped
FROM drop_detach_set
LEFT JOIN LATERAL (
    SELECT partition_relname FROM dba.detached_partitions
    WHERE LOWER(parent_relname) = LOWER(drop_detach_set.table_name)
        AND detached_date <= current_date - GREATEST(drop_detach_set.value::interval, '4 days'::interval)
        AND LOWER(schema) = LOWER(drop_detach_set.schema_name)
) drop_table_set ON 1=1
where partition_relname is not null;

-- Check trigger consistency on all partitioned tables
SELECT dba.partition_check_triggers_on_all_partitions(n.nspname, c.relname)
FROM pg_partitioned_table pt
JOIN pg_class c ON c.oid = pt.partrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');

RESET search_path;
