-- Create new partitions based on number
SELECT
    dba.partition_add_up_to_nr_of_free_partitions(
        relnamespace::regnamespace::text,
        par.relname,
        GREATEST(3, CAST(configuration ->> 'nr' AS INT) + 2))
FROM pg_partitioned_table pt
JOIN pg_class par ON par.oid = pt.partrelid
JOIN dba.partition_configuration cfg ON
    LOWER(cfg.schema_name) = LOWER(relnamespace::regnamespace::text)
    AND LOWER(cfg.table_name) = LOWER(par.relname)
WHERE
    CAST(configuration ->> 'auto-maintenance' AS boolean)
    AND pt.partstrat = 'r';

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

-- Detach partitions
with config as (
    SELECT q.schema_name, q.table_name, d.key, d.value::text
    FROM dba.partition_configuration q
    JOIN json_each_text(configuration) d ON true
    ORDER BY 1, 2
),
detach_set as (
    select *
    from config
    where key = 'detach'
),
-- Only process tables partitioned on a date or timestamp
detach_date_set as MATERIALIZED (
SELECT
    schema_name,
    detach_set.table_name,
    LOWER(child.relname) as partition_name,
    detach_set.value::interval as detach_interval,
    (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*'))[2] as upper_boundary
FROM detach_set
JOIN pg_class parent ON parent.relname = detach_set.table_name
JOIN pg_inherits ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid   = parent.relnamespace
JOIN         (SELECT
            partrelid,
            unnest(partattrs) column_index
         FROM
             pg_partitioned_table) pt ON pt.partrelid = parent.oid
JOIN information_schema.columns col ON
        col.table_schema = detach_set.schema_name
        AND col.table_name = parent.relname
        AND ordinal_position = pt.column_index
    JOIN pg_catalog.pg_attribute a ON a.attrelid = parent.oid AND a.attname = col.column_name
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE
    (t.typname ~ 'timestamp' OR t.typname ~ 'date')
    AND LOWER(nmsp_parent.nspname)=LOWER(detach_set.schema_name)
    AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
    AND NOT LOWER(child.relname) ~ 'mammoth'
)
SELECT
    schema_name,
    detach_date_set.table_name,
    partition_name,
    dba.partition_detach_partition(schema_name, detach_date_set.table_name, partition_name) FROM detach_date_set
WHERE
   detach_date_set.upper_boundary::DATE < ( CURRENT_DATE - detach_interval )::DATE
ORDER BY schema_name, detach_date_set.table_name, upper_boundary::DATE ASC ;

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
    WHERE parent_relname = drop_detach_set.table_name
        AND detached_date <= current_date - GREATEST(drop_detach_set.value::interval, '4 days'::interval)
        AND schema = drop_detach_set.schema_name
) drop_table_set ON 1=1
where partition_relname is not null;