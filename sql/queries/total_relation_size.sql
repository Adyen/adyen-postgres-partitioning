-- This query calculates the total disk space used by the specified table, including all indexes and TOAST data
select
    pg_size_pretty(sum(pg_total_relation_size(inhrelid)))
from pg_inherits 
where inhparent = '<table_name>'::regclass::oid;

CREATE OR REPLACE FUNCTION dba.total_partitioned_relation_size_pretty(rel regclass)
RETURNS TEXT
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN
    return (select
        pg_size_pretty(sum(pg_total_relation_size(inhrelid)))
    from pg_inherits
    where inhparent = rel);
END;
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dba.total_partitioned_relation_size(rel regclass)
RETURNS BIGINT
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN
    return (select
        sum(pg_total_relation_size(inhrelid))
    from pg_inherits
    where inhparent = rel);
END;
$func$ LANGUAGE plpgsql;
