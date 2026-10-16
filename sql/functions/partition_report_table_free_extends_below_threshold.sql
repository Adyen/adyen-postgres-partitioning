/*
Creates a CSV file listing all partitioned tables that have fewer unused partitions than the
given threshold. The default threshold is 3.

    PARAMETER                   TYPE    DESCRIPTION
    v_free_extends_threshold    INT     the minimum required number of free partitions (default: 3)
    v_outfile                   TEXT    full path to the output CSV file

Example:
    SELECT dba.partition_report_table_free_extends_below_threshold();
    SELECT dba.partition_report_table_free_extends_below_threshold(2);
    SELECT dba.partition_report_table_free_extends_below_threshold(4, '/tmp/somefile.csv');
*/
CREATE OR REPLACE FUNCTION dba.partition_report_table_free_extends_below_threshold(v_free_extends_threshold integer DEFAULT 3, v_outfile text DEFAULT '/var/lib/pgsql/tmp/partition_report_table_free_extends_below_threshold.csv')
RETURNS VOID LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN

EXECUTE format($sql$
    COPY (
        WITH partitioned_tables AS (
            SELECT
                LOWER(relnamespace::regnamespace::text) AS schema,
                LOWER(par.relname) AS relname,
                dba.partition_calculate_free_partitions(relnamespace::regnamespace::text, par.relname) AS count
            FROM (
                SELECT partrelid, unnest(partattrs) column_index
                FROM pg_partitioned_table
                WHERE partstrat = 'r'
            ) pt
            JOIN pg_class par ON par.oid = pt.partrelid
        )
        SELECT * FROM partitioned_tables WHERE count < %L
    )
    TO %L CSV HEADER
$sql$, v_free_extends_threshold, v_outfile);

END
$func$;
