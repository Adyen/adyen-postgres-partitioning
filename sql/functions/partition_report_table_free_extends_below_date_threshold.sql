/*
Creates a CSV file listing all partitioned tables partitioned on a date or timestamp column
where the start of the last partition is fewer than v_days_to_go days in the future.

    PARAMETER       TYPE    DESCRIPTION
    v_days_to_go    INT     the threshold in days (default: 7)
    v_outfile       TEXT    full path to the output CSV file

Example:
    SELECT dba.partition_report_table_free_extends_below_date_threshold();
    SELECT dba.partition_report_table_free_extends_below_date_threshold(2);
    SELECT dba.partition_report_table_free_extends_below_date_threshold(4, '/tmp/somefile.csv');
*/
CREATE OR REPLACE FUNCTION dba.partition_report_table_free_extends_below_date_threshold(v_days_to_go integer DEFAULT 7, v_outfile text DEFAULT '/var/lib/pgsql/tmp/partition_report_table_free_extends_below_date_threshold.csv')
RETURNS VOID LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
BEGIN

EXECUTE FORMAT($sql$
    COPY (
        WITH partitioned_tables AS (
            SELECT
                LOWER(relnamespace::regnamespace::text) AS schema,
                LOWER(c.relname) AS relname,
                (SELECT v_range[1] FROM dba.partition_get_last_partition_details(relnamespace::regnamespace::text, c.relname)) AS start
            FROM pg_partitioned_table pt
            JOIN pg_class c ON c.oid = pt.partrelid
            JOIN LATERAL dba.partition_get_partition_column_info(
                    c.relnamespace::regnamespace::text, c.relname) AS pci ON TRUE
            WHERE pt.partstrat = 'r'
              AND (pci.v_column_type ~ 'date' OR pci.v_column_type ~ 'timestamp')
        )
        SELECT schema, relname, (start::date - CURRENT_DATE) AS days_to_go
        FROM partitioned_tables
        WHERE (start::date - CURRENT_DATE) <= %L
    )
    TO %L CSV HEADER
$sql$, v_days_to_go, v_outfile);

END
$func$;
