/*
Converting traditional/normal table to a partitioned table with native or inheritance partitioning.
Native partitioning supports ONLY by RANGE.

The function will rename original table to $TABLE_mammoth, create an empty table
called $TABLE and put it as main table, create another partition $TABLE_$v_endkey$v_interval

From: test_partition
to:
    - test_partition (parent table)
      - test_partition_mammoth
      - test_partition_20220401_20220430
    - test_partition_template (if the column is not in PK/unique index)

    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the normal table name
    v_keycolumn     TEXT    column name which the table will be partitioned based on
    v_startkey      TEXT    starting value for the the column in the original table;
                            supports date & timestamp(tz) in YYYY-MM-DD format, and integers
    v_endkey        TEXT    LAST VALUE for inheritance, NEW VALUE for native *)
    v_interval      TEXT    length for the new partition table, e.g.: 1 month, 1 week, 1000000000, and so on
    v_type          TEXT    type of partitioning: native or inheritance
    v_nopk          BOOLEAN set to true if the partition column is not in primary key or unique index;
                            discouraged, unless application can ensure the data validity
    v_move_trg      BOOLEAN set to true if you want to move triggers to newly partitioned table

Example:
    SELECT dba.partition_table('public','test_partition','id','1','999999','1000','inheritance');
    SELECT dba.partition_table('public','test_partition','id','1','1000000','1000','native');
    SELECT dba.partition_table('public','test_partition_date','trip_date','1970-01-01','2023-01-01','1 month','native',TRUE,TRUE);

Limitations:
    - In inheritance based partitioning, if the table is being referenced by other tables,
      the function will not work.
    - In native partitioning, referenced tables are fine. But, YOU HAVE TO RUN VALIDATE CONSTRAINT;
      we set them as not valid to make this function executed faster no matter the table size

Caveats:
    Index names from the original $TABLE are not carried to any new tables, instead the names will follow
    postgres design, e.g.: $TABLE_col1_col2_col2_idx and so on

Notes:
    *) Postgres native partitioning where the last value is exclusive
*/
CREATE OR REPLACE FUNCTION dba.partition_table(v_schemaname TEXT, v_tablename TEXT, v_keycolumn TEXT, v_startkey TEXT, v_endkey TEXT, v_interval TEXT, v_type TEXT, v_nopk BOOLEAN DEFAULT FALSE, v_move_trg BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN LANGUAGE plpgsql AS $func$
BEGIN
    RAISE DEBUG 'Converting table % using % partitioning method',
        v_schemaname || '.' || v_tablename, v_type USING ERRCODE='ADYEN';
    IF LOWER(v_type) = 'native' THEN
        PERFORM dba.partition_native(v_schemaname, v_tablename, v_keycolumn, v_startkey, v_endkey, v_interval, v_nopk, v_move_trg);
    ELSIF LOWER(v_type) = 'inheritance' THEN
        PERFORM dba.partition_inheritance(v_schemaname, v_tablename, v_keycolumn, v_startkey, v_endkey, v_interval);
    ELSE
        RAISE EXCEPTION '% IS NOT SUPPORTED.', v_type USING HINT = 'ADYEN: supported types are native and inheritance';
    END IF;
    RETURN TRUE;
END
$func$;

