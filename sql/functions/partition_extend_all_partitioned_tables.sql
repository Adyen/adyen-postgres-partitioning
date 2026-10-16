/*
Creates new partitions for all native range-partitioned tables that have
dba.partition_configuration entries with auto-maintenance enabled, until each has at least
three available unused partitions.

For tables partitioned on a date or timestamp the function creates new partitions until there
are three partitions whose starting date is later than today.

For tables partitioned on an integer the function creates new partitions until there are three
partitions whose lower boundary exceeds the current maximum value in the table.

Example:
    SELECT dba.partition_extend_all_partitioned_tables();
*/
CREATE OR REPLACE FUNCTION dba.partition_extend_all_partitioned_tables()
RETURNS BOOLEAN LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_table       RECORD;
    v_result      BOOLEAN;
    v_all_succeeded BOOLEAN := TRUE;
    v_err_code    TEXT;
    v_msg_text    TEXT;
    v_exc_context TEXT;
    v_msg_detail  TEXT;
    v_exc_hint    TEXT;
BEGIN

FOR v_table IN
    SELECT
        LOWER(par.relname) AS relname,
        LOWER(relnamespace::regnamespace::text) AS schema,
        GREATEST(3, CAST(configuration ->> 'nr' AS INT) + 2) AS required_partitions
    FROM pg_partitioned_table pt
    JOIN pg_class par ON par.oid = pt.partrelid
    JOIN dba.partition_configuration cfg ON
        LOWER(cfg.schema_name) = LOWER(relnamespace::regnamespace::text)
        AND LOWER(cfg.table_name) = LOWER(par.relname)
    WHERE CAST(configuration ->> 'auto-maintenance' AS boolean)
      AND pt.partstrat = 'r'
LOOP
    BEGIN
        EXECUTE format($sel$ SELECT dba.partition_add_up_to_nr_of_free_partitions(%L, %L, %L) $sel$,
                       v_table.schema, v_table.relname, v_table.required_partitions)
        INTO v_result;

        IF v_result IS FALSE THEN
            RAISE LOG 'Partition maintenance: Failed to extend table %.%',
                v_table.schema, v_table.relname;
            v_all_succeeded := FALSE;
        END IF;
    EXCEPTION
    WHEN sqlstate 'Z1001' THEN
        GET STACKED DIAGNOSTICS
          v_err_code = RETURNED_SQLSTATE,
          v_msg_text = MESSAGE_TEXT,
          v_exc_context = PG_EXCEPTION_CONTEXT;

        RAISE LOG 'Partition maintenance: Failed to extend table %.%
         ERROR CODE: % : %
         CONTEXT: %', v_table.schema, v_table.relname, v_err_code, v_msg_text, v_exc_context;

        v_all_succeeded := FALSE;

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
          v_err_code = RETURNED_SQLSTATE,
          v_msg_text = MESSAGE_TEXT,
          v_exc_context = PG_EXCEPTION_CONTEXT,
          v_msg_detail = PG_EXCEPTION_DETAIL,
          v_exc_hint = PG_EXCEPTION_HINT;

        RAISE LOG 'Partition maintenance: ERROR CODE: % MESSAGE TEXT: % CONTEXT: % DETAIL: % HINT: %',
            v_err_code, v_msg_text, v_exc_context, v_msg_detail, v_exc_hint;

        v_all_succeeded := FALSE;
    END;

END LOOP;

RETURN v_all_succeeded;

END
$func$;
