/*
Calculates the interval between the result of the provided query and a date (default: current date).

    PARAMETER    TYPE    DESCRIPTION
    v_query      TEXT    The query to run (return type must be date)
    v_date       DATE    The date to compare against (default current_date)

The query runs under the caller's search_path, so schema-qualify the objects it references.

Example:
    SELECT dba.calculate_query_date_interval('select creationdate from public.some_partition');
*/

CREATE OR REPLACE FUNCTION dba.calculate_query_date_interval(v_query text, v_date date default current_date)
RETURNS INTERVAL
LANGUAGE plpgsql
AS $func$
DECLARE
    v_test_date         date;
    v_interval          interval;
BEGIN
    EXECUTE v_query into v_test_date;
    raise debug 'given date: %, calculated date: %', v_date, v_test_date;
    v_interval := pg_catalog.age(v_date::timestamptz, v_test_date::timestamptz);
    RETURN v_interval;
END;
$func$;
