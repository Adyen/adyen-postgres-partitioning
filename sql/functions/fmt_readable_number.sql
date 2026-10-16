/*
Formats a bigint for human-readable output by inserting underscores every three digits.

    PARAMETER    TYPE      DESCRIPTION
    v_n          BIGINT    the number to format

Example:
    SELECT dba.fmt_readable_number(123400000000000000); -- returns '123_400_000_000_000_000'
*/

CREATE OR REPLACE FUNCTION dba.fmt_readable_number(v_n bigint)
RETURNS text
LANGUAGE sql
IMMUTABLE STRICT
SET search_path = pg_catalog, dba, pg_temp
AS $func$
    SELECT regexp_replace(v_n::text, '(\d)(?=(\d{3})+$)', '\1_', 'g');
$func$;
