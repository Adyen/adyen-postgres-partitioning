/*
Extracts the timestamp from a UUIDv7. The first 48 bits of a UUIDv7 hold the number of milliseconds
since the Unix epoch. Same signature and result as uuid_v7_to_timestamptz from the pg_uuidv7 extension.

    PARAMETER    TYPE    DESCRIPTION
    v_uuid       UUID    the UUIDv7 to read the timestamp from

Example:
    SELECT dba.uuid_v7_to_timestamptz('018570bb-4a7d-7000-8000-000000000000'); -- returns 2023-01-02 04:26:40.637+00
*/

CREATE OR REPLACE FUNCTION dba.uuid_v7_to_timestamptz(v_uuid uuid)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE STRICT
SET search_path = pg_catalog, dba, pg_temp
AS $func$
    SELECT timestamptz 'epoch'
        + ('x' || lpad(substr(replace(v_uuid::text, '-', ''), 1, 12), 16, '0'))::bit(64)::bigint
        * interval '1 millisecond';
$func$;
