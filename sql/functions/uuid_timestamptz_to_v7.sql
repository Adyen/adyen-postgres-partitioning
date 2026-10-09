/*
Builds a UUIDv7 for the given timestamp (millisecond precision). Same signature and result as
uuid_timestamptz_to_v7 from the pg_uuidv7 extension. Used to calculate the boundaries of partitions
on a UUIDv7 column.

    PARAMETER    TYPE           DESCRIPTION
    v_ts         TIMESTAMPTZ    the timestamp to encode
    v_zero       BOOLEAN        default false. When true all bits after the timestamp are zero (apart from the
                                version and variant bits), which gives the lowest UUIDv7 for that millisecond.
                                When false these bits are random.

Example:
    SELECT dba.uuid_timestamptz_to_v7('2023-01-02 04:26:40.637+00', true); -- returns 018570bb-4a7d-7000-8000-000000000000
*/

CREATE OR REPLACE FUNCTION dba.uuid_timestamptz_to_v7(v_ts timestamptz, v_zero boolean DEFAULT false)
RETURNS uuid
LANGUAGE sql
VOLATILE
SET search_path = pg_catalog, dba, pg_temp
AS $func$
    -- Bytes 1-6 hold the timestamp. In the random variant, setting bits 52 and 53 turns the
    -- version nibble of a v4 UUID (0100) into 0111; the v4 variant bits are already correct.
    SELECT encode(
        overlay(
            CASE WHEN v_zero
                THEN '\x00000000000070008000000000000000'::bytea
                ELSE set_bit(set_bit(uuid_send(gen_random_uuid()), 52, 1), 53, 1)
            END
            PLACING substring(int8send(floor(extract(epoch FROM v_ts) * 1000)::bigint) FROM 3)
            FROM 1 FOR 6),
        'hex')::uuid;
$func$;
