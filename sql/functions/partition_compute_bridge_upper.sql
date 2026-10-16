/*
Computes the bridge partition upper bound by snapping v_switch_boundary up to the next
grid boundary defined by v_grid_anchor and v_grid_width. If the natural bridge would be
smaller than 1/3 of one grid slot, extends by one additional slot.

    PARAMETER           TYPE    DESCRIPTION
    v_switch_boundary   BIGINT  exclusive upper bound of the mammoth partition
    v_grid_anchor       BIGINT  any known grid-aligned boundary from the reference table
    v_grid_width        BIGINT  partition size on the reference table

Example:
    SELECT dba.partition_compute_bridge_upper(15700, 20000, 10000);
*/
CREATE OR REPLACE FUNCTION dba.partition_compute_bridge_upper(v_switch_boundary bigint, v_grid_anchor bigint, v_grid_width bigint)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_aligned_lower bigint;
    v_bridge_gap    bigint;
BEGIN
    IF v_grid_width <= 0 THEN
        RAISE EXCEPTION 'grid_width must be positive, got %', v_grid_width;
    END IF;

    v_aligned_lower := v_grid_anchor
                     + ceil((v_switch_boundary - v_grid_anchor)::numeric / v_grid_width)::bigint
                       * v_grid_width;

    v_bridge_gap := v_aligned_lower - v_switch_boundary;

    IF v_bridge_gap < v_grid_width / 3 THEN
        RAISE LOG 'bridge_gap (% (%)) < P/3 (% (%)) — skipping one grid slot',
                  v_bridge_gap,     dba.fmt_readable_number(v_bridge_gap),
                  v_grid_width / 3, dba.fmt_readable_number(v_grid_width / 3);
        RETURN v_aligned_lower + v_grid_width;
    END IF;

    RETURN v_aligned_lower;
END;
$func$;
