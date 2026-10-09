/*
This function calculates the number of unused partitions for a partitioned table. When the table has partitions in
multiple ranges, the minimal number of unused partitions over all the ranges is returned. If the identifier for a range
is provided, the number of unused partitions for this range is returned.

When the table is partitioned based on a date, timestamp or uuid the function calculates the number of partitions
where the starting date/timestamp of the partition is larger than the current date.

When the table is partitioned based on an integer the function calculates the amount of partitions
where the lower boundary of the partition is larger than the current maximum value from the table.

When the table is partitioned based on any other column type the function will return an error.

    PARAMETER           TYPE    DESCRIPTION
    v_schema            TEXT    schema location for the table
    v_relname           TEXT    the normal table name
    v_column_name       TEXT    the column name of the partitioned column
    v_coltype           TEXT    the type of the partitioned column
    v_range_identifier  TEXT    the identifier for the range. This must be an 'r' followed by a number.

Example:
    SELECT dba.partition_calculate_free_partitions('public','partitioned_table');
    SELECT dba.partition_calculate_free_partitions('public','partitioned_table', 'column_name', 'column_type');
    SELECT dba.partition_calculate_free_partitions('public','partitioned_table', 'column_name', 'column_type', 'r1');
*/
CREATE OR REPLACE FUNCTION dba.partition_calculate_free_partitions(v_schema TEXT, v_relname TEXT, v_column_name TEXT DEFAULT NULL, v_coltype TEXT DEFAULT NULL, v_range_identifier TEXT DEFAULT NULL)
RETURNS INT LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_additional_partitions INT;
    v_is_partitioned        BOOLEAN;
    v_boundary_regex        CONSTANT TEXT := '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*';
    v_is_range              BOOLEAN;
    v_range                 TEXT;
    v_range_count           INT;
BEGIN

    -- Normalize the identifiers so names with uppercase letters are matched case-insensitively.
    v_schema := LOWER(v_schema);
    v_relname := LOWER(v_relname);
    v_coltype := LOWER(v_coltype);
    v_range_identifier := LOWER(v_range_identifier);

    -- Do a check if the table is actually partitioned
    EXECUTE format($sel$
        SELECT count(*) > 0
        FROM pg_partitioned_table pt
        JOIN pg_class par on par.oid = pt.partrelid
        WHERE
            relnamespace::regnamespace::text = quote_ident(%L)
            AND par.relname = %L
    $sel$, v_schema, v_relname)
    INTO v_is_partitioned;

    IF NOT v_is_partitioned THEN
        RAISE EXCEPTION 'Table % is not a partitioned table.', v_schema || '.' || v_relname USING ERRCODE='Z1002';
    END IF;

    IF v_coltype IS NULL OR v_column_name IS NULL THEN
        SELECT pci.v_column_name, pci.v_column_type
        INTO v_column_name, v_coltype
        FROM dba.partition_get_partition_column_info(v_schema, v_relname) AS pci;
    END IF;

    IF v_column_name IS NOT NULL THEN
        SELECT a.attname
        INTO v_column_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE LOWER(n.nspname) = v_schema
          AND LOWER(c.relname) = v_relname
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND (a.attname = v_column_name OR LOWER(a.attname) = LOWER(v_column_name))
        ORDER BY (a.attname = v_column_name) DESC
        LIMIT 1;
    END IF;

    -- It is a range when the v_range_identifier is NOT NULL and not empty string
    v_is_range := (v_range_identifier <> '') IS TRUE;

    RAISE DEBUG 'Working on an interval: %', v_is_range;

    IF v_is_range AND NOT v_coltype ~ 'int' THEN
        RAISE EXCEPTION 'Table % has multiple ranges, but is the partition column is not a integer', v_schema || '.' || v_relname USING ERRCODE='Z1003';
    END IF;

    IF v_is_range THEN
        RAISE DEBUG 'Current interval: %', v_range_identifier;
    END IF;

    -- If the table has multiple ranges, we calculate the number of free partitions per range and return the smallest number.
    -- When the table is not partitioned over multiple ranges this block is skipped.
    <<range_block>>
    BEGIN
        -- We only check for multiple ranges if no range_identifier is provided.
        IF NOT v_is_range THEN
            -- We can have multiple ranges, or the table is not partitioned in multiple ranges
            FOR v_range IN
                EXECUTE format($sel$
                    SELECT  (regexp_match(child.relname, %L || '_(r\d+)_.*'))[1]
                    FROM pg_partitioned_table pt
                    JOIN pg_class parent on pt.partrelid = parent.oid
                    JOIN pg_inherits i on pt.partrelid = i.inhparent
                    JOIN pg_class child on i.inhrelid = child.oid
                    WHERE parent.relname = %L
                        AND parent.relnamespace::regnamespace::text=quote_ident(%L)
                        AND child.relname like %L || '\_r%%\_%%'
                    GROUP by (regexp_match(child.relname, %L || '_(r\d+)_.*'))[1]
                $sel$, v_relname, v_relname, v_schema, v_relname, v_relname)

            LOOP

                -- If v_range is empty, the table is not partitioned in multiple ranges. Exit this block and continue the default calculation.
                EXIT range_block WHEN v_range IS NULL;

                -- Recursively calculate the number of free partitions per range
                EXECUTE format($sel$  select dba.partition_calculate_free_partitions(%L, %L, %L, %L, %L) $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range) INTO v_range_count;
                RAISE DEBUG 'Number of partitions for interval % of table %: %', v_range, v_relname, v_range_count;

                -- We keep the result if this is the first range we calculated, or if the result is smaller than the current value.
                IF v_additional_partitions IS NULL OR v_range_count < v_additional_partitions THEN
                    v_additional_partitions := v_range_count;

                    RAISE DEBUG 'Minimal available partitions for an interval %', v_additional_partitions;
                END IF;
            END LOOP;

            IF v_additional_partitions IS NOT NULL THEN
                -- Return the smallest value for all partitions.
                RAISE DEBUG 'Returning minimal number of partitions for interval';
                RETURN v_additional_partitions;
            END IF;

            RAISE DEBUG 'No interval calculation';
        END IF;
    END range_block;

    -- At this point we can be in one of two situations
    --  - The table is not partitioned into multiple ranges
    --  - We are calculating the number of free partitions for a given range

    -- The calculation is equal for both situations. Lets calculate.
    CASE
        WHEN  v_coltype ~ 'int' THEN
            -- Count the number of unused partitions
            -- An unused partition must have a higher lower boundary than the current maximum value in the partition column

            EXECUTE format($sel$
                -- The boundaries of the latest available partition
                with partitions as (
                    SELECT
                        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::bigint as lower,
                        (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[2]::bigint as upper
                    FROM pg_inherits
                        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                        JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                    WHERE
                        LOWER(nmsp_child.nspname)=LOWER(%L)
                        AND LOWER(parent.relname)=LOWER(%L)
                        AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
                        AND (NOT %L::boolean or child.relname like %L || '\_' || %L || '\_%%')
                    ORDER BY 1
                )
                select count(*) from partitions where lower > (select coalesce(max(%I),0) from %I.%I where %I < (select max(upper) from partitions))
            $sel$,
            v_boundary_regex, v_boundary_regex, v_schema, v_relname, v_is_range, v_relname, v_range_identifier,
            v_column_name, v_schema, v_relname, v_column_name
            )
            INTO v_additional_partitions;

            RAISE DEBUG 'Number of additional integer range based partitions: %',  v_additional_partitions;

        WHEN v_coltype ~ 'date' OR v_coltype ~ 'timestamp' THEN
            -- Count the number of partitions starting after today
            EXECUTE format($sel$
                WITH partitions AS MATERIALIZED (
                    SELECT
                        child.oid,
                        child.relpartbound
                    FROM pg_inherits
                        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                        JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                    WHERE
                        LOWER(nmsp_child.nspname)=LOWER(%L)
                        AND LOWER(parent.relname)=LOWER(%L)
                        AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
                )
                SELECT
                    COUNT(*)
                FROM
                    partitions p
                WHERE
                    (regexp_match(pg_catalog.pg_get_expr(p.relpartbound, p.oid), %L))[1]::date > current_date
            $sel$, v_schema, v_relname, v_boundary_regex)
            INTO v_additional_partitions;

            RAISE DEBUG 'Number of additional date range based partitions: %',  v_additional_partitions;
        WHEN  v_coltype ~ 'uuid' THEN
            -- Count the number of partitions starting after today. In order to do so we have to convert the uuid
            -- into a date.
            EXECUTE format($sel$
                WITH partitions AS MATERIALIZED (
                    SELECT
                        child.oid,
                        child.relpartbound
                    FROM pg_inherits
                        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                        JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                    WHERE
                        LOWER(nmsp_child.nspname)=LOWER(%L)
                        AND LOWER(parent.relname)=LOWER(%L)
                        AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
                )
                SELECT
                    COUNT(*)
                FROM
                    partitions p
                WHERE
                    dba.uuid_v7_to_timestamptz((regexp_match(pg_catalog.pg_get_expr(p.relpartbound, p.oid), %L))[1]::uuid)::date > current_date
            $sel$, v_schema, v_relname, v_boundary_regex)
            INTO v_additional_partitions;

            RAISE DEBUG 'Number of additional uuid range based partitions: %',  v_additional_partitions;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype USING ERRCODE='Z1001';
    END CASE;

    RETURN v_additional_partitions;
END
$func$;
