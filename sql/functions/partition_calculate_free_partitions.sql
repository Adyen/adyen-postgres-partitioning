/*
This function calculates the number of unused partitions for a partitioned table. When the table has partitions in
multiple ranges, the minimal number of unused partitions over all the ranges is returned. If the identifier for a range
is provided, the number of unused partitions for this range is returned.

When the table is partitioned based on a date or timestamp the function calculates the amount of partitions
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
RETURNS INT LANGUAGE plpgsql AS $func$

DECLARE
    v_additional_partitions INT;
    v_is_partitioned        BOOLEAN;
    v_boundary_regex        CONSTANT TEXT := '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*';
    v_is_range              BOOLEAN;
    v_range                 TEXT;
    v_range_count           INT;

BEGIN
    -- Do a check if the table is actually partitioned
    EXECUTE format($sel$
        SELECT count(*) > 0
        FROM pg_partitioned_table pt
        JOIN pg_class par on par.oid = pt.partrelid
        WHERE
            relnamespace::regnamespace::text = '%I'
            AND par.relname = '%I'
    $sel$, v_schema, v_relname)
    INTO v_is_partitioned;

    IF NOT v_is_partitioned THEN
        RAISE EXCEPTION 'Table % is not a partitioned table.', v_schema || '.' || v_relname USING ERRCODE='ADYEN';
    END IF;

    IF v_coltype IS NULL OR v_column_name IS NULL THEN
        EXECUTE format($sel$
            SELECT
                LOWER(col.column_name),
                t.typname
            FROM
                (SELECT
                    partrelid,
                    unnest(partattrs) column_index
                 FROM
                     pg_partitioned_table) pt
            JOIN pg_class c on c.oid = pt.partrelid
            JOIN information_schema.columns col ON
                col.table_schema = c.relnamespace::regnamespace::text
                AND col.table_name = c.relname
                AND ordinal_position = pt.column_index
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = col.column_name
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            WHERE
                LOWER(c.relname) = LOWER('%I')
                and LOWER(relnamespace::regnamespace::text) = LOWER('%I')
        $sel$, v_relname, v_schema)
        INTO v_column_name, v_coltype;
    END IF;

    -- It is a range when the v_range_identifier is NOT NULL and not empty string
    v_is_range := (v_range_identifier <> '') IS TRUE;

    IF v_is_range AND NOT v_coltype ~ 'int' THEN
        RAISE EXCEPTION 'Table % has multiple ranges, but is the partition column is not a integer', v_schema || '.' || v_relname USING ERRCODE='ADYEN';
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
                    SELECT  (regexp_match(child.relname, '%I_(r\d+)_.*'))[1]
                    FROM pg_partitioned_table pt
                    JOIN pg_class parent on pt.partrelid = parent.oid
                    JOIN pg_inherits i on pt.partrelid = i.inhparent
                    JOIN pg_class child on i.inhrelid = child.oid
                    WHERE parent.relname = '%I'
                        AND parent.relnamespace::regnamespace::text='%I'
                    GROUP by (regexp_match(child.relname, '%I_(r\d+)_.*'))[1]
                $sel$, v_relname, v_relname, v_schema, v_relname)

            LOOP

                -- If v_range is empty, the table is not partitioned in multiple ranges. Exit this block and continue the default calculation.
                EXIT range_block WHEN v_range IS NULL;

                -- Recursively calculate the number of free partitions per range
                EXECUTE format($sel$  select dba.partition_calculate_free_partitions('%I', '%I', '%I', '%I', %L) $sel$, v_schema, v_relname, v_column_name, v_coltype, v_range) INTO v_range_count;

                -- We keep the result if this is the first range we calculated, or if the result is smaller than the current value.
                IF v_additional_partitions IS NULL OR v_range_count < v_additional_partitions THEN
                    v_additional_partitions := v_range_count;
                END IF;

            END LOOP;

            -- Return the smallest value for all partitions.
            RETURN v_additional_partitions;
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
                        LOWER(nmsp_child.nspname)=LOWER('%I')
                        AND LOWER(parent.relname)=LOWER('%I')
                        AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
                        AND (NOT '%I' or child.relname like '%I_' || %L || '_%%')
                    ORDER BY 1
                )
                select count(*) from partitions where lower > (select coalesce(max(%I),0) from %s where %I < (select max(upper) from partitions))
            $sel$,
            v_boundary_regex, v_boundary_regex, v_schema, v_relname, v_is_range, v_relname, v_range_identifier,
            v_column_name, v_schema || '.' || v_relname, v_column_name
            )
            INTO v_additional_partitions;

        WHEN v_coltype ~ 'date' OR v_coltype ~ 'timestamp' THEN
            -- Count the number of partitions starting after today
            EXECUTE format($sel$
                SELECT
                    count(*)
                FROM pg_inherits
                    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                WHERE
                    LOWER(nmsp_child.nspname)=LOWER('%I')
                    AND LOWER(parent.relname)=LOWER('%I')
                    AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
                    AND (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), %L))[1]::date > current_date
            $sel$, v_schema, v_relname, v_boundary_regex)
            INTO v_additional_partitions;
        ELSE
            RAISE EXCEPTION 'Data type % IS NOT SUPPORTED.', v_coltype USING ERRCODE='ADYEN';
    END CASE;

    RETURN v_additional_partitions;
END
$func$;
