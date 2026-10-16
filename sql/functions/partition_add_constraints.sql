/*
When a table is partitioned based an integer, but there are other queries using date, timestamp or bigint columns for selecting records we
can add a check constraint on this date/timestamp/bigint column to help the optimizer prune irrelevant partitions.

This function adds
 - A check constraint based on the current minimal value of the date/timestamp/bigint column as soon as there is at
   least one row in the partition.
 - A check constraint based on the current maximum value of the date/timestamp/bigint column as soon as there is a record matching
   the upper boundary of the partition (partition is full).

In order for a table to be selected by this function it must satisfy the following conditions
 - The table is partitioned based on an integer column
 - The check constraint has to be applied on a date/timestamp/bigint column
 - The parent table has a comment including the string '<<<marker>_constraint: <column>>>'. The column must be the column
   name of the column to create constraints for.

   For example <<date_constraint: order_date>>

The function will create check constraints with names
- <child_partition_name>_<marker>_constraint_min
- <child_partition_name>_<marker>_constraint_max

For example
 - orders_1000_2000_date_constraint_min
 - orders_1000_2000_date_constraint_max

Example:
    SELECT dba.partition_add_constraints(v_schema=>'public', v_relname=>'orders', v_marker=>'order_id', v_column_name=>'order_id');
The function will create check constraints with names
 - orders_1000_2000_order_id_constraint_min
 - orders_1000_2000_order_id_constraint_max
*/
CREATE OR REPLACE FUNCTION dba.partition_add_constraints(v_schema TEXT, v_relname TEXT, v_marker TEXT, v_column_name TEXT)
RETURNS VOID LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_boundary_regex            CONSTANT TEXT := '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*';
    v_coltype                   TEXT;
    v_partition_column_name     TEXT;
    v_child                     RECORD;
    v_partition_is_full         BOOLEAN;
    v_constraint_boundary       TEXT;
    v_row_ct                    BIGINT;
    v_is_correct_column_type    BOOLEAN;
    v_constraint_name           TEXT;
BEGIN

    v_schema := LOWER(v_schema);
    v_relname := LOWER(v_relname);
    v_column_name := LOWER(v_column_name);

    -- We do this only for tables partitioned on an integer column
    SELECT pci.v_column_name, pci.v_column_type
    INTO v_partition_column_name, v_coltype
    FROM dba.partition_get_partition_column_info(v_schema, v_relname) AS pci;

    IF NOT (v_coltype ~ 'int') OR (v_coltype IS NULL) THEN
        RAISE EXCEPTION 'Table %.% is not partitioned on an integer column type', v_schema, v_relname;
    END IF;

    -- The column for the constraint must be of type timestamp, date or bigint
    EXECUTE FORMAT( $sql$ SELECT data_type ~ 'timestamp' OR data_type ~ 'date' OR data_type ~ 'int'
                FROM information_schema.columns
                WHERE
                    LOWER(table_name) = LOWER(%L)
                    AND LOWER(table_schema) = LOWER(%L)
                    AND LOWER(column_name) = LOWER(%L)
             $sql$, v_relname, v_schema, v_column_name)
    INTO v_is_correct_column_type;

    IF (NOT v_is_correct_column_type) OR (v_is_correct_column_type IS NULL) THEN
        RAISE EXCEPTION 'Column % of table %.% is not a date, timestamp or bigint', v_column_name, v_schema, v_relname;
    END IF;

    -- Loop over all children
    FOR v_child IN
        SELECT
            child.relname,
            (regexp_match(pg_catalog.pg_get_expr(child.relpartbound, child.oid), v_boundary_regex))[2]::bigint as upper
        FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE
            LOWER(nmsp_child.nspname)=LOWER(v_schema)
            AND LOWER(parent.relname)=LOWER(v_relname)
            AND pg_catalog.pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
            AND NOT LOWER(child.relname) ~ 'mammoth'
    LOOP

        -- If child has at least one row we need to have a min constraint
        EXECUTE FORMAT( $sql$ SELECT 1 FROM %I.%I limit 1 $sql$, v_schema, v_child.relname);
        GET DIAGNOSTICS v_row_ct = ROW_COUNT;

        IF v_row_ct = 1 THEN
            -- Construct constraint name
            v_constraint_name := concat(v_child.relname, '_', v_marker, '_min');

            -- Check if min constraint already exists
            perform 1
            FROM pg_catalog.pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE
                t.relname = v_child.relname
                AND LOWER(t.relnamespace::regnamespace::text) = v_schema
                AND c.contype = 'c'
                AND c.conname = v_constraint_name;

            IF NOT FOUND THEN
                -- Create constraint
                RAISE LOG 'Partition maintenance: Create new min constraint for partition %.%', v_schema, v_child.relname;

                -- select minimal value
                EXECUTE FORMAT($sql$ SELECT MIN(%I) FROM %I.%I  $sql$, v_column_name, v_schema, v_child.relname)
                INTO v_constraint_boundary;

                -- Create the constraint
                EXECUTE format($sql$ ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%I >= %L) NOT VALID
                    $sql$, v_schema, v_child.relname, v_constraint_name, v_column_name, v_constraint_boundary);

                -- Mark constraint as validated in the catalog. This is within one transaction; save to do
                EXECUTE format($sql$ UPDATE pg_constraint pgc SET convalidated = true FROM pg_class c
                    WHERE
                        c.oid = pgc.conrelid
                        AND LOWER(connamespace::regnamespace::text) = %L
                        AND LOWER(c.relname) = LOWER(%L)
                        AND conname = %L
                    $sql$, v_schema, v_child.relname, v_constraint_name);

            END IF;
        END IF;

        -- Check if child partition is full: A record matching upper boundary (exclusive) of the partition exists
        EXECUTE format($sql$ SELECT (MAX(%I)) = (SELECT %s - 1) FROM %I.%I $sql$
            , v_partition_column_name, v_child.upper, v_schema, v_child.relname)
        INTO v_partition_is_full;

        IF v_partition_is_full THEN
            -- Construct constraint name
            v_constraint_name := concat(v_child.relname, '_', v_marker, '_max');

            -- Check if max constraint already exists
            perform 1
            FROM pg_catalog.pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE
                t.relname = v_child.relname
                AND LOWER(t.relnamespace::regnamespace::text) = v_schema
                AND c.contype = 'c'
                AND c.conname = v_constraint_name;

            IF NOT FOUND THEN
                -- Create constraint
                RAISE LOG 'Partition maintenance: Create new max constraint for partition %.%', v_schema, v_child.relname;

                -- max value for data/timestamp column
                EXECUTE FORMAT($sql$ SELECT MAX(%I) FROM %I.%I  $sql$, v_column_name, v_schema, v_child.relname)
                INTO v_constraint_boundary;

                -- Add the constraint
                EXECUTE format($sql$ ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%I <= %L) NOT VALID
                    $sql$, v_schema, v_child.relname, v_constraint_name, v_column_name, v_constraint_boundary);

                -- Mark constraint as validated in the catalog. This is within one transaction; save to do
                EXECUTE format($sql$ UPDATE pg_constraint pgc SET convalidated = true FROM pg_class c
                    WHERE
                        c.oid = pgc.conrelid
                        AND LOWER(connamespace::regnamespace::text) = %L
                        AND LOWER(c.relname) = LOWER(%L)
                        AND conname = %L
                    $sql$, v_schema, v_child.relname, v_constraint_name);
            END IF;
        END IF;
    END LOOP;

END
$func$;
