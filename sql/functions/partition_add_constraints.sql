/*
When a table is partitioned based an integer, but queries are using a date or timestamp column for selecting records we
can add a check constraint on this date/timestamp column to help the optimizer prune unrelevant partitions.

This function adds
 - A check constraint based on the current minimal value of the date/timestamp column as soon as there is at
   least one row in the partition.
 - A check constraint based on the current maximam value of the date/timestamp column as soon as there is a record matching
   the upper boundary of the partition (partition is full).

In order for a table to be selected by this function it must satisfy the following conditions
 - The table is partitioned based on an integer column
 - The check constraint has to be applied on a date or timestamp column

The function will create check constraints with names
- <child_partition_name>_<marker>_constraint_min
- <child_partition_name>_<marker>_constraint_max

For example
 - test_partition_1000_2000_date_constraint_min
 - test_partition_1000_2000_date_constraint_max

Example:
    SELECT dba.partition_add_constraints('public', 'test_partition', 'trip_date', 'trip_date');
*/

CREATE OR REPLACE FUNCTION dba.partition_add_constraints(v_schema TEXT, v_relname TEXT, v_marker TEXT, v_column_name TEXT)
RETURNS VOID LANGUAGE plpgsql AS $func$

DECLARE
    v_boundary_regex            CONSTANT TEXT := '.*\(\''?(.*?)\''?\).*\(\''?(.*?)\''?\).*';
    v_coltype                   TEXT;
    v_partition_column_name     TEXT;
    v_child                     RECORD;
    v_partition_is_full         BOOLEAN;
    v_time                      TIMESTAMP;
    v_row_ct                    BIGINT;
    v_is_correct_column_type    BOOLEAN;
    v_constraint_name           TEXT;

BEGIN
    -- Get the partitioning column type
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
            c.relname = '%I'
            and relnamespace::regnamespace::text='%I'
    $sel$, v_relname, v_schema)
    INTO v_partition_column_name, v_coltype;

    -- We only create date constraints on tables partitioned on an integer
    IF NOT (v_coltype ~ 'int') OR (v_coltype IS NULL) THEN
        RAISE EXCEPTION 'Table %.% is not partitioned on an integer column type', v_schema, v_relname;
    END IF;

    -- The column for the constraint must be of type timestamp or date
    EXECUTE FORMAT( $sql$
        SELECT data_type ~ 'timestamp' OR data_type ~ 'date'
        FROM information_schema.columns
        WHERE
            table_name = '%I'
            AND table_schema = '%I'
            AND column_name = '%s'
     $sql$, v_relname, v_schema, v_column_name)
    INTO v_is_correct_column_type;

    IF (NOT v_is_correct_column_type) OR (v_is_correct_column_type IS NULL) THEN
        RAISE EXCEPTION 'Column % of table %.% is not a date or a timestamp', v_column_name, v_schema, v_relname;
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
            AND child.relname !~ 'mammoth'
    LOOP

        -- If child has at least one row we need to have a min constraint
        EXECUTE FORMAT( $sql$ SELECT 1 FROM %I.%I limit 1 $sql$, v_schema, v_child.relname);
        GET DIAGNOSTICS v_row_ct = ROW_COUNT;

        IF v_row_ct >= 1 THEN
            -- Construct constraint name
            v_constraint_name := concat(v_child.relname, '_', v_marker, '_min');

            -- Check if min constraint already exists
            PERFORM 1
            FROM pg_catalog.pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE
                t.relname = v_child.relname
                AND t.relnamespace = v_schema::regnamespace
                AND c.contype = 'c'
                AND c.conname = v_constraint_name;

            IF NOT FOUND THEN
                -- Create constraint
                RAISE NOTICE 'Create new min constraint for partition %.%', v_schema, v_child.relname;

                -- select minimal value
                EXECUTE FORMAT($sql$ SELECT MIN(%s) FROM %I.%I  $sql$, v_column_name, v_schema, v_child.relname)
                INTO v_time;

                -- Create the constraint
                EXECUTE format($sql$ ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%s >= '%s') NOT VALID
                    $sql$, v_schema, v_child.relname, v_constraint_name, v_column_name, v_time);

                -- Mark constraint as validated in the catalog. This is within one transaction; save to do
                EXECUTE format($sql$
                    UPDATE pg_constraint pgc
                    SET convalidated = true
                    FROM pg_class c
                    WHERE
                        c.oid = pgc.conrelid
                        AND connamespace = '%I'::regnamespace::oid
                        AND c.relname = '%I'
                        AND conname = '%s'
                $sql$, v_schema, v_child.relname, v_constraint_name);

            END IF;
        END IF;

        -- Check if child partition is full: A record matching upper boundary (exclusive) of the partition exists
        EXECUTE format($sql$
            SELECT (MAX(%s)) = (SELECT %s - 1) FROM %I.%I
        $sql$,v_partition_column_name, v_child.upper, v_schema, v_child.relname)
        INTO v_partition_is_full;

        IF v_partition_is_full THEN
            -- Construct constraint name
            v_constraint_name := concat(v_child.relname, '_', v_marker, '_max');

            -- Check if max constraint already exists
            PERFORM 1
            FROM pg_catalog.pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE
                t.relname = v_child.relname
                AND t.relnamespace = v_schema::regnamespace
                AND c.contype = 'c'
                AND c.conname = v_constraint_name;

            IF NOT FOUND THEN
                -- Create constraint
                RAISE NOTICE 'Create new max constraint for partition %.%', v_schema, v_child.relname;

                -- max value for data/timestamp column
                EXECUTE FORMAT($sql$
                    SELECT MAX(%s) FROM %I.%I
                $sql$, v_column_name, v_schema, v_child.relname)
                INTO v_time;

                -- Add the constraint
                EXECUTE format($sql$
                    ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%s <= '%s') NOT VALID
                $sql$, v_schema, v_child.relname, v_constraint_name, v_column_name, v_time);

                -- Mark constraint as validated in the catalog. This is within one transaction; save to do
                EXECUTE format($sql$
                    UPDATE pg_constraint pgc
                    SET convalidated = true
                    FROM pg_class c
                    WHERE
                        c.oid = pgc.conrelid
                        AND connamespace = '%I'::regnamespace::oid
                        AND c.relname = '%I'
                        AND conname = '%s'
                $sql$, v_schema, v_child.relname, v_constraint_name);
            END IF;
        END IF;
    END LOOP;
END
$func$;
