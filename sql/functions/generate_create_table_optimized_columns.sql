/*
Returns a CREATE TABLE statement for a target table using the column order optimized for
alignment padding. Column defaults, NOT NULL, collation, and storage settings are copied
from the source table.

    PARAMETER        TYPE    DESCRIPTION
    v_source_schema  TEXT    schema of the source table
    v_source_table   TEXT    source table name
    v_target_schema  TEXT    schema of the target table
    v_target_table   TEXT    target table name

Example:
    SELECT dba.generate_create_table_optimized_columns('public', 'orders_20240101_20240201', 'public', 'orders_20240201_20240301');
*/

CREATE OR REPLACE FUNCTION dba.generate_create_table_optimized_columns(v_source_schema TEXT, v_source_table TEXT, v_target_schema TEXT, v_target_table TEXT)
RETURNS TEXT LANGUAGE plpgsql
SET search_path = pg_catalog, dba, pg_temp
AS $func$
DECLARE
    v_columns               TEXT[];
    v_column_name           TEXT;
    v_column_definition     TEXT;
    v_column_definitions    TEXT := '';
    v_column_type           TEXT;
    v_not_null              BOOLEAN;
    v_default_expression    TEXT;
    v_attcollation          OID;
    v_typcollation          OID;
    v_collation_name        TEXT;
    v_collation_schema      TEXT;
    v_attstorage            "char";
    v_typstorage            "char";
    v_storage_clause        TEXT;
    v_attgenerated          "char";
BEGIN
    v_columns := dba.get_optimized_column_order(v_source_schema, v_source_table);

    FOREACH v_column_name IN ARRAY v_columns LOOP
        SELECT
            format_type(a.atttypid, a.atttypmod),
            a.attnotnull,
            pg_get_expr(ad.adbin, ad.adrelid),
            a.attcollation,
            t.typcollation,
            c.collname,
            n.nspname,
            a.attstorage,
            t.typstorage,
            a.attgenerated
        INTO
            v_column_type,
            v_not_null,
            v_default_expression,
            v_attcollation,
            v_typcollation,
            v_collation_name,
            v_collation_schema,
            v_attstorage,
            v_typstorage,
            v_attgenerated
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.collnamespace
        WHERE a.attrelid = format('%I.%I', lower(v_source_schema), lower(v_source_table))::regclass
            AND a.attname = v_column_name
            AND a.attnum > 0
            AND NOT a.attisdropped;

        v_column_definition := format('%I %s', v_column_name, v_column_type);

        IF v_collation_name IS NOT NULL AND v_attcollation <> v_typcollation THEN
            v_column_definition := v_column_definition || format(' COLLATE %I.%I', v_collation_schema, v_collation_name);
        END IF;

        -- pg_attrdef stores generated column expressions next to defaults. They must be emitted as
        -- GENERATED ALWAYS AS, otherwise a DEFAULT referencing another column is rejected and
        -- ATTACH PARTITION would reject the mismatching column definition.
        IF v_attgenerated IN ('s', 'v') THEN
            v_column_definition := v_column_definition || format(' GENERATED ALWAYS AS (%s) %s',
                v_default_expression, CASE v_attgenerated WHEN 's' THEN 'STORED' ELSE 'VIRTUAL' END);
        ELSIF v_default_expression IS NOT NULL THEN
            v_column_definition := v_column_definition || format(' DEFAULT %s', v_default_expression);
        END IF;

        IF v_not_null THEN
            v_column_definition := v_column_definition || ' NOT NULL';
        END IF;

        IF v_attstorage IS NOT NULL AND v_typstorage IS NOT NULL AND v_attstorage <> v_typstorage THEN
            v_storage_clause := CASE v_attstorage
                WHEN 'p' THEN 'PLAIN'
                WHEN 'm' THEN 'MAIN'
                WHEN 'x' THEN 'EXTENDED'
                WHEN 'e' THEN 'EXTERNAL'
                ELSE NULL
            END;
            IF v_storage_clause IS NOT NULL THEN
                v_column_definition := v_column_definition || format(' STORAGE %s', v_storage_clause);
            END IF;
        END IF;

        v_column_definitions := v_column_definitions
            || CASE WHEN v_column_definitions = '' THEN '' ELSE ', ' END
            || v_column_definition;
    END LOOP;

    RETURN format('CREATE TABLE %I.%I (%s)', v_target_schema, v_target_table, v_column_definitions);
END
$func$;
