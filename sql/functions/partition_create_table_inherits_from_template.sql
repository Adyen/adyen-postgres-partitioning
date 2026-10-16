/*
    This function will create a new table and inherit the specifications
    from a template table. This is used as a way to create a new partition
    of a very large table.

    The final name of the table is constructed as follows:
    $(v_template_table)_$(v_suffix)

    PARAMETER           TYPE    DESCRIPTION
    v_schema            TEXT    schema the template table resides in
    v_template_table    TEXT    the template table
    v_suffix            TEXT    the suffix for the new table name

    Example: SELECT dba.partition_create_table_inherits_from_template('public', 'orders', '20220301_20220331');
    
*/
CREATE OR REPLACE FUNCTION dba.partition_create_table_inherits_from_template(v_schema TEXT, v_template_table TEXT, v_suffix TEXT)
                RETURNS BOOLEAN LANGUAGE plpgsql
                SET search_path = pg_catalog, dba, pg_temp
                AS $func$

                DECLARE
                    v_final_name    TEXT := v_template_table || '_' || v_suffix;
                    v_reloptions    TEXT;

                BEGIN
                    RAISE DEBUG 'Creating table % from template table % in schema %', v_template_table || '_' || v_suffix, v_template_table, v_schema;
                    EXECUTE format('CREATE TABLE %I.%I () INHERITS (%I.%I);', lower(v_schema), lower(v_final_name), lower(v_schema), lower(v_template_table));

                    -- Copy the table storage parameters to the new partition
                    EXECUTE format($sel$
                            SELECT array_to_string(reloptions, ',') from pg_class
                            WHERE relname = %L and relnamespace::regnamespace::text=%L;
                        $sel$, lower(v_template_table), lower(v_schema))
                        INTO v_reloptions;

                    IF v_reloptions IS NOT NULL THEN
                        EXECUTE format('ALTER TABLE %I.%I set (%s)', lower(v_schema), lower(v_final_name), v_reloptions);
                    END IF;

                    RETURN TRUE;
                END
                $func$;
                
