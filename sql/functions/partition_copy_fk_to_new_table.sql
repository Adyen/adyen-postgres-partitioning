/*
This function will enable copy_fk_to_new_table
copy_fk_to_new_table has been designed to copy the foreign keys from one table to another.

This is used during the creation of a new partition to ensure that the partition
has the same foreign keys as the template table.

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    the schema the tempalte lives in
    v_template  TEXT    the template to take the foreign keys from
    v_new_table TEXT    the table to add the new foreign keys to
*/

CREATE OR REPLACE FUNCTION dba.partition_copy_fk_to_new_table(v_schema TEXT, v_template TEXT, v_new_table TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql AS $func$

DECLARE
    v_name TEXT;
    v_statement TEXT;
    
BEGIN
    FOR v_name, v_statement IN
    SELECT conname, pg_get_constraintdef(oid) as statement from pg_constraint where conrelid=(LOWER(v_schema || '.' || v_template))::regclass and contype='f'
    LOOP
        EXECUTE format('ALTER TABLE ONLY %s.%s ADD CONSTRAINT %s %s;', v_schema, v_new_table,
               regexp_replace(v_name, LOWER(v_template), LOWER(v_new_table), 'g'), v_statement);
    END LOOP;
    
    RETURN TRUE;
END
$func$;
