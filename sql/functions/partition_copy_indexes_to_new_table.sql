/*
This function will duplicate primary key and other indexes
from template table

    PARAMETER   TYPE    DESCRIPTION
    v_schema    TEXT    schema the template table resides in
    v_template  TEXT    template to take the indexes from
    v_new_table TEXT    table to apply indexes to
    v_randomize BOOLEAN option to randomize index name, default TRUE

    Example:
    SELECT dba.partition_copy_indexes_to_new_table('public', 'test_partition_inh', 'test_partition_inh_20220301_20220331');
    SELECT dba.partition_copy_indexes_to_new_table('public', 'test_partition_inh', 'test_partition_inh_20220301_20220331', false);
*/

CREATE OR REPLACE FUNCTION dba.partition_copy_indexes_to_new_table(v_schema TEXT, v_template TEXT, v_new_table TEXT, v_randomize BOOLEAN DEFAULT TRUE)
RETURNS BOOLEAN LANGUAGE plpgsql AS $func$

DECLARE
    v_row                       RECORD;
    v_final_creation_statement  TEXT;
    v_newindexname              TEXT;
    
BEGIN
    RAISE DEBUG 'Copying indexes FROM % TO %', v_schema ||'.'|| v_template, v_schema ||'.'|| v_new_table USING ERRCODE='ADYEN';
    FOR v_row IN
        WITH indexes AS (
          SELECT indexdef, indexname FROM pg_indexes
          WHERE schemaname = LOWER(v_schema)
            AND tablename = LOWER(v_template)
        )
        SELECT indexdef, indisprimary, indexname FROM indexes
        JOIN pg_class ON pg_class.relname = indexes.indexname
        JOIN pg_index ON pg_class.oid = pg_index.indexrelid
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = LOWER(v_schema)
    LOOP
        IF v_row.indisprimary IS TRUE THEN
            IF length(v_new_table) > 58 THEN
                v_newindexname := substring(LOWER(v_new_table), 1, 58);
            ELSE
                v_newindexname := LOWER(v_new_table);
            END IF;
            v_final_creation_statement := regexp_replace(v_row.indexdef,'.*btree','ALTER TABLE ONLY ' || LOWER(v_schema) || '.' || LOWER(v_new_table) || ' ADD CONSTRAINT ' || LOWER(v_newindexname) || '_pkey' || ' PRIMARY KEY' ) || ';';
            IF (
                SELECT indisprimary FROM pg_indexes
                JOIN pg_class ON pg_class.relname = pg_indexes.indexname
                JOIN pg_index ON pg_class.oid = pg_index.indexrelid
                JOIN pg_namespace ON (pg_namespace.oid = pg_class.relnamespace AND pg_namespace.nspname = LOWER(v_schema))
                WHERE schemaname = LOWER(v_schema) AND tablename = LOWER(v_new_table) AND indisprimary = TRUE
            ) IS TRUE THEN
                RAISE NOTICE 'PRIMARY KEY exists, skipping' USING ERRCODE='ADYEN';
            ELSE
                RAISE DEBUG 'Creating primary key using: %', v_final_creation_statement USING ERRCODE='ADYEN';
                EXECUTE format(v_final_creation_statement);
            END IF;
        ELSE
            IF LOWER(v_row.indexname) ~ LOWER(v_template) THEN
                v_newindexname := regexp_replace(v_row.indexname, LOWER(v_template), LOWER(v_new_table));
            ELSE
                -- this is forced because we need table name in indexname
                -- otherwise it can cause further break down
                IF v_randomize THEN
                    v_newindexname := substring(LOWER(v_new_table), 1, 56) || '_idx_' || trunc(random() * 98 + 1);
                ELSE
                    v_newindexname := substring(LOWER(v_new_table), 1, 59) || '_idx';
                END IF;
            END IF;
            IF length(v_newindexname) > 63 THEN
                IF v_randomize THEN
                    v_newindexname := substring(LOWER(v_newindexname), 1, 56) || '_idx' || trunc(random() * 98 + 1);
                ELSE
                    v_newindexname := substring(LOWER(v_newindexname), 1, 59) || '_idx';
                END IF;
            END IF;
            v_final_creation_statement := regexp_replace(v_row.indexdef, LOWER(v_schema) || '.' || LOWER(v_template), LOWER(v_schema) || '.' || LOWER(v_new_table));
            v_final_creation_statement := regexp_replace(v_final_creation_statement, LOWER(v_row.indexname), LOWER(v_newindexname));
            RAISE DEBUG 'Creating an index using: %', v_final_creation_statement USING ERRCODE='ADYEN';
            EXECUTE format(v_final_creation_statement);
        END IF;
    END LOOP;

    RETURN TRUE;
END
$func$;
