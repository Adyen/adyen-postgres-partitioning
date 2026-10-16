/*
This function generates a set of statements to create concurrent indexes on all partitions of a partitioned tables and a
create index statement for the parent table. It does not execute these statements.

DEPRECATED: if a <table>_template table exists, a create index statement for it is returned as well. Template tables
are no longer created by this framework and this behavior is only kept for backward compatibility. It will be removed
in a future version.
When the table is not partitioned a statement to create the index concurrently is returned.

The function tries to find a unique index name in the form of <table_name>_<columns>[1-9]_idx. If no unique name
can be found, the name with a nine in it will we returned. Executing this statement will fail with a duplicate index error.

When a unique index has to be created, we only add this index on the  parent table when the partition column is included
in the index. Otherwise we would get an error. 

This function Uses dba.find_matching_index_by_definition() to check if an equivalent index already exists on the parent or 
partitions based on canonical definition. Ensures duplicate indexes are not created and only missing ones are generated.

    PARAMETER       TYPE    DESCRIPTION
    v_schema        TEXT    schema location for the table
    v_tablename     TEXT    the parent table name
    v_columns       TEXT[]  the columns to create the index on including the names the operator class parameters such as desc, nulls first, nulls distinct
    v_include_list  TEXT[]  the columns to be in the include list of the index
    v_method        TEXT    the name of the index method, default btree. Possible other values: hash, gist, spgist, gin, brin
    v_is_unique     BOOLEAN default false. Indicate the index has to be unique
    v_condition     TEXT    default NULL. A WHERE clause for conditional/partial indexes 
    v_create_parent_index   BOOLEAN     default true. If false, the index on the parent table (which is non-concurrent) is skipped.

Example:
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','someTable', ARRAY['column_1', 'column_2']);
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','someTable', ARRAY['lower(column_1)', 'column_2 desc nulls first'], 'gin');
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','someTable', ARRAY['lower(column_1)', 'column_2 desc nulls first'], 'gin', true);
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','someTable', ARRAY['column_1', 'column_2'], ARRAY['column3']);
    SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','someTable', ARRAY['column_1', 'column_2'], ARRAY['column3'], 'column_2 IS NOT NULL');


Returns:
    A table containing the following statements in order
     - A create index concurrently statement for every child table
     - A create index statement for the parent table
     - A create index statement for the <table>_template table when it exists (deprecated, see above)
*/
CREATE OR REPLACE FUNCTION dba.partition_add_concurrent_index_on_partitioned_table(v_schema TEXT, v_table TEXT, v_columns TEXT[], v_include_list TEXT[] default NULL, v_method TEXT default 'btree', v_is_unique boolean default false, v_condition TEXT default NULL, v_create_parent_index boolean default true)
     RETURNS table(stmt text) LANGUAGE plpgsql
     SET search_path = pg_catalog, dba, pg_temp
     AS $func$

     DECLARE
         v_indexname            TEXT;
         v_row                  RECORD;
         v_row_count            INT;
         v_column_names         TEXT[];
         v_total_indexes        INT;
         v_column_name          TEXT;
         v_include_clause       TEXT := '';
         v_condition_clause     TEXT := '';
         v_columns_str          TEXT;
         v_index_exist          INT;

     BEGIN
         v_schema:=LOWER(v_schema);
         v_table:=LOWER(v_table);

         -- When creating a unique index we need to know the partition column
         IF (v_is_unique) THEN
             SELECT pci.v_column_name
             INTO v_column_name
             FROM dba.partition_get_partition_column_info(v_schema, v_table) AS pci;
         END IF;

         -- separate the column names from the rest of the arguments like functions and operators like 'desc', 'nulls first', etc
         SELECT ARRAY (SELECT regexp_replace(split_part(UNNEST(v_columns), ' ', 1), '^[^a-zA-Z0-9_]*([a-zA-Z0-9_]+).*$', '\1'))
         INTO v_column_names;

         IF (SELECT LOWER(v_method) NOT IN ('btree', 'hash', 'gist', 'spgist', 'gin', 'brin') ) THEN
             RAISE EXCEPTION 'Index method % is not supported', v_method;
         END IF;

         DROP TABLE IF EXISTS temp_partition_concurrent_indexes;

         -- Create a temporary table to store the results.
         CREATE TEMP TABLE IF NOT EXISTS temp_partition_concurrent_indexes (
            order_number int,
            table_name text,
            index_name text,
            index_def text default null,
            index_is_exist boolean default false
            )
         ON COMMIT DELETE ROWS;

         -- List all the child partitions
         EXECUTE FORMAT ($sql$
             INSERT INTO temp_partition_concurrent_indexes (
                 SELECT
                     1 AS order_number,
                     child.relname as table_name,
                     substring(LOWER(child.relname) || %L , 1, 59) || '_idx' AS index_name
                 FROM pg_inherits
                 JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                 JOIN pg_class child ON pg_inherits.inhrelid   = child.oid
                 JOIN pg_namespace nmsp_child ON nmsp_child.oid   = child.relnamespace
                 JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
                 WHERE
                     LOWER(nmsp_parent.nspname) = LOWER(%L)
                     AND LOWER(parent.relname)=LOWER(%L))
             $sql$,
             '_' || LOWER(array_to_string(v_column_names, '_')), v_schema, v_table);

         -- Add the parent table if a non-unique index OR it is a unique index, but the index contains the partition column
         IF ( ( ( NOT v_is_unique) OR ( v_is_unique AND v_column_name=ANY(lower(v_columns::text)::text[]) ) ) AND v_create_parent_index ) THEN
             EXECUTE FORMAT ($sql$
                 INSERT INTO temp_partition_concurrent_indexes values (2, %L, substring(LOWER(%L) || %L, 1, 59) || '_idx')
                 $sql$, v_table, v_table, '_' || LOWER(array_to_string(v_column_names, '_')));
         END IF;

         -- Deprecated: <table>_template tables are no longer created by this framework. This is only kept for
         -- backward compatibility with tables that still have one.
         perform 1
         FROM pg_class c
         WHERE LOWER(c.relname) = LOWER(v_table || '_template') AND LOWER(c.relnamespace::regnamespace::text) = LOWER(v_schema);

         IF FOUND THEN
              EXECUTE FORMAT ($sql$
                   INSERT INTO temp_partition_concurrent_indexes values (3, %L, substring(LOWER(%L) || %L, 1, 59) || '_idx')
                   $sql$, v_table || '_template', v_table || '_template', '_' || LOWER(array_to_string(v_column_names, '_')));
         END IF;

         FOR v_row IN SELECT * FROM temp_partition_concurrent_indexes LOOP
             EXECUTE FORMAT ($sql$
                 SELECT '1' FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid
                 AND relname = %L AND nspname = %L AND lower(relkind) = 'i'
             $sql$, v_row.index_name, v_schema) ;

             GET DIAGNOSTICS v_row_count = ROW_COUNT;

             IF v_row_count = 0 THEN
                 -- Index name is unique. We are done.
                 continue;
             ELSE
                 -- An index with this name already exists. Remove the suffix and add a number at the end.
                 -- After number 9 we give up and executing the create index statement will fail.
                 FOR counter in 1..9 LOOP
                     IF LENGTH(v_row.index_name) = 64 THEN
                         -- We should not cross the 64 characters when adding a number. Remove the suffix and one character.
                         v_indexname := left(v_row.index_name , -5) || counter || '_idx';
                     ELSE
                         v_indexname := left(v_row.index_name , -4) || counter || '_idx';
                     END IF;

                     RAISE DEBUG 'Testing index name % for uniqueness', v_indexname;

                     EXECUTE FORMAT ($sql$
                         SELECT '1' FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid
                         AND relname = %L AND nspname = %L AND lower(relkind) = 'i'
                     $sql$, v_indexname, v_schema) ;

                     GET DIAGNOSTICS v_row_count = ROW_COUNT;

                     IF v_row_count = 0 THEN
                         -- We have found a unique index name. Update the temp table with this name.
                         EXECUTE FORMAT($sql$ UPDATE temp_partition_concurrent_indexes SET index_name = %L WHERE table_name = %L
                         $sql$, v_indexname, v_row.table_name);

                         exit;
                     END IF;
                 END LOOP;
             END IF;
         END LOOP;

         SELECT COUNT(*) FROM temp_partition_concurrent_indexes
         INTO v_total_indexes;

         IF v_include_list IS NOT NULL THEN
            v_include_clause = ' INCLUDE (' || array_to_string(v_include_list, ',') || ')';
         END IF;

         IF v_condition IS NOT NULL THEN
            v_condition_clause = ' WHERE ' || v_condition;
         END IF;

         -- The column list is embedded in the generated statement through %L, which takes care of quotes in it
         SELECT array_to_string(v_columns, ', ') INTO v_columns_str;

         RAISE DEBUG '%',v_columns_str;

        EXECUTE format($idx$
        WITH s AS (
            SELECT
                index_name,
                'CREATE ' ||
                CASE WHEN %L::boolean THEN 'UNIQUE ' ELSE '' END || 'INDEX ' ||
                CASE WHEN (order_number = 1 OR %s = 1) THEN 'CONCURRENTLY ' ELSE '' END ||
                quote_ident(index_name) || ' ON ' || %L || '.' ||
                quote_ident(table_name) || ' USING ' || %L || ' ( ' || %L || ' )' ||
                %L ||
                %L ||
                ';' AS generated_index_def
            FROM temp_partition_concurrent_indexes
            ORDER BY order_number, index_name
        )
        UPDATE temp_partition_concurrent_indexes t
        SET index_def = s.generated_index_def
        FROM s
        WHERE t.index_name = s.index_name
        $idx$,

            v_is_unique,
            v_total_indexes,
            format('%I', v_schema),
            format('%I', v_method),
            LOWER(v_columns_str),
            v_include_clause,
            v_condition_clause
        );

        FOR v_row IN SELECT table_name,index_name,index_def FROM temp_partition_concurrent_indexes LOOP

            -- calling the function find_matching_index_by_definition to see if any existing index with same definition
            SELECT dba.find_matching_index_by_definition(v_schema,v_row.table_name,v_row.index_def) INTO v_index_exist;

            IF v_index_exist > 0 THEN -- if index exist 

                IF v_index_exist = 2 THEN -- if existing index is invalid we stop the execution with below error message
                    RAISE EXCEPTION 'There is an existing invalid index on table % with same index defenition AS %.Please validate it and proceed.', quote_ident(v_row.table_name) , quote_ident(v_row.index_def);
                END IF;

                UPDATE temp_partition_concurrent_indexes SET index_is_exist = TRUE WHERE index_name = v_row.index_name;

            END IF;

        END LOOP;


         RETURN QUERY
         EXECUTE format($sql$
             SELECT '/* Creating index ' || row_number() over (order by order_number, index_name) || ' of %s */' ||
            
                 'CREATE ' ||
                 CASE WHEN %L::boolean THEN 'UNIQUE ' ELSE '' END || 'INDEX ' ||
                 CASE WHEN (order_number = 1 OR %s = 1) THEN 'CONCURRENTLY ' ELSE '' END ||
                 quote_ident(index_name) || ' ON ' || %L || '.' ||
                 quote_ident(table_name) || ' USING ' || %L || ' ( ' || %L || ' )' ||
                 %L ||
                 %L ||
                 ';' 
             
             FROM temp_partition_concurrent_indexes
             WHERE index_is_exist is false
             ORDER BY order_number, index_name
         $sql$, v_total_indexes, v_is_unique, v_total_indexes, format('%I', v_schema), format('%I', v_method), LOWER(v_columns_str), v_include_clause, v_condition_clause);
     END
     $func$; 
