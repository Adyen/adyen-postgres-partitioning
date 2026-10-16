/*
        This function will duplicate primary key and other indexes
        from template table

        PARAMETER                       TYPE    DESCRIPTION
        v_schema                        TEXT    schema the template table resides in
        v_template                      TEXT    template to take the indexes from
        v_new_table                     TEXT    table to apply indexes to
        v_randomize                     BOOLEAN option to randomize index name, default TRUE
        p_skip_unique_indexes           BOOLEAN when TRUE, unique (non-primary) indexes are skipped.
                                                If p_skip_unique_index_column_name is also provided,
                                                only unique indexes NOT containing that column are skipped.
                                                Primary keys are never skipped. Default FALSE.
        p_skip_unique_index_column_name TEXT    when NULL and p_skip_unique_indexes is TRUE, ALL unique
                                                (non-primary) indexes are skipped. When set to a column name,
                                                only unique (non-primary) indexes that do NOT include this
                                                column are skipped. Ignored when p_skip_unique_indexes is FALSE.

        Each create index statement is assembled from the target identifiers (index name, schema and table) and the
        tail of the source index definition: everything from USING onwards for an index, the parenthesised column list
        for a primary key. The ONLY keyword is preserved when the source index carries it, so copying an index from a
        partitioned table does not recurse into the target's partitions.

        Example: SELECT dba.partition_copy_indexes_to_new_table('public', 'orders', 'orders_20220301_20220331');
                 SELECT dba.partition_copy_indexes_to_new_table('public', 'orders', 'orders_20220301_20220331', false);
                 SELECT dba.partition_copy_indexes_to_new_table('public', 'orders_mammoth', 'random_table', false, true, 'order_date');
        
*/
CREATE OR REPLACE FUNCTION dba.partition_copy_indexes_to_new_table(v_schema TEXT, v_template TEXT, v_new_table TEXT, v_randomize BOOLEAN DEFAULT TRUE, p_skip_unique_indexes BOOLEAN DEFAULT FALSE, p_skip_unique_index_column_name TEXT DEFAULT NULL)
                RETURNS BOOLEAN LANGUAGE plpgsql
                SET search_path = pg_catalog, dba, pg_temp
                AS $func$
                DECLARE
                    v_row                       RECORD;  -- one index of the template table per iteration
                    v_final_creation_statement  TEXT;    -- the DDL statement that is executed
                    v_newindexname              TEXT;    -- name the copied index gets on the new table
                    -- Set when the template table is a partition, used by the partition naming rules.
                    v_parent_name               TEXT;    -- name of the partitioned parent of the template
                    v_is_partition              BOOLEAN; -- TRUE when template and target look like partitions
                    v_use_partition_naming      BOOLEAN; -- TRUE while partition naming is still viable
                    v_new_suffix                TEXT;    -- part of the target name after the parent name
                    v_new_lower                 TEXT;    -- lower boundary taken from the target name
                    v_new_upper                 TEXT;    -- upper boundary taken from the target name
                    v_col_suffix                TEXT;    -- column part taken from the template index name
                    -- Character budgets and the values actually used once names are trimmed to 63 chars.
                    v_max_upper                 INT;
                    v_max_col                   INT;
                    v_max_parent                INT;
                    v_parent_used               TEXT;
                    v_upper_used                TEXT;
                    v_col_used                  TEXT;
                    v_index_exists              BOOLEAN; -- TRUE when the target index name is taken
                    v_index_def_tail            TEXT;    -- reused part of the source index definition
                BEGIN

                RAISE DEBUG 'Copying indexes FROM % TO %', v_schema ||'.'|| v_template, v_schema ||'.'|| v_new_table;

                -- Look up the parent of the template table. A row comes back only when the template is
                -- a partition or an inheritance child, which is the case when indexes are copied from
                -- one partition to the next.
                SELECT parent.relname
                INTO v_parent_name
                FROM pg_inherits i
                JOIN pg_class child ON child.oid = i.inhrelid
                JOIN pg_class parent ON parent.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = child.relnamespace
                WHERE n.nspname = LOWER(v_schema)
                  AND child.relname = LOWER(v_template);

                v_is_partition := v_parent_name IS NOT NULL;
                -- The template is a partition, so the target is expected to be a partition as well and
                -- to be named <parent>_<lower boundary>_<upper boundary>. Split the target name into
                -- those two boundaries, they are needed to name the copied indexes.
                IF v_is_partition THEN
                    v_new_suffix := substring(LOWER(v_new_table) FROM length(v_parent_name) + 2);
                    v_new_lower := split_part(v_new_suffix, '_', 1);
                    v_new_upper := substring(v_new_suffix FROM length(v_new_lower) + 2);
                    -- The target name does not carry two boundaries, so it is not a partition name.
                    -- Treat the target as a plain table and use the generic naming rules instead.
                    IF v_new_lower IS NULL OR v_new_upper IS NULL OR v_new_lower = '' OR v_new_upper = '' THEN
                        v_is_partition := FALSE;
                    END IF;
                END IF;

                -- Walk over every index of the template table. indisprimary tells a primary key apart
                -- from a plain index, indisunique and indkey drive the skip rules below.
                FOR v_row IN
                    WITH indexes AS (
                      SELECT indexdef, indexname FROM pg_indexes
                      WHERE schemaname = LOWER(v_schema)
                        AND tablename = LOWER(v_template)
                    )
                    SELECT indexdef, indisprimary, indisunique, pg_index.indkey, indexname FROM indexes
                    JOIN pg_class ON pg_class.relname = indexes.indexname
                    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
                    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                    WHERE pg_namespace.nspname = LOWER(v_schema)
                LOOP
                    -- Skip rules for unique indexes that are not the primary key. They exist because a
                    -- partitioned parent only accepts a unique index that contains the partition key.
                    IF p_skip_unique_indexes IS TRUE
                       AND v_row.indisunique IS TRUE
                       AND v_row.indisprimary IS FALSE THEN
                        -- No unique column name provided, so every unique index is skipped.
                        IF p_skip_unique_index_column_name IS NULL THEN
                            RAISE LOG 'Skipping unique index % (p_skip_unique_indexes is TRUE, no column filter)',
                                v_row.indexname;
                            CONTINUE;
                        -- A unique column name is provided and this index does not contain it, so it is skipped. Unique
                        -- indexes that do contain the column fall through and are copied.
                        ELSIF NOT EXISTS (
                            SELECT 1 FROM pg_attribute a
                            WHERE a.attrelid = format('%I.%I', LOWER(v_schema), LOWER(v_template))::regclass
                              AND a.attnum = ANY(v_row.indkey::int2[])
                              AND a.attname = LOWER(p_skip_unique_index_column_name)
                        ) THEN
                            RAISE LOG 'Skipping unique index % (column % not in index)',
                                v_row.indexname, p_skip_unique_index_column_name;
                            CONTINUE;
                        END IF;
                    END IF;

                    -- A primary key is copied as a constraint, everything else as an index.
                    IF v_row.indisprimary IS TRUE THEN
                        -- The constraint is named after the target table plus _pkey. Cut the table name
                        -- at 58 characters so the suffix still fits in the 63 character identifier limit.
                        IF length(v_new_table) > 58 THEN
                            v_newindexname := substring(LOWER(v_new_table), 1, 58);
                        ELSE
                            v_newindexname := LOWER(v_new_table);
                        END IF;
                        -- The primary key is added as a constraint, so only the parenthesised part of
                        -- the source definition is reused: the column list plus any index parameters
                        -- that follow it. It starts at the first '(' after USING, which skips the index
                        -- name, the source table and the access method.
                        v_index_def_tail := substring(v_row.indexdef FROM strpos(v_row.indexdef, ' USING '));
                        v_index_def_tail := substring(v_index_def_tail FROM strpos(v_index_def_tail, '('));
                        v_final_creation_statement := format('ALTER TABLE ONLY %I.%I ADD CONSTRAINT %I PRIMARY KEY %s;',
                            LOWER(v_schema), LOWER(v_new_table), LOWER(v_newindexname) || '_pkey', v_index_def_tail);
                        -- Adding a second primary key is an error, so leave the target alone when it
                        -- already has one, for instance because it was created with LIKE INCLUDING ALL.
                        IF (
                            SELECT indisprimary FROM pg_indexes
                            JOIN pg_class ON pg_class.relname = pg_indexes.indexname
                            JOIN pg_index ON pg_class.oid = pg_index.indexrelid
                            JOIN pg_namespace ON (pg_namespace.oid = pg_class.relnamespace AND pg_namespace.nspname = LOWER(v_schema))
                            WHERE schemaname = LOWER(v_schema) AND tablename = LOWER(v_new_table) AND indisprimary = TRUE
                        ) IS TRUE THEN
                            RAISE NOTICE 'PRIMARY KEY exists, skipping';
                        -- Add the primary key constraint to the target table.
                        ELSE
                            RAISE DEBUG 'Creating primary key using: %', v_final_creation_statement;
                            EXECUTE v_final_creation_statement;
                        END IF;
                    ELSE
                        -- Partition naming strategy:
                        -- 1) Always include full lower boundary in the index name.
                        -- 2) If length exceeds 63 chars, truncate upper boundary first.
                        -- 3) If still too long, truncate indexed column suffix.
                        -- 4) If still too long, fall back to non-partition naming.
                        v_use_partition_naming := v_is_partition;
                        -- Both tables are partitions, so aim for a name that reads
                        -- <parent>_<lower>_<upper>_<columns>_idx.
                        IF v_use_partition_naming THEN
                            -- Extract column suffix from template index name, ignoring any truncated boundaries.
                            v_col_suffix := LOWER(v_row.indexname);
                            v_col_suffix := substring(v_col_suffix FROM length(v_parent_name) + 2);
                            v_col_suffix := regexp_replace(v_col_suffix, '^(\d+_)+', '');
                            v_col_suffix := regexp_replace(v_col_suffix, '_idx\d*$', '');
                            v_col_suffix := regexp_replace(v_col_suffix, '_+$', '');
                            -- Nothing usable is left, or what is left starts with a digit and would read
                            -- as a boundary, so the generic naming rules take over.
                            IF v_col_suffix IS NULL OR v_col_suffix = '' OR v_col_suffix ~ '^\d' THEN
                                v_use_partition_naming := FALSE;
                            ELSE
                                -- Build the initial name with full upper boundary and full column suffix.
                                v_parent_used := LOWER(v_parent_name);
                                v_upper_used := v_new_upper;
                                v_col_used := v_col_suffix;
                                v_newindexname := v_parent_used || '_' || v_new_lower || '_' || v_upper_used || '_' || v_col_used || '_idx';
                                -- The name does not fit the 63 character identifier limit, so parts of it
                                -- are shortened below in order of decreasing importance.
                                IF length(v_newindexname) > 63 THEN
                                    -- Step 1: shorten the upper boundary while keeping full lower boundary.
                                    -- The 7 accounts for the three separators and the _idx suffix.
                                    v_max_upper := 63 - length(v_parent_used) - length(v_new_lower) - length(v_col_used) - 7;
                                    -- There is room left for a shortened upper boundary.
                                    IF v_max_upper >= 1 THEN
                                        v_upper_used := substring(v_new_upper, 1, v_max_upper);
                                    ELSE
                                        -- Step 2: upper boundary at 1 char, then trim column suffix.
                                        v_upper_used := substring(v_new_upper, 1, 1);
                                        v_max_col := 63 - length(v_parent_used) - length(v_new_lower) - length(v_upper_used) - 7;
                                        -- Keep as much of the column suffix as fits, dropping a trailing
                                        -- separator. Nothing left to keep means the suffix is dropped.
                                        IF v_max_col >= 1 THEN
                                            v_col_used := substring(v_col_used, 1, v_max_col);
                                            v_col_used := regexp_replace(v_col_used, '_+$', '');
                                            IF v_col_used = '' THEN
                                                v_col_used := NULL;
                                            END IF;
                                        -- Not even one character fits, so the name carries no column part.
                                        ELSE
                                            v_col_used := NULL;
                                        END IF;
                                    END IF;

                                    -- Rebuild after truncation decisions.
                                    IF v_col_used IS NULL THEN
                                        v_newindexname := v_parent_used || '_' || v_new_lower || '_' || v_upper_used || '_idx';
                                    ELSE
                                        v_newindexname := v_parent_used || '_' || v_new_lower || '_' || v_upper_used || '_' || v_col_used || '_idx';
                                    END IF;

                                    -- Still too long, which means the parent name itself is the problem.
                                    IF length(v_newindexname) > 63 THEN
                                        -- Step 3: as a last resort, trim parent name to fit.
                                        -- The 6 accounts for the two separators and the _idx suffix, and
                                        -- the column part is subtracted when it survived step 2.
                                        v_max_parent := 63 - length(v_new_lower) - length(v_upper_used) - 6;
                                        IF v_col_used IS NOT NULL THEN
                                            v_max_parent := v_max_parent - length(v_col_used) - 1;
                                        END IF;
                                        -- A shortened parent name still leaves a usable name, so rebuild
                                        -- the name once more with it.
                                        IF v_max_parent >= 1 THEN
                                            v_parent_used := substring(v_parent_used, 1, v_max_parent);
                                            IF v_col_used IS NULL THEN
                                                v_newindexname := v_parent_used || '_' || v_new_lower || '_' || v_upper_used || '_idx';
                                            ELSE
                                                v_newindexname := v_parent_used || '_' || v_new_lower || '_' || v_upper_used || '_' || v_col_used || '_idx';
                                            END IF;
                                        -- The boundaries alone already fill the limit, so give up on
                                        -- partition naming and use the generic rules.
                                        ELSE
                                            v_use_partition_naming := FALSE;
                                        END IF;
                                    END IF;
                                END IF;
                            END IF;
                        END IF;

                        -- Generic naming, used for plain tables and whenever the partition rules above
                        -- could not produce a name.
                        IF NOT v_use_partition_naming THEN
                            -- The template index name contains the template table name, so swapping that
                            -- for the target table name keeps the original, descriptive name.
                            IF LOWER(v_row.indexname) ~ LOWER(v_template) THEN
                                v_newindexname := regexp_replace(v_row.indexname, LOWER(v_template), LOWER(v_new_table));
                            ELSE
                                -- this is forced because we need table name in indexname
                                -- otherwise it can cause further break down
                                -- Randomizing keeps repeated calls from colliding on the same name.
                                IF v_randomize THEN
                                    v_newindexname := substring(LOWER(v_new_table), 1, 56) || '_idx_' || trunc(random() * 98 + 1);
                                ELSE
                                    v_newindexname := substring(LOWER(v_new_table), 1, 59) || '_idx';
                                END IF;
                            END IF;
                            -- The name exceeds the 63 character identifier limit, so cut it back and
                            -- re-apply the suffix.
                            IF length(v_newindexname) > 63 THEN
                                IF v_randomize THEN
                                    v_newindexname := substring(LOWER(v_newindexname), 1, 56) || '_idx' || trunc(random() * 98 + 1);
                                ELSE
                                    v_newindexname := substring(LOWER(v_newindexname), 1, 59) || '_idx';
                                END IF;
                            END IF;
                        END IF;

                        -- Check whether the chosen name is already taken in this schema. Index names are
                        -- unique per schema, not per table.
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = LOWER(v_schema)
                              AND c.relkind = 'i'
                              AND c.relname = LOWER(v_newindexname)
                        ) INTO v_index_exists;

                        -- The name is taken, so this index is assumed to be present already and is left
                        -- as it is.
                        IF v_index_exists THEN
                            RAISE NOTICE 'Index % exists, skipping', v_newindexname;
                        -- Create the index on the target table.
                        ELSE
                            -- Assemble the CREATE INDEX statement from four parts: the uniqueness of
                            -- the source index, the target identifiers (new index name, schema and new
                            -- table) quoted through format's %I, the ONLY keyword when the source
                            -- definition carries it, and the tail of the source definition.
                            -- The tail is everything from USING onwards (access method, columns,
                            -- INCLUDE, WITH, TABLESPACE, WHERE) and is carried over unchanged, so it
                            -- keeps clauses this function does not need to interpret.
                            -- pg_get_indexdef emits ON ONLY for an index on a partitioned table. Keep
                            -- it: dropping ONLY would recurse into the target's partitions and build
                            -- an index on every existing partition instead of creating only the
                            -- (invalid) parent index the source definition describes. The check is
                            -- limited to the head of the definition so a predicate containing the
                            -- words ON ONLY cannot trigger it.
                            v_index_def_tail := substring(v_row.indexdef FROM strpos(v_row.indexdef, ' USING '));
                            v_final_creation_statement := format('%s %I ON %s%I.%I%s',
                                CASE WHEN v_row.indisunique THEN 'CREATE UNIQUE INDEX' ELSE 'CREATE INDEX' END,
                                LOWER(v_newindexname),
                                CASE WHEN substring(v_row.indexdef, 1, strpos(v_row.indexdef, ' USING ')) LIKE '% ON ONLY %'
                                     THEN 'ONLY ' ELSE '' END,
                                LOWER(v_schema), LOWER(v_new_table), v_index_def_tail);
                            RAISE DEBUG 'Creating an index using: %', v_final_creation_statement;
                            -- Not EXECUTE format(...): a partial index predicate can contain a percent sign.
                            EXECUTE v_final_creation_statement;
                        END IF;
                    END IF;
                END LOOP;
                RETURN TRUE;
                END
                $func$;
            
