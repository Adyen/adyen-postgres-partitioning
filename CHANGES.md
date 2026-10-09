# Changes to adyen-postgres-partitioning

This update expands the library from 14 functions to 48, adding end-to-end partitioning workflows, automated maintenance, partition alignment, table optimization, revert support, and a richer set of inspection and diagnostic utilities.

## Contributors

- Chanukya Sista
- Dave Pitts
- Derk van Veen
- Dmitry Fomin
- Dwarka Rao
- Emre Ozcan
- Erald Himaj
- Maja Purcell
- Milen Blagojevic
- Saiful Muhajir

---

## Updated functions

**Case-insensitive identifiers**
Schema, table and column names are now matched case-insensitively across the framework, including the values stored in `dba.partition_configuration`. This affects `partition_table_native_wrapper`, `partition_table_native_aligned_wrapper`, `partition_add_constraints`, `partition_add_concurrent_index_on_partitioned_table`, `partition_calculate_free_partitions`, `partition_add_up_to_nr_of_free_partitions`, `partition_change_range_on_partitioned_table`, `partition_get_last_partition_details`, the three detach procedures, and `partition_maintenance`.

**`partition_table`, `partition_native`**
The `v_nopk` option (partitioning on a column that is not unique) has been removed. The parameter is kept so existing calls still work, but passing `TRUE` now raises an error. `partition_native` raises an error when a unique index does not contain the partition key, unless `p_allow_skipping_unique_indexes` is `TRUE`. For integer keys `partition_native` adds 1 to the end key it receives; the wrappers compensate for this. New parameters: `p_allow_skipping_unique_indexes` and `p_copy_statistics_to_children`.

**`partition_table_native_wrapper`, `partition_table_native_aligned_wrapper`**
Pass the two new `p_*` parameters through to `partition_native`.

**`partition_add_concurrent_index_on_partitioned_table`**
Added `v_include_list` (columns for an `INCLUDE` clause), `v_condition` (a `WHERE` clause for partial indexes) and `v_create_parent_index`. The new `v_include_list` parameter is the fourth parameter, so `v_method` moved to fifth position. Use named arguments for the optional parameters. Equivalent existing indexes are detected through `find_matching_index_by_definition`.

**`partition_add_concurrent_index_on_partitioned_table`, `partition_add_foreign_key_on_partitioned_table`**
Handling of a `<table>_template` table is deprecated. It is only kept for backward compatibility and will be removed in a future version.

**`partition_add_up_to_nr_of_free_partitions`**
New partitions are now created with `create_optimized_table_copy`, so they get the optimized column order, indexes, foreign keys, check constraints, triggers and owner of the last partition. Progress messages are logged at `LOG` level.

**`partition_table_revert`**
Foreign keys in other tables that reference the partitioned table are dropped before the revert and re-added afterwards, including foreign keys from partitioned tables, without a full table scan.

**`create_optimized_table_copy`**
Copies all check constraints, including those inherited from a partitioned parent. Also copies per-column statistics targets and extended statistics objects.

**`generate_create_table_optimized_columns`**
Generated columns are now emitted as `GENERATED ALWAYS AS (...) STORED|VIRTUAL` instead of a plain default.

**`partition_extend_all_partitioned_tables`**
Continues with the next table when extending a table fails, and returns `FALSE` at the end if any table failed.

**`partition_change_range_on_partitioned_table`**
New partitions now start at the upper boundary of the last remaining partition after the empty partitions have been detached. The first new partition is now created with `create_optimized_table_copy` instead of `CREATE TABLE ... (LIKE ... INCLUDING ALL)`, so it gets the same optimized column order, statistics and triggers as partitions created by `partition_add_up_to_nr_of_free_partitions`.

**`partition_fix_triggers_on_all_partitions`**
Log messages now name the reference child partition or the parent table.

**`find_matching_index_by_definition`**
Parameters are named `p_schema_name`, `p_table_name` and `p_create_index_sql`.

**`partition_copy_indexes_to_new_table`**
Index names that start with `<schema>_<table>` are no longer mangled when generating new index names.

**`partition_maintenance`**
Now calls `partition_extend_all_partitioned_tables`, chooses between `partition_detach_partitions` and `partition_detach_partitions_without_uuidv7` depending on whether UUID-partitioned tables exist, runs `partition_query_based_maintenance_detach_partitions`, and checks trigger consistency on all partitioned tables.

**`partition_detach_partition`**
Added a `v_lock_timeout_ms` parameter (default 1000 ms) so each lock attempt times out rather than waiting indefinitely. Before detaching, the function now pre-locks all tables that hold a foreign key referencing the partition, preventing deadlocks. The insert into `dba.detached_partitions` now uses `ON CONFLICT DO UPDATE` so re-detaching a previously recorded partition (for example after a revert-and-re-partition cycle) no longer raises an error.

---

## End-to-end partitioning wrappers

These functions handle the full workflow of converting a regular table into a partitioned one in a single call.

**`partition_table_native_wrapper`**
Locks the table, calls `partition_native` to create the mammoth and initial partitions, adds free partitions up to the configured minimum, runs `ANALYZE`, and inserts a row into `dba.partition_configuration` to enable automatic maintenance. Supports integer, date, and timestamp partition keys. Parameters allow tuning lock timeout, retry count, retry sleep interval, and whether to skip the final statistics step.

**`partition_table_native_aligned_wrapper`**
Extends the native wrapper with grid alignment. Given an already-partitioned leader table, it derives the leader's grid width and anchor, computes a bridge partition that snaps the boundary up to the nearest aligned value (using a P/3 skip rule to avoid creating a near-empty bridge), then creates the first proper aligned partition. All subsequent partitions added by maintenance land on the same grid as the leader. Supports a dry-run mode that logs the full plan without making any structural changes.

---

## Detach procedures

Four new procedures cover different detach scenarios, all reading from `dba.partition_configuration` and delegating each detach attempt to `lock_safe_execute`.

**`partition_detach_partitions`**
Iterates all tables configured in `dba.partition_configuration` with a `detach` interval. Detaches any partition whose upper boundary is older than `NOW() - detach_interval`. Works on date, timestamp, and UUIDv7 partition keys.

**`partition_detach_partitions_without_uuidv7`**
Same as above but limited to date and timestamp partition keys. Use this when UUIDv7 helper functions are not available in the database.

**`partition_query_based_detach_partitions`**
Detaches partitions based on the result of an arbitrary SQL query rather than the partition boundary. The query uses the placeholder `<<partition>>`, which is replaced with the quoted, schema-qualified partition name at runtime. If the interval between the query result and the current date exceeds the configured threshold, the partition is detached.

**`partition_query_based_maintenance_detach_partitions`**
A driver procedure that reads `detach_query` and `detach` entries from `dba.partition_configuration` and calls `partition_query_based_detach_partitions` for each configured table.

---

## Maintenance functions

**`partition_extend_all_partitioned_tables`**
Scans `dba.partition_configuration` for tables with `auto-maintenance: true` and calls `partition_add_up_to_nr_of_free_partitions` on each one, ensuring at least 3 free partitions always exist.

**`partition_change_range_on_partitioned_table`**
Changes the partition interval on a table while it is live. Detaches all empty future partitions, then re-creates the same number of partitions using the new interval. The active (data-containing) partition is never touched. Optionally drops the detached empty partitions immediately.

**`partition_realign_boundaries`**
Corrects misaligned future partitions on an integer-range table without touching the data partition. Detaches the empty partitions ahead of the active partition, inserts a bridge partition to cover the gap up to the first correctly aligned boundary, then fills up to 3 new grid-aligned partitions. Supports dry-run mode.

**`partition_realign_with_leader`**
Same as `partition_realign_boundaries`, but derives the target grid width and anchor from a leader table rather than accepting them as explicit parameters.

**`partition_alter_partitioned_table_options`**
Generates `ALTER TABLE` statements to apply a storage option clause (for example `SET (autovacuum_enabled = false)`) to the parent table and all its partitions. Returns the statements as a set of rows for review before execution.

**`partition_drop_default_partition`**
Detaches and drops the default partition if it exists and is empty. Raises an error if the default partition contains rows.

**`partition_table_is_partitioned`**
Returns `TRUE` if the given table exists and is natively range-partitioned, `FALSE` otherwise.

**`partition_get_partition_column_info`**
Returns the partition column name and its data type for a given partitioned table.

**`partition_partitioned_on_primary_key`**
Returns `TRUE` if the partition key column is part of the table's primary key.

**`partition_get_grid_width`**
Returns the width (interval) of the last non-mammoth partition. This represents the canonical partition size used for extending and aligning.

**`partition_get_active_upper_bound`**
Returns the upper boundary of the partition that currently contains the maximum key value (integer types only). This is the boundary ahead of which all partitions are considered free.

**`partition_get_current_partition_boundaries`**
Returns the name, lower bound, and upper bound of the partition that contains the current maximum key value.

**`partition_compute_bridge_upper`**
Computes the upper bound of a bridge partition given a start value, a grid anchor, and a grid width. Applies the P/3 skip rule: if the bridge would be shorter than one third of the grid width, it is extended by one full grid width to avoid creating a very thin partition.

**`partition_check_triggers_on_all_partitions`**
Verifies that all triggers on the parent table are also present with matching definitions on every child partition. Returns `TRUE` if consistent.

**`partition_fix_triggers_on_all_partitions`**
Identifies trigger inconsistencies across partitions (missing triggers, or triggers whose definitions differ from the parent) and generates the DDL statements needed to fix them.

---

## Table copy and column optimization

**`get_optimized_column_order`**
Returns a suggested column ordering for a table that minimizes alignment padding. Columns are sorted by their storage alignment requirement (8-byte, 4-byte, 2-byte, 1-byte, then variable-length) to reduce wasted space in each row.

**`generate_create_table_optimized_columns`**
Generates a `CREATE TABLE` statement with the optimized column order produced by `get_optimized_column_order`, including all data types, defaults, NOT NULL constraints, and collations.

**`create_optimized_table_copy`**
Creates a full copy of a source table under a new name with columns reordered for minimal alignment padding. Copies indexes (including primary key), foreign key constraints, check constraints, storage parameters, column-level options, triggers, and table owner. Intended for use when creating new partition slabs that benefit from a tighter physical layout.

**`find_matching_index_by_definition`**
Given a `CREATE INDEX` statement, finds an existing index on the target table that matches by canonical definition (normalizing whitespace, expression case, and operator class notation). Used internally by `partition_add_concurrent_index_on_partitioned_table` and `create_optimized_table_copy`.

---

## Revert functions

**`partition_table_revert`**
Reverts a natively partitioned table back to a regular unpartitioned table. Requires that all non-mammoth partitions are empty (i.e. still within the rollback window). Under an exclusive lock it detaches all non-mammoth partitions, detaches the mammoth, renames indexes and constraints back to their original names, retires the partitioned shell as `<table>_partitioned_retired`, and promotes the mammoth back to the original table name. Supports dry-run mode. Transaction control is the caller's responsibility.

**`partition_table_revert_cleanup`**
To be called after committing a `partition_table_revert`. Drops all detached non-mammoth partitions and the retired shell, and removes the corresponding rows from `dba.detached_partitions` and `dba.partition_configuration`.

---

## Migration

**`partition_convert_inheritance_to_native`**
Converts a table partitioned using PostgreSQL inheritance into a natively partitioned table. Handles moving data, recreating constraints and indexes, and updating `dba.partition_configuration`.

---

## Reporting

**`partition_report_table_free_extends_below_threshold`**
Writes a CSV file listing all partitioned tables whose number of free partitions is below a given threshold. Useful for monitoring scripts.

**`partition_report_table_free_extends_below_date_threshold`**
Same as above, but for date- and timestamp-partitioned tables: reports tables where the furthest free partition boundary is fewer than N days in the future.

---

## Utility and infrastructure

**`lock_safe_execute`**
Executes a SQL statement, function call, or procedure call with a lock timeout. If a lock cannot be acquired within `v_detach_lock_timeout_ms` milliseconds, the attempt is abandoned and retried after `v_detach_retry_sleep_sec` seconds. Supports a maximum retry count and an optional time window (only attempt between `v_time_start` and `v_time_end`). Used internally by all detach and revert operations.

**`calculate_query_date_interval`**
Executes a query that returns a single date value and computes the interval between that date and a reference date (defaulting to the current date). Used in query-based detach logic to evaluate whether a partition is old enough to detach.

**`fmt_readable_number`**
Formats a `bigint` as a string with underscore separators every three digits (for example `1_234_567_890`). Used in log messages throughout the library to make large partition boundary values easier to read.

**`uuid_v7_to_timestamptz`, `uuid_timestamptz_to_v7`**
Plain SQL helpers in the `dba` schema that convert between a UUIDv7 and its embedded timestamp. They need no extension and are used by the UUIDv7 detach logic.

---

## Security hardening

- All dynamic SQL quotes identifiers with `%I` / `quote_ident` and values with `%L` / `quote_literal`. Statements passed on to `lock_safe_execute` are built with `format()` and passed as one literal, so a quote in a schema, table or partition name can no longer break out of the statement.
- Every function and procedure pins `search_path` to `pg_catalog, dba, pg_temp`. Procedures that `COMMIT` set it again after every commit. Calls to other library functions are schema-qualified with `dba.`. `partition_maintenance.sql` sets the same `search_path` and resets it at the end.
- `partition_add_foreign_key_on_partitioned_table` quotes child table names and handles multi-column foreign keys.
- `lock_safe_execute` no longer reads and executes files, and `execute_file` has been removed.
- `partition_drop_detached_partition` uses static SQL for its bookkeeping and builds the `DROP TABLE` with `format()`.
- `detach_query` values run with the pinned `search_path`, so they must schema-qualify the tables they reference. `<<partition>>` is replaced with the quoted partition name.

## Tests

`test/framework` contains unit tests for the functions, with a runner (`run_partition_tests.sh`) that creates a database, loads all functions, runs the tests and reports the results. See `test/framework/README.md`.
