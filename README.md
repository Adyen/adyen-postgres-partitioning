# adyen-postgres-partitioning

A PostgreSQL function library for creating and maintaining range-partitioned tables with minimal application impact. The library prioritises the weakest lock possible at every step. When an exclusive lock is unavoidable it is always taken with a timeout, retried automatically, and optionally restricted to a configurable time window.

Every function starts with a detailed comment block describing what it does, all parameters, and usage examples.

## Requirements

PostgreSQL 13 or later.

## Installation

Create the schema and supporting tables, then load all functions. Run these from the repository root in `psql`:

```sql
\i sql/schema/schema.sql
\i sql/tables/tables.sql
\i sql/functions/create_all_functions.sql
```

### Run the tests

```sql
\i test/tables.sql
\i test/run_functions.sql
\i test/configuration.sql
\i test/cleanup.sql
```

#### Unit tests

A larger set of unit tests lives in `test/framework`. The runner creates a fresh database, loads the schema and all functions, runs every test and drops the database when all tests pass:

```bash
./test/framework/run_partition_tests.sh                       # all tests
./test/framework/run_partition_tests.sh test_partition_table  # a single test
```

See [test/framework/README.md](test/framework/README.md) for the connection settings and details.

---

## Part 1 — Partitioning an existing table

### How it works

When a table is partitioned, the original table is renamed to `<table>_mammoth` and becomes the first partition, holding all pre-existing data. A new empty table called `<table>` is created as the partitioned parent. Forward-looking partitions are attached immediately and the table is registered in `dba.partition_configuration` for automatic maintenance.

### Option A: standard partitioning

`partition_table_native_wrapper` converts a regular table into a natively partitioned table in one call. It locks the table, creates the mammoth and the first free partitions, runs `ANALYZE`, and registers the table for automatic maintenance.

```sql
-- Partition on a date column
SELECT dba.partition_table_native_wrapper(
    'public', 'orders', 'order_date',
    '2020-01-01',   -- lower boundary of the mammoth (oldest data)
    '2024-01-01',   -- exclusive upper boundary of the mammoth
    '1 month'       -- interval for new partitions
);

-- Partition on an integer column
SELECT dba.partition_table_native_wrapper(
    'public', 'orders', 'order_id',
    '0',             -- lower boundary of the mammoth
    '1000000000',    -- exclusive upper boundary of the mammoth
    '100000000'      -- interval for new partitions
);

-- Tune lock behaviour
SELECT dba.partition_table_native_wrapper(
    'public', 'orders', 'order_id', '0', '1000000000', '100000000',
    v_detach_lock_timeout_ms => 500,
    v_max_retries            => 20
);
```

| Parameter | Default | Description |
|---|---|---|
| `v_schemaname` | — | Schema of the table |
| `v_tablename` | — | Table name |
| `v_keycolumn` | — | Partition key column |
| `v_startkey` | — | Lower boundary of the mammoth partition |
| `v_endkey` | — | Exclusive upper boundary of the mammoth partition |
| `v_interval` | — | Width of each new partition |
| `v_move_trg` | `TRUE` | Move triggers to the partitioned table |
| `v_detach_lock_timeout_ms` | `1000` | Lock timeout per attempt (ms) |
| `v_detach_retry_sleep_sec` | `20` | Sleep between lock attempts (s) |
| `v_max_retries` | `10` | Maximum lock attempts |
| `v_skip_statistics` | `FALSE` | Skip final ANALYZE (not recommended) |
| `p_allow_skipping_unique_indexes` | `FALSE` | Allow unique indexes that do not contain the partition key to be skipped on the partitioned table. When `FALSE`, such an index raises an exception because uniqueness would only be enforced per partition |
| `p_copy_statistics_to_children` | `FALSE` | Keep extended statistics and per-column statistics targets on the first new partition so they propagate to future partitions |

Schema, table and column names are matched case-insensitively and stored in lowercase.

### Option B: partition and align with a leader table

Use `partition_table_native_aligned_wrapper` when the new table must share partition boundaries with an existing integer-partitioned table. The function derives the grid width and alignment anchor from the leader, inserts a bridge partition to snap up to the first aligned boundary, and creates the first proper aligned partition. All subsequent partitions added by maintenance will land on the same grid.

Always do a dry run first — it logs the full plan without making any structural changes.

```sql
-- Step 1: dry run (default) — review the plan in the server logs
SELECT dba.partition_table_native_aligned_wrapper(
    'public', 'order_items', 'orders', '1000000000'
);

-- Step 2: apply
SELECT dba.partition_table_native_aligned_wrapper(
    'public', 'order_items', 'orders', '1000000000',
    v_dry_run => FALSE
);

-- Override the partition column on the target table if it differs from the leader's
SELECT dba.partition_table_native_aligned_wrapper(
    'public', 'order_items', 'orders', '1000000000',
    v_dry_run     => FALSE,
    v_column_name => 'item_id'
);
```

| Parameter | Default | Description |
|---|---|---|
| `v_schemaname` | — | Schema of both tables |
| `v_tablename` | — | Table to partition |
| `v_leader_tablename` | — | Already-partitioned table to align with |
| `v_switch_boundary` | — | Exclusive upper boundary of the mammoth; no rows may exist at or above this value |
| `v_dry_run` | `TRUE` | When TRUE, logs the plan but makes no changes |
| `v_column_name` | `NULL` | Partition column on the target table; defaults to the leader's column |
| `v_move_trg` | `TRUE` | Move triggers to the partitioned table |
| `v_detach_lock_timeout_ms` | `1000` | Lock timeout per attempt (ms) |
| `v_detach_retry_sleep_sec` | `20` | Sleep between lock attempts (s) |
| `v_max_retries` | `10` | Maximum lock attempts |
| `v_skip_statistics` | `FALSE` | Skip final ANALYZE (not recommended) |
| `p_allow_skipping_unique_indexes` | `FALSE` | Allow unique indexes that do not contain the partition key to be skipped on the partitioned table |
| `p_copy_statistics_to_children` | `FALSE` | Keep extended statistics and per-column statistics targets on the first new partition |

---

## Part 2 — Maintenance

Maintenance keeps the partition set healthy over time. It covers four concerns: extending the partition set ahead of incoming data, detaching old partitions, dropping detached partitions after a cooling-off period, and adding date constraints. All of these are configured per table in `dba.partition_configuration`.

### Configuration

Each row in `dba.partition_configuration` identifies a table and carries a JSON configuration object:

| Key | Type | Description |
|---|---|---|
| `auto-maintenance` | boolean | Enable automatic partition extension |
| `nr` | integer | Minimum number of free partitions to maintain (default: 3) |
| `detach` | interval | Detach partitions older than this interval (e.g. `'2 years'`) |
| `drop_detached` | interval | Drop detached partitions after this cooling-off period (e.g. `'4 days'`) |
| `date_constraint` | object | Add date check constraints to an integer-keyed table |
| `detach_query` | text | SQL query used for query-based detach (see below) |

Example:

```sql
INSERT INTO dba.partition_configuration VALUES (
    'public', 'orders',
    '{"auto-maintenance": true, "nr": 6, "detach": "2 years", "drop_detached": "7 days"}'
);
```

### Step 1 — Extend: keep free partitions available

Call `partition_extend_all_partitioned_tables` on a schedule (for example nightly). It processes every table with `auto-maintenance: true` and creates new partitions until each table has at least the configured minimum number of free ones.

- For integer-partitioned tables: free means the lower boundary of the partition is above the current maximum value in the table.
- For date/timestamp-partitioned tables: free means the starting date of the partition is after today.

```sql
SELECT dba.partition_extend_all_partitioned_tables();
```

To extend a single table manually:

```sql
SELECT dba.partition_add_up_to_nr_of_free_partitions('public', 'orders', 5);
```

### Step 2 — Detach: retire old partitions

Detached partitions are no longer accessible through the parent table but still exist in the database. They are recorded in `dba.detached_partitions` with their original boundaries and the date of detachment.

**Boundary-based detach** — detaches any partition whose upper boundary is older than `NOW() - detach`:

```sql
-- For date, timestamp, and UUIDv7 partition keys
CALL dba.partition_detach_partitions();

-- For date and timestamp partition keys only
CALL dba.partition_detach_partitions_without_uuidv7();
```

**Query-based detach** — detaches partitions based on the result of a SQL query against the partition's actual data. Use the placeholder `<<partition>>`, which is replaced with the quoted, schema-qualified partition name at runtime (`<schema>.<<partition>>` also works). This is useful when the detach decision depends on the data inside the partition rather than its boundary.

> **Note:** the query runs with `search_path` set to `pg_catalog, dba, pg_temp`, so schema-qualify any other tables or functions it references.

Configure it in `dba.partition_configuration`:

```sql
INSERT INTO dba.partition_configuration VALUES (
    'public', 'orders',
    '{"auto-maintenance": true, "detach_query": "SELECT max(order_date) FROM <<partition>>", "detach": "2 years", "drop_detached": "7 days"}'
);
```

Run the maintenance driver on a schedule to process all configured tables:

```sql
CALL dba.partition_query_based_maintenance_detach_partitions();
```

Or call it directly for a single table:

```sql
CALL dba.partition_query_based_detach_partitions(
    'public',
    'orders',
    'SELECT max(order_date) FROM <<partition>>',
    '2 years'
);
```

### Step 3 — Drop: remove detached partitions after the cooling-off period

Once the `drop_detached` interval has passed since `detached_date` in `dba.detached_partitions`, partitions can be dropped. The maintenance script enforces a minimum safety window of 4 days and will not drop anything detached more recently than that.

Drop detached partitions for a specific table:

```sql
SELECT dba.partition_drop_detached_partition('public', 'orders', 'orders_20200101_20200201');
```

### Additional maintenance operations

**Change partition interval** — replace empty future partitions with ones of a new size, without touching the data partition:

```sql
SELECT dba.partition_change_range_on_partitioned_table(
    v_schema_name                               => 'public',
    v_table_name                                => 'orders',
    v_new_interval                              => '200000000',
    v_number_of_additional_partitions_to_create => 5,
    v_drop_detached_partitions                  => TRUE
);
```

**Realign boundaries** — when future partitions have drifted out of alignment with the intended grid, replace them with correctly aligned ones. A bridge partition absorbs the gap between the current data boundary and the first grid-aligned boundary. Always dry-run first.

```sql
-- Dry run (default): inspect the plan in the server logs
SELECT dba.partition_realign_boundaries('public', 'orders', 100000000);

-- Apply
SELECT dba.partition_realign_boundaries('public', 'orders', 100000000, v_dry_run => FALSE);

-- Realign to match a leader table's grid
SELECT dba.partition_realign_with_leader('public', 'order_items', 'orders', v_dry_run => FALSE);
```

**Add date constraints** — for tables partitioned on an integer column that are also queried by date, the maintenance script can add check constraints once a partition has data:

```sql
INSERT INTO dba.partition_configuration VALUES (
    'public', 'orders',
    '{"auto-maintenance": true, "date_constraint": {"marker": "order_date", "constraint_column": "order_date"}}'
);
```

**Lock-safe execution** — all detach and maintenance operations use `lock_safe_execute` internally. You can also use it directly for any DDL that must not block application queries indefinitely:

```sql
-- Execute a statement, retrying on lock timeout
CALL dba.lock_safe_execute('ALTER TABLE public.orders ADD COLUMN note text');

-- Restrict execution to a time window
CALL dba.lock_safe_execute(
    'my_maintenance_procedure',
    v_time_start => TIME '02:00+00',
    v_time_end   => TIME '05:00+00'
);
```

> **Note:** the input runs under the caller's `search_path`, so schema-qualify the objects it references.

---

## Part 3 — Reverting partitioning

Reverting returns a partitioned table to its original unpartitioned state. It is safe to execute as long as all non-mammoth partitions are still empty, i.e. no data has yet crossed the boundary of the mammoth partition.

### Step 1 — Dry run

Always start with a dry run. It logs the full revert plan, including which partitions will be detached and how indexes and constraints will be renamed, without making any changes.

```sql
CALL dba.partition_table_revert('public', 'orders');
```

### Step 2 — Apply the revert inside a transaction

The revert runs inside a transaction so you can review the result before committing. Under the lock it:

1. Verifies all non-mammoth partitions are empty.
2. Drops foreign keys in other tables that reference the partitioned table.
3. Detaches all non-mammoth partitions and records them in `dba.detached_partitions`.
4. Detaches the mammoth partition.
5. Renames indexes and constraints on the mammoth back to their original names.
6. Renames the partitioned shell to `<table>_partitioned_retired`.
7. Renames the mammoth to the original table name.
8. Re-adds the dropped foreign keys against the original table name, without a full table scan.

```sql
BEGIN;
CALL dba.partition_table_revert('public', 'orders', v_dry_run => FALSE);
-- Inspect the result, then:
COMMIT;
```

### Step 3 — Cleanup

After committing, call `partition_table_revert_cleanup` to drop the detached non-mammoth partitions and the retired shell, and remove the row from `dba.partition_configuration`:

```sql
CALL dba.partition_table_revert_cleanup('public', 'orders');
```

---

## Contributing

We strongly encourage you to contribute to our repository. Find out more in our [contribution guidelines](https://github.com/Adyen/.github/blob/master/CONTRIBUTING.md).

## Support

If you have a feature request, or spotted a bug or a technical problem, create a GitHub issue.

## License

MIT license. For more information, see the LICENSE file.

---

## Thank you

This library is the result of years of work by a great team. A big thank you to everyone who contributed:

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
