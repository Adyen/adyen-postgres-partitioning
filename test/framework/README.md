# Partition framework tests

Unit tests for the functions in `sql/functions`. Every `test_*.sql` file defines one or more tests as functions or
procedures in the `dba_test` schema. They use the assertion helpers from `test_helpers.sql` and record `PASS`, `FAIL` or
`SKIP` per check.

These tests are separate from the smoke test in `test/` (`tables.sql`, `run_functions.sql`, `configuration.sql`,
`cleanup.sql`).

## Prerequisites

- A PostgreSQL server you can create databases on. The tests were written for PostgreSQL 13 and later.
- `psql` on your `PATH`.
- Connection settings. By default the runner uses `localhost`, port `5432` and your OS user (`$USER`). Override them
  through the environment or in `test_config.env`:
  - `DB_HOST`
  - `DB_PORT`
  - `DB_USER`

  `test_config.env` is read, not sourced. It may only contain `DB_HOST=value`, `DB_PORT=value` and `DB_USER=value`
  lines (and comments). Environment variables take precedence over the file.
- A local server. The runner refuses a `DB_HOST` other than `localhost`, `127.0.0.1`, `::1` or a socket directory,
  unless `PARTITION_TESTS_ALLOW_REMOTE=1` is set.
- A superuser. Some tests modify system catalogs directly, and the two `partition_report_table_free_extends_below_*`
  tests use `COPY ... TO/FROM` a randomly named file in `/tmp`, which is emptied after it is read.

## Run all tests

```bash
./test/framework/run_partition_tests.sh
```

## Run a single test

```bash
./test/framework/run_partition_tests.sh test_partition_calculate_free_partitions
```

## What the runner does

1. Drops and creates the database `partition_framework_test`. The runner marks the database with a comment and refuses
   to drop an existing `partition_framework_test` that does not carry that comment.
2. Loads `test/setup_schema.sql` and all functions through `sql/functions/create_all_functions.sql`.
3. Loads `test_helpers.sql` and every `test_*.sql` file.
4. Runs all tests and prints a result table and a summary.
5. Drops the database when all tests pass. When a test fails the database is kept so you can inspect it.

## Notes

- The tests for `partition_detach_partitions`, `partition_detach_partitions_without_uuidv7`,
  `partition_query_based_detach_partitions`, `partition_query_based_maintenance_detach_partitions`,
  `partition_maintenance`, `partition_table_revert` and `partition_table_revert_fk` use transaction control. The runner
  executes them separately from `dba_test.run_all_tests()`.
- UUIDv7 tests always run, because the library ships `dba.uuid_v7_to_timestamptz` and `dba.uuid_timestamptz_to_v7`.
- `test_security_hardening.sql` checks that quotes in partition names cannot break out of generated statements, that `search_path` is pinned, and that the UUIDv7 helpers match PostgreSQL 18's built-in functions.
- Some checks are skipped by design. They show up as `SKIP` in the results.
