#!/usr/bin/env bash
set -euo pipefail

# Single entry point for running the partition framework tests.
# It creates a fresh test database, loads the schema and all functions, loads the tests, runs
# them, prints a summary table, and drops the database on success (keeps it on failure).
#
# Usage:
#   ./test/framework/run_partition_tests.sh                   # run all tests
#   ./test/framework/run_partition_tests.sh <test_name>       # run a single test file, e.g. test_partition_table

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$BASE_DIR/../.." && pwd)"
CONFIG_FILE="$BASE_DIR/test_config.env"

die() {
  echo "run_partition_tests.sh: $*" >&2
  exit 2
}

# The config file is parsed, not sourced, so it cannot run shell code. Only DB_HOST, DB_PORT and
# DB_USER are accepted, and they do not override values already set in the environment.
if [[ -f "$CONFIG_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*(#.*)?$ ]] && continue
    if [[ "$line" =~ ^(DB_HOST|DB_PORT|DB_USER)=([A-Za-z0-9_./:-]*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      if [[ -z "${!key:-}" ]]; then
        printf -v "$key" '%s' "${BASH_REMATCH[2]}"
      fi
    else
      die "unsupported line in $CONFIG_FILE: $line"
    fi
  done <"$CONFIG_FILE"
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-$USER}"
DB_NAME="partition_framework_test"
DB_MARKER="partition framework scratch database"

[[ "$DB_HOST" =~ ^(/[A-Za-z0-9_./-]*|[A-Za-z0-9.:-]+)$ ]] || die "invalid DB_HOST: $DB_HOST"
[[ "$DB_PORT" =~ ^[0-9]+$ ]] || die "invalid DB_PORT: $DB_PORT"
[[ "$DB_USER" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid DB_USER: $DB_USER"

# The tests drop and create a database and modify catalogs, so a remote server needs an explicit opt-in.
case "$DB_HOST" in
  localhost | 127.0.0.1 | ::1 | /*) ;;
  *)
    [[ "${PARTITION_TESTS_ALLOW_REMOTE:-}" == "1" ]] ||
      die "refusing to run against non-local host '$DB_HOST'; set PARTITION_TESTS_ALLOW_REMOTE=1 to allow it"
    ;;
esac

# Optional argument to run a single test by its base name (without .sql).
TEST_NAME="${1:-}"
[[ -z "$TEST_NAME" || "$TEST_NAME" =~ ^[A-Za-z0-9_]+$ ]] || die "invalid test name: $TEST_NAME"

# Base psql command, configured to stop on errors, avoid paging and ignore ~/.psqlrc.
PSQL_BASE=(psql -X -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -v ON_ERROR_STOP=1 -P pager=off)

# Only drop a database that this runner created, recognised by its comment.
existing="$("${PSQL_BASE[@]}" -d postgres -Atc "SELECT 'exists:' || coalesce(shobj_description(oid, 'pg_database'), '') FROM pg_database WHERE datname = '$DB_NAME';")"
if [[ -n "$existing" && "$existing" != "exists:$DB_MARKER" ]]; then
  die "database $DB_NAME exists but was not created by this runner; drop it manually if it is safe to do so"
fi

# Always start from a clean database to avoid interference from previous runs.
"${PSQL_BASE[@]}" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" >/dev/null
"${PSQL_BASE[@]}" -d postgres -c "CREATE DATABASE \"$DB_NAME\";" >/dev/null
"${PSQL_BASE[@]}" -d postgres -c "COMMENT ON DATABASE \"$DB_NAME\" IS '$DB_MARKER';" >/dev/null

# Create the schema and the configuration tables, then load all framework functions.
# The function files are loaded through create_all_functions.sql, which uses paths relative to the repository root.
cd "$REPO_ROOT"
"${PSQL_BASE[@]}" -d "$DB_NAME" -q -f test/setup_schema.sql >/dev/null
"${PSQL_BASE[@]}" -d "$DB_NAME" -q -f sql/functions/create_all_functions.sql >/dev/null

# Load the test framework helpers, followed by all test files.
"${PSQL_BASE[@]}" -d "$DB_NAME" -q -f "$BASE_DIR/test_helpers.sql" >/dev/null

for file in "$BASE_DIR"/test_*.sql; do
  [[ "$(basename "$file")" == "test_helpers.sql" ]] && continue
  "${PSQL_BASE[@]}" -d "$DB_NAME" -q -f "$file" >/dev/null
done

# Normal tests are executed via dba_test.run_all_tests() or dba_test.run_test().
# Some procedures perform transaction control (COMMIT/ROLLBACK). These must be executed
# outside of run_all_tests() to avoid nested procedure transaction errors.
if [[ -n "$TEST_NAME" ]]; then
  case "$TEST_NAME" in
    test_partition_query_based_maintenance_detach_partitions)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.query_based_maintenance_detach_partitions_exec();"
      ;;
    test_partition_query_based_detach_partitions)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.query_based_detach_partitions_exec();"
      ;;
    test_partition_detach_partitions_without_uuidv7)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.detach_partitions_without_uuidv7_exec();"
      ;;
    test_partition_detach_partitions)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.run_test('test_partition_detach_partitions');"
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.detach_partitions_date_exec();"
      ;;
    test_partition_maintenance)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_maintenance_exec();"
      ;;
    test_partition_table_revert)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_table_revert_exec();"
      ;;
    test_partition_table_revert_fk)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_table_revert_fk_exec();"
      ;;
    security_quote_in_partition_name_exec)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.security_quote_in_partition_name_exec();"
      ;;
    *)
      "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.run_test('$TEST_NAME');"
      ;;
  esac
else
  # Run all standard tests first.
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.run_all_tests();"

  # Execute transaction-control procedures separately (outside run_all_tests).
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.query_based_maintenance_detach_partitions_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.query_based_detach_partitions_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.detach_partitions_without_uuidv7_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.detach_partitions_date_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_maintenance_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_table_revert_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.partition_table_revert_fk_exec();"
  "${PSQL_BASE[@]}" -d "$DB_NAME" -c "CALL dba_test.security_quote_in_partition_name_exec();"
fi

# Print a human-readable results table.
"${PSQL_BASE[@]}" -d "$DB_NAME" -c "SELECT * FROM dba_test.test_results ORDER BY test_name;"

# Summarize totals.
IFS='|' read -r fail_count pass_count skip_count <<<"$("${PSQL_BASE[@]}" -d "$DB_NAME" -Atc "SELECT count(*) FILTER (WHERE result = 'FAIL'), count(*) FILTER (WHERE result = 'PASS'), count(*) FILTER (WHERE result = 'SKIP') FROM dba_test.test_results;")"

echo "Results: ${pass_count:-0} passed, ${fail_count:-0} failed, ${skip_count:-0} skipped"
echo "============================================================"
echo "FINAL TEST STATUS: ${fail_count:-0} FAILED TEST(S)"
echo "============================================================"

# If everything passed, drop the test database. Otherwise keep it for inspection.
if [[ "${fail_count:-0}" -eq 0 ]]; then
  "${PSQL_BASE[@]}" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" >/dev/null
  exit 0
fi

echo "Kept database $DB_NAME for inspection. Drop it with:"
echo "  psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c 'DROP DATABASE \"$DB_NAME\";'"
exit 1
