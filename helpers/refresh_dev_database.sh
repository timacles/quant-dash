#!/usr/bin/env bash
set -euo pipefail

# Refreshes dev from prod using a staging DB on 192.168.50.237.
# Flow: rebuild _fin_stage, restore prod dump, archive current dev, promote stage, verify data.

SOURCE_DSN="host=192.168.50.5 dbname=financials"
TARGET_ADMIN_DSN="host=192.168.50.237 dbname=postgres"
STAGE_DSN="host=192.168.50.237 dbname=_fin_stage"
DEV_DSN="host=192.168.50.237 dbname=financials_dev"
OLD_DB_NAME="_fin_old_$(date +%m%d%y)"

run_sql() {
  local sql="$1"
  psql "$TARGET_ADMIN_DSN" -c "$sql"
}

# Build fresh staging database.
run_sql 'DROP DATABASE IF EXISTS _fin_stage;'
run_sql 'CREATE DATABASE _fin_stage;'

# Restore prod into staging.
pg_dump "$SOURCE_DSN" | psql "$STAGE_DSN"

# Archive current dev and promote staging.
run_sql "DROP DATABASE IF EXISTS ${OLD_DB_NAME};"
run_sql "ALTER DATABASE financials_dev RENAME TO ${OLD_DB_NAME};"
run_sql 'ALTER DATABASE _fin_stage RENAME TO financials_dev;'

# Sanity check loaded data.
psql "$DEV_DSN" -c 'select max(date) as max_etf_flows_date from etf_flows;'

echo "--- refresh complete ---"
