#!/usr/bin/env bash
set -euo pipefail

PROD_HOST="csi-laptop"
PROD_DB="financials"
PROD_USER=""
SOURCE_DSN=""
REMOTE_TMP_ROOT="/tmp"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  ./sync_etf_reference_tables.sh --source-dsn "postgresql://..." [options]

Options:
  --source-dsn DSN     Connection string for the source Postgres database.
  --prod-host HOST     SSH host for prod. Default: csi-laptop
  --prod-db DB         Prod database name. Default: financials
  --prod-user USER     Prod database user. Optional.
  --remote-tmp DIR     Remote temp parent directory. Default: /tmp
  --dry-run            Print commands without executing them.
  -h, --help           Show this help.
EOF
}

run_cmd() {
  printf '$'
  for arg in "$@"; do
    printf ' %q' "$arg"
  done
  printf '\n'

  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi

  "$@"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-dsn)
      SOURCE_DSN="$2"
      shift 2
      ;;
    --prod-host)
      PROD_HOST="$2"
      shift 2
      ;;
    --prod-db)
      PROD_DB="$2"
      shift 2
      ;;
    --prod-user)
      PROD_USER="$2"
      shift 2
      ;;
    --remote-tmp)
      REMOTE_TMP_ROOT="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$SOURCE_DSN" ]]; then
  echo "--source-dsn is required" >&2
  usage >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/sql/sync_etf_reference_tables.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 1
fi

LOCAL_TMP_DIR="$(mktemp -d)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REMOTE_TMP_DIR="${REMOTE_TMP_ROOT%/}/etf_sync_${TIMESTAMP}"
METADATA_CSV="$LOCAL_TMP_DIR/etf_metadata.csv"
UNIVERSE_CSV="$LOCAL_TMP_DIR/etf_universe.csv"
REMOTE_SQL_FILE="$REMOTE_TMP_DIR/sync_etf_reference_tables.sql"
REMOTE_METADATA_CSV="$REMOTE_TMP_DIR/etf_metadata.csv"
REMOTE_UNIVERSE_CSV="$REMOTE_TMP_DIR/etf_universe.csv"

cleanup() {
  rm -rf "$LOCAL_TMP_DIR"
}
trap cleanup EXIT

SOURCE_METADATA_SQL="\\copy (
select
  symbol,
  display_name,
  asset_class,
  theme_type,
  sector,
  industry,
  region,
  country,
  style,
  commodity_group,
  duration_bucket,
  credit_bucket,
  risk_bucket,
  benchmark_group,
  benchmark_symbol,
  is_macro_reference
from public.etf_metadata
order by symbol
) to '$METADATA_CSV' with (format csv, header true)"

SOURCE_UNIVERSE_SQL="\\copy (
select
  etf,
  active
from public.etf_universe
order by etf
) to '$UNIVERSE_CSV' with (format csv, header true)"

echo "Exporting source tables to $LOCAL_TMP_DIR"
run_cmd psql "$SOURCE_DSN" -v ON_ERROR_STOP=1 -c "$SOURCE_METADATA_SQL"
run_cmd psql "$SOURCE_DSN" -v ON_ERROR_STOP=1 -c "$SOURCE_UNIVERSE_SQL"

run_cmd ssh "$PROD_HOST" "mkdir -p '$REMOTE_TMP_DIR'"
run_cmd scp "$SQL_FILE" "$METADATA_CSV" "$UNIVERSE_CSV" "${PROD_HOST}:$REMOTE_TMP_DIR/"

PSQL_REMOTE_CMD=(psql -d "$PROD_DB")
if [[ -n "$PROD_USER" ]]; then
  PSQL_REMOTE_CMD+=(-U "$PROD_USER")
fi
PSQL_REMOTE_CMD+=(
  -v ON_ERROR_STOP=1
  -v "metadata_csv=$REMOTE_METADATA_CSV"
  -v "universe_csv=$REMOTE_UNIVERSE_CSV"
  -f "$REMOTE_SQL_FILE"
)

REMOTE_CMD=""
for arg in "${PSQL_REMOTE_CMD[@]}"; do
  REMOTE_CMD+=" $(printf '%q' "$arg")"
done
REMOTE_CMD="${REMOTE_CMD# }"

echo "Applying upserts on $PROD_HOST:$PROD_DB"
run_cmd ssh "$PROD_HOST" "$REMOTE_CMD"

echo "Cleaning up remote temp files"
run_cmd ssh "$PROD_HOST" "rm -rf '$REMOTE_TMP_DIR'"

echo "Sync complete"
