#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
SCHEMA_FILE="$ROOT_DIR/report_schema.json"

INSTRUCTIONS_FILE=""
REPORT_FILE=""
OUTPUT_FILE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [--instructions PATH] [--report PATH] [--output PATH]

Options:
  --instructions PATH  Instructions markdown (default: $ROOT_DIR/instructions.md)
  --report PATH        Report JSON input (default: $ROOT_DIR/daily_report.json)
  --output PATH        Output JSON path (default: $ROOT_DIR/macro_analysis_TIMESTAMP.json)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instructions)
      INSTRUCTIONS_FILE="$2"
      shift 2
      ;;
    --report)
      REPORT_FILE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
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

INSTRUCTIONS_FILE="${INSTRUCTIONS_FILE:-$ROOT_DIR/instructions.md}"
REPORT_FILE="${REPORT_FILE:-$ROOT_DIR/daily_report.json}"
OUTPUT_FILE="${OUTPUT_FILE:-$ROOT_DIR/macro_analysis_${TIMESTAMP}.json}"

mkdir -p "$(dirname "$OUTPUT_FILE")"

cd "$ROOT_DIR"
/home/tim/.npm-global/lib/node_modules/@openai/codex/bin/codex.js exec --output-schema "$SCHEMA_FILE" -o "$OUTPUT_FILE" "$(bash generate_prompt.sh "$INSTRUCTIONS_FILE" "$REPORT_FILE")" --skip-git-repo-check -c 'reasoning_effort="xhigh"'

echo
echo "Saved Codex output to: $OUTPUT_FILE"
