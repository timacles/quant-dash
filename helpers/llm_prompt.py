#!/usr/bin/env python3
"""Build an LLM prompt from an instructions file and live database data.

Usage:
    .venv/bin/python3 helpers/llm_prompt.py path/to/instructions.md

Reads the instructions file, then appends the latest data from the prod
database views vw_json_etf_reports and vw_json_macro_signal_table, printing
the assembled prompt to stdout.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

import psycopg2


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR.parent / "config.toml"

JSON_VIEWS = (
    "public.vw_json_etf_reports",
    "public.vw_json_macro_signal_table",
)

VIEW_LABELS = {
    "public.vw_json_etf_reports": "ETF Reports",
    "public.vw_json_macro_signal_table": "Macro Signal Table",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an LLM prompt from an instructions file and live database data."
    )
    parser.add_argument("instructions", help="Path to the instructions file (e.g. instructions/analysis.md)")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to config.toml. Defaults to the repo config.toml.",
    )
    return parser.parse_args()


def load_config(path: str) -> dict:
    config_path = Path(path)
    if not config_path.is_absolute():
        candidate = SCRIPT_DIR / config_path
        if candidate.exists():
            config_path = candidate
    with config_path.open("rb") as f:
        return tomllib.load(f)


def read_instructions(path: str) -> str:
    path_obj = Path(path)
    if not path_obj.exists():
        print(f"Error: instructions file not found: {path}", file=sys.stderr)
        sys.exit(1)
    return path_obj.read_text(encoding="utf-8")


def fetch_json_view(conn: psycopg2.extensions.connection, view_name: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT data FROM {view_name}")
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return None


def fmt_report_date(section: dict | None) -> str:
    if section and "report_date" in section:
        return str(section["report_date"])
    return "unknown"


def main() -> int:
    args = parse_args()

    # Read instructions
    instructions = read_instructions(args.instructions)

    # Load config & connect to prod
    try:
        config = load_config(args.config)
        db_config = config.get("database", {}).get("prod")
        if not db_config:
            print("Error: [database.prod] not found in config.toml", file=sys.stderr)
            return 1
        connect_kwargs = {k: v for k, v in db_config.items() if k in ("dbname", "host", "port", "user", "password")}
        conn = psycopg2.connect(**connect_kwargs)
    except Exception as exc:
        print(f"Error connecting to database: {exc}", file=sys.stderr)
        return 1

    # Fetch data
    sections = {}
    with conn:
        for view in JSON_VIEWS:
            data = fetch_json_view(conn, view)
            sections[view] = data
    conn.close()

    # --- Assemble prompt ---
    lines = []

    # Instructions
    lines.append("# Instructions\n")
    lines.append(instructions.strip())
    lines.append("")

    # Data section
    lines.append("# Current Data\n")
    lines.append("Below is the latest data from the system to use for your analysis.\n")

    for view in JSON_VIEWS:
        label = VIEW_LABELS.get(view, view)
        data = sections.get(view)
        if data is None:
            lines.append(f"## {label}\n")
            lines.append("*No data available.*\n")
            continue

        report_date = fmt_report_date(data)
        lines.append(f"## {label}  ({report_date})\n")
        lines.append("```json")
        lines.append(json.dumps(data, indent=2))
        lines.append("```\n")

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
