#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import tomllib

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR.parent / "config.toml"


DISCREPANCY_QUERIES = {
    "flows_not_in_universe": """
        WITH flows_symbols AS (
            SELECT DISTINCT etf AS symbol
            FROM public.etf_flows
        ),
        universe_symbols AS (
            SELECT etf AS symbol
            FROM public.etf_universe
            WHERE active = TRUE
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM flows_symbols
            EXCEPT
            SELECT symbol FROM universe_symbols
        ) AS diff
        ORDER BY symbol
    """,
    "universe_not_in_flows": """
        WITH flows_symbols AS (
            SELECT DISTINCT etf AS symbol
            FROM public.etf_flows
        ),
        universe_symbols AS (
            SELECT etf AS symbol
            FROM public.etf_universe
            WHERE active = TRUE
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM universe_symbols
            EXCEPT
            SELECT symbol FROM flows_symbols
        ) AS diff
        ORDER BY symbol
    """,
    "flows_not_in_metadata": """
        WITH flows_symbols AS (
            SELECT DISTINCT etf AS symbol
            FROM public.etf_flows
        ),
        metadata_symbols AS (
            SELECT symbol
            FROM public.etf_metadata
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM flows_symbols
            EXCEPT
            SELECT symbol FROM metadata_symbols
        ) AS diff
        ORDER BY symbol
    """,
    "universe_not_in_metadata": """
        WITH universe_symbols AS (
            SELECT etf AS symbol
            FROM public.etf_universe
            WHERE active = TRUE
        ),
        metadata_symbols AS (
            SELECT symbol
            FROM public.etf_metadata
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM universe_symbols
            EXCEPT
            SELECT symbol FROM metadata_symbols
        ) AS diff
        ORDER BY symbol
    """,
    "metadata_not_in_universe": """
        WITH universe_symbols AS (
            SELECT etf AS symbol
            FROM public.etf_universe
            WHERE active = TRUE
        ),
        metadata_symbols AS (
            SELECT symbol
            FROM public.etf_metadata
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM metadata_symbols
            EXCEPT
            SELECT symbol FROM universe_symbols
        ) AS diff
        ORDER BY symbol
    """,
    "metadata_not_in_flows": """
        WITH flows_symbols AS (
            SELECT DISTINCT etf AS symbol
            FROM public.etf_flows
        ),
        metadata_symbols AS (
            SELECT symbol
            FROM public.etf_metadata
        )
        SELECT symbol
        FROM (
            SELECT symbol FROM metadata_symbols
            EXCEPT
            SELECT symbol FROM flows_symbols
        ) AS diff
        ORDER BY symbol
    """,
}


SYMBOL_FLOW_DATES_QUERY = """
    SELECT
        u.etf AS symbol,
        min(f.date) AS min_date,
        max(f.date) AS max_date,
        count(f.*) AS row_count
    FROM public.etf_universe u
    LEFT JOIN public.etf_flows f
        ON f.etf = u.etf
    WHERE u.active = TRUE
      AND (%(symbol)s IS NULL OR u.etf = %(symbol)s)
    GROUP BY u.etf
    ORDER BY u.etf
"""


METADATA_REQUIRED_COLUMNS = (
    "display_name",
    "asset_class",
    "theme_type",
    "sector",
    "industry",
    "region",
    "country",
    "style",
    "commodity_group",
    "duration_bucket",
    "credit_bucket",
    "risk_bucket",
    "benchmark_group",
    "benchmark_symbol",
    "is_macro_reference",
)


@dataclass(frozen=True)
class GapsResult:
    checked_at: str
    db_target: str
    summary: dict[str, int]
    details: dict[str, list[str]]


@dataclass(frozen=True)
class SymbolRow:
    symbol: str
    min_date: str | None
    max_date: str | None
    row_count: int


@dataclass(frozen=True)
class SymbolResult:
    checked_at: str
    db_target: str
    symbol_filter: str | None
    rows: list[SymbolRow]


@dataclass(frozen=True)
class MetadataRow:
    symbol: str
    missing_columns: list[str]


@dataclass(frozen=True)
class MetadataResult:
    checked_at: str
    db_target: str
    rows: list[MetadataRow]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check symbol consistency across etf_flows, etf_universe, and etf_metadata."
    )
    parser.add_argument(
        "mode",
        choices=("gaps", "symbol", "metadata"),
        help="Check mode. 'gaps' runs reference consistency checks. 'symbol' shows flow date coverage. 'metadata' finds metadata rows with missing values.",
    )
    parser.add_argument(
        "symbol",
        nargs="?",
        help="Symbol to inspect in symbol mode, or ALL for all active symbols.",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to config.toml. Defaults to the repo config.toml.",
    )
    parser.add_argument(
        "--db-target",
        default="dev",
        help="Database target from config.toml, such as dev or prod. Default: dev.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Default: text.",
    )
    args = parser.parse_args()
    if args.mode in {"gaps", "metadata"} and args.symbol:
        parser.error(f"{args.mode} mode does not accept a symbol argument.")
    if args.mode == "symbol" and not args.symbol:
        parser.error("symbol mode requires SYMBOL or ALL.")
    return args


def load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.is_absolute():
        candidate = SCRIPT_DIR / config_path
        if candidate.exists():
            config_path = candidate
    with config_path.open("rb") as config_file:
        return tomllib.load(config_file)


def resolve_database_config(config: dict[str, Any], target: str) -> dict[str, Any]:
    database_config = config.get("database", {})
    if not isinstance(database_config, dict) or not database_config:
        raise RuntimeError("Missing [database] configuration.")

    if any(key in database_config for key in ("host", "dbname", "user", "username", "service")):
        return database_config

    if target not in database_config:
        available_targets = ", ".join(sorted(database_config))
        raise RuntimeError(f"Unknown database target '{target}'. Available targets: {available_targets}")

    selected = database_config[target]
    if not isinstance(selected, dict) or not selected:
        raise RuntimeError(f"Database config for target '{target}' is empty or invalid.")
    return selected


def build_connection_kwargs(config: dict[str, Any], target: str) -> dict[str, Any]:
    db_config = dict(resolve_database_config(config, target))
    if "username" in db_config and "user" not in db_config:
        db_config["user"] = db_config["username"]

    allowed_keys = ("dbname", "host", "port", "user", "password", "service")
    connect_kwargs = {key: value for key, value in db_config.items() if key in allowed_keys and value not in (None, "")}
    if not connect_kwargs:
        raise RuntimeError(f"No usable database connection settings found for target '{target}'.")
    return connect_kwargs


def fetch_symbol_list(conn: psycopg2.extensions.connection, query: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(query)
        return [str(row[0]) for row in cur.fetchall()]


def fetch_symbol_flow_dates(conn: psycopg2.extensions.connection, symbol: str | None) -> list[SymbolRow]:
    with conn.cursor() as cur:
        cur.execute(SYMBOL_FLOW_DATES_QUERY, {"symbol": symbol})
        return [
            SymbolRow(
                symbol=str(symbol_value),
                min_date=min_date.isoformat() if min_date is not None else None,
                max_date=max_date.isoformat() if max_date is not None else None,
                row_count=int(row_count),
            )
            for symbol_value, min_date, max_date, row_count in cur.fetchall()
        ]


def fetch_metadata_gaps(conn: psycopg2.extensions.connection) -> list[MetadataRow]:
    query = f"""
        SELECT
            symbol,
            ARRAY_REMOVE(
                ARRAY[
                    {", ".join(
                        [
                            f"CASE WHEN {column} IS NULL OR {column} = '' THEN '{column}' END"
                            if column != "is_macro_reference"
                            else f"CASE WHEN {column} IS NULL THEN '{column}' END"
                            for column in METADATA_REQUIRED_COLUMNS
                        ]
                    )}
                ],
                NULL
            ) AS missing_columns
        FROM public.etf_metadata
        WHERE ARRAY_LENGTH(
            ARRAY_REMOVE(
                ARRAY[
                    {", ".join(
                        [
                            f"CASE WHEN {column} IS NULL OR {column} = '' THEN '{column}' END"
                            if column != "is_macro_reference"
                            else f"CASE WHEN {column} IS NULL THEN '{column}' END"
                            for column in METADATA_REQUIRED_COLUMNS
                        ]
                    )}
                ],
                NULL
            ),
            1
        ) IS NOT NULL
        ORDER BY symbol
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [
            MetadataRow(
                symbol=str(symbol),
                missing_columns=[str(column) for column in missing_columns],
            )
            for symbol, missing_columns in cur.fetchall()
        ]


def run_gaps_check(conn: psycopg2.extensions.connection, db_target: str) -> GapsResult:
    details = {name: fetch_symbol_list(conn, query) for name, query in DISCREPANCY_QUERIES.items()}
    summary = {name: len(symbols) for name, symbols in details.items()}
    checked_at = datetime.now(timezone.utc).isoformat()
    return GapsResult(
        checked_at=checked_at,
        db_target=db_target,
        summary=summary,
        details=details,
    )


def run_symbol_check(conn: psycopg2.extensions.connection, db_target: str, symbol: str | None) -> SymbolResult:
    checked_at = datetime.now(timezone.utc).isoformat()
    rows = fetch_symbol_flow_dates(conn, symbol)
    return SymbolResult(
        checked_at=checked_at,
        db_target=db_target,
        symbol_filter=symbol,
        rows=rows,
    )


def run_metadata_check(conn: psycopg2.extensions.connection, db_target: str) -> MetadataResult:
    checked_at = datetime.now(timezone.utc).isoformat()
    rows = fetch_metadata_gaps(conn)
    return MetadataResult(
        checked_at=checked_at,
        db_target=db_target,
        rows=rows,
    )


def build_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def format_row(row: list[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    table_lines = [format_row(headers), separator]
    table_lines.extend(format_row(row) for row in rows)
    return "\n".join(table_lines)


def render_gaps_text(result: GapsResult) -> str:
    lines = [
        f"Checked at: {result.checked_at}",
        f"DB target: {result.db_target}",
        "",
        "Summary:",
    ]

    summary_rows = [[name, str(count)] for name, count in result.summary.items()]
    lines.append(build_table(["check", "count"], summary_rows))

    for name, symbols in result.details.items():
        lines.append("")
        lines.append(name)
        if symbols:
            lines.append(build_table(["symbol"], [[symbol] for symbol in symbols]))
        else:
            lines.append("none")

    return "\n".join(lines)


def render_symbol_text(result: SymbolResult) -> str:
    lines = [
        f"Checked at: {result.checked_at}",
        f"DB target: {result.db_target}",
        f"Filter: {result.symbol_filter or 'all active symbols'}",
        "",
    ]
    rows = [
        [
            row.symbol,
            row.min_date or "missing",
            row.max_date or "missing",
            str(row.row_count),
        ]
        for row in result.rows
    ]
    if rows:
        lines.append(build_table(["symbol", "min_date", "max_date", "row_count"], rows))
    else:
        lines.append("No matching symbols found.")
    return "\n".join(lines)


def render_metadata_text(result: MetadataResult) -> str:
    lines = [
        f"Checked at: {result.checked_at}",
        f"DB target: {result.db_target}",
        "",
    ]
    rows = [[row.symbol, ", ".join(row.missing_columns)] for row in result.rows]
    if rows:
        lines.append(build_table(["symbol", "missing_columns"], rows))
    else:
        lines.append("No metadata gaps found.")
    return "\n".join(lines)


def render_gaps_json(result: GapsResult) -> str:
    payload = {
        "checked_at": result.checked_at,
        "db_target": result.db_target,
        "summary": result.summary,
        "details": result.details,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def render_symbol_json(result: SymbolResult) -> str:
    payload = {
        "checked_at": result.checked_at,
        "db_target": result.db_target,
        "symbol_filter": result.symbol_filter,
        "rows": [
            {
                "symbol": row.symbol,
                "min_date": row.min_date,
                "max_date": row.max_date,
                "row_count": row.row_count,
            }
            for row in result.rows
        ],
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def render_metadata_json(result: MetadataResult) -> str:
    payload = {
        "checked_at": result.checked_at,
        "db_target": result.db_target,
        "rows": [
            {
                "symbol": row.symbol,
                "missing_columns": row.missing_columns,
            }
            for row in result.rows
        ],
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def main() -> int:
    args = parse_args()

    try:
        config = load_config(args.config)
        connect_kwargs = build_connection_kwargs(config, args.db_target)
        with psycopg2.connect(**connect_kwargs) as conn:
            if args.mode == "gaps":
                result = run_gaps_check(conn, args.db_target)
            elif args.mode == "symbol":
                symbol_filter = None if args.symbol and args.symbol.upper() == "ALL" else args.symbol
                result = run_symbol_check(conn, args.db_target, symbol_filter)
            else:
                result = run_metadata_check(conn, args.db_target)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.mode == "gaps" and args.format == "json":
        print(render_gaps_json(result))
    elif args.mode == "gaps":
        print(render_gaps_text(result))
    elif args.mode == "symbol" and args.format == "json":
        print(render_symbol_json(result))
    elif args.mode == "symbol":
        print(render_symbol_text(result))
    elif args.format == "json":
        print(render_metadata_json(result))
    else:
        print(render_metadata_text(result))

    if args.mode == "gaps" and any(count > 0 for count in result.summary.values()):
        return 1
    if args.mode == "metadata" and result.rows:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
