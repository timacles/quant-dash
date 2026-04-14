#!/usr/bin/env python3

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
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.toml"


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


LATEST_FLOW_DATES_QUERY = """
    SELECT
        u.etf AS symbol,
        max(f.date) AS latest_date,
        count(f.*) AS row_count
    FROM public.etf_universe u
    LEFT JOIN public.etf_flows f
        ON f.etf = u.etf
    WHERE u.active = TRUE
    GROUP BY u.etf
    ORDER BY u.etf
"""


@dataclass(frozen=True)
class CheckResult:
    checked_at: str
    db_target: str
    summary: dict[str, int]
    details: dict[str, list[str]]
    latest_flow_dates: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check symbol consistency across etf_flows, etf_universe, and etf_metadata."
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
    return parser.parse_args()


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


def fetch_latest_flow_dates(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(LATEST_FLOW_DATES_QUERY)
        rows = []
        for symbol, latest_date, row_count in cur.fetchall():
            rows.append(
                {
                    "symbol": str(symbol),
                    "latest_date": latest_date.isoformat() if latest_date is not None else None,
                    "row_count": int(row_count),
                }
            )
        return rows


def run_checks(conn: psycopg2.extensions.connection, db_target: str) -> CheckResult:
    details = {name: fetch_symbol_list(conn, query) for name, query in DISCREPANCY_QUERIES.items()}
    summary = {name: len(symbols) for name, symbols in details.items()}
    latest_flow_dates = fetch_latest_flow_dates(conn)
    checked_at = datetime.now(timezone.utc).isoformat()
    return CheckResult(
        checked_at=checked_at,
        db_target=db_target,
        summary=summary,
        details=details,
        latest_flow_dates=latest_flow_dates,
    )


def render_text(result: CheckResult) -> str:
    lines = [
        f"Checked at: {result.checked_at}",
        f"DB target: {result.db_target}",
        "",
        "Summary:",
    ]

    for name, count in result.summary.items():
        lines.append(f"  {name}: {count}")

    for name, symbols in result.details.items():
        lines.append("")
        lines.append(f"{name}:")
        if symbols:
            lines.extend(f"  - {symbol}" for symbol in symbols)
        else:
            lines.append("  - none")

    lines.append("")
    lines.append("Latest flow dates for active universe symbols:")
    for row in result.latest_flow_dates:
        latest_date = row["latest_date"] or "missing"
        lines.append(f"  - {row['symbol']}: latest_date={latest_date}, row_count={row['row_count']}")

    return "\n".join(lines)


def render_json(result: CheckResult) -> str:
    payload = {
        "checked_at": result.checked_at,
        "db_target": result.db_target,
        "summary": result.summary,
        "details": result.details,
        "latest_flow_dates": result.latest_flow_dates,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def main() -> int:
    args = parse_args()

    try:
        config = load_config(args.config)
        connect_kwargs = build_connection_kwargs(config, args.db_target)
        with psycopg2.connect(**connect_kwargs) as conn:
            result = run_checks(conn, args.db_target)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(render_json(result))
    else:
        print(render_text(result))

    if any(count > 0 for count in result.summary.values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
