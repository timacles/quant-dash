"""Per-route request handlers."""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs

import psycopg2

from .config import PULL_STATS_PATH, BASE_DIR, load_config, build_connection_kwargs
from .db import (
    fetch_all_section_configs,
    fetch_analysis_row,
    fetch_latest_report_date,
    fetch_macro_summary,
    fetch_section_all_columns,
    fetch_section_display_configs,
    fetch_section_rows,
    get_section,
    parse_limit,
    resolve_sections,
    serialize_date,
    update_section_config,
)
from .render import build_page
from .sections import SECTIONS

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_STATIC_DIR = Path(__file__).resolve().parent / "static"

_PULL_STATS_PAGE: str = (_TEMPLATES_DIR / "pull_stats.html").read_text(encoding="utf-8")
_CONFIG_PAGE: str = (_TEMPLATES_DIR / "config.html").read_text(encoding="utf-8")

_STATIC_MIME: dict[str, str] = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
}


def json_error(start_response: Any, status: str, message: str) -> list[bytes]:
    start_response(status, [("Content-Type", "application/json; charset=utf-8")])
    return [json.dumps({"error": message}).encode("utf-8")]


def route_healthz(start_response: Any) -> list[bytes]:
    start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"ok"]


def route_static_file(path: str, start_response: Any) -> list[bytes]:
    file_path = (_STATIC_DIR / Path(path).name).resolve()
    if not str(file_path).startswith(str(_STATIC_DIR)):
        start_response("403 Forbidden", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Forbidden"]
    if not file_path.exists():
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not found"]
    mime = _STATIC_MIME.get(file_path.suffix, "application/octet-stream")
    start_response("200 OK", [("Content-Type", mime), ("Cache-Control", "public, max-age=3600")])
    return [file_path.read_bytes()]


def route_pull_stats_page(start_response: Any) -> list[bytes]:
    start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
    return [_PULL_STATS_PAGE.encode("utf-8")]


def route_config_page(start_response: Any) -> list[bytes]:
    start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
    return [_CONFIG_PAGE.encode("utf-8")]


def route_pull_stats_stream(query: dict[str, list[str]], start_response: Any) -> Iterable[bytes]:
    start_response(
        "200 OK",
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Cache-Control", "no-cache"),
        ],
    )
    return _stream_pull_stats(query)


def route_api_latest_date(start_response: Any) -> list[bytes]:
    config = load_config()
    connect_kwargs = build_connection_kwargs(config)
    with psycopg2.connect(**connect_kwargs) as conn:
        latest_date = fetch_latest_report_date(conn)
    start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
    return [json.dumps({"date": serialize_date(latest_date)}).encode("utf-8")]


def route_api_sections(query: dict[str, list[str]], start_response: Any) -> list[bytes]:
    try:
        config = load_config()
        report_date = query.get("date", [None])[0]
        limit = parse_limit(query.get("limit", [None])[0])
        connect_kwargs = build_connection_kwargs(config)
        payload: dict[str, Any] = {
            "date": report_date,
            "as_of_date": report_date,
            "limit": limit,
            "limits": {},
            "macro_summary": None,
            "sections": {},
            "analysis": None,
        }
        with psycopg2.connect(**connect_kwargs) as conn:
            fetch_section_display_configs(conn, SECTIONS)
            payload["macro_summary"] = fetch_macro_summary(conn, report_date)
            for section in SECTIONS:
                rows = fetch_section_rows(conn, section, limit, report_date)
                payload["sections"][section.key] = rows
                payload["limits"][section.key] = limit
            payload["as_of_date"] = serialize_date(
                next((rows[0]["date"] for rows in payload["sections"].values() if rows), report_date)
            )
            if not payload["as_of_date"] and payload["macro_summary"]:
                payload["as_of_date"] = payload["macro_summary"].get("date")
            if not payload["date"]:
                payload["date"] = payload["as_of_date"]
            payload["analysis"] = fetch_analysis_row(conn, payload["date"])
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps(payload).encode("utf-8")]
    except Exception as exc:
        return json_error(start_response, "500 Internal Server Error", str(exc))


def route_api_section(query: dict[str, list[str]], start_response: Any) -> list[bytes]:
    try:
        config = load_config()
        report_date = query.get("date", [None])[0]
        section_key = query.get("key", [""])[0]
        limit = parse_limit(query.get("limit", [None])[0])
        section = get_section(section_key)
        if section is None:
            return json_error(start_response, "404 Not Found", "Unknown section")

        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            fetch_section_display_configs(conn, SECTIONS)
            rows = fetch_section_rows(conn, section, limit, report_date)
        as_of_date = serialize_date(rows[0]["date"]) if rows else report_date
        payload = {
            "key": section.key,
            "date": report_date or as_of_date,
            "as_of_date": as_of_date,
            "limit": limit,
            "rows": rows,
        }
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps(payload).encode("utf-8")]
    except Exception as exc:
        return json_error(start_response, "500 Internal Server Error", str(exc))


def route_api_config_get(query: dict[str, list[str]], start_response: Any) -> list[bytes]:
    try:
        config = load_config()
        connect_kwargs = build_connection_kwargs(config)
        section_key = query.get("key", [None])[0]
        with psycopg2.connect(**connect_kwargs) as conn:
            all_configs = fetch_all_section_configs(conn)
        if section_key:
            match = next((s for s in all_configs if s["section_key"] == section_key), None)
            if match is None:
                return json_error(start_response, "404 Not Found", f"Unknown section: {section_key!r}")
            payload = match
        else:
            payload = {"sections": all_configs}
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps(payload).encode("utf-8")]
    except Exception as exc:
        return json_error(start_response, "500 Internal Server Error", str(exc))


def route_api_config_update(environ: dict[str, Any], start_response: Any) -> list[bytes]:
    try:
        content_length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(content_length)
        data = json.loads(body)

        section_key = data.get("section_key", "")
        columns: list[str] = data.get("columns", [])
        column_labels: dict[str, str] = data.get("column_labels", {})

        if not section_key:
            return json_error(start_response, "400 Bad Request", "section_key is required")
        if not columns:
            return json_error(start_response, "400 Bad Request", "columns must be a non-empty list")
        if set(column_labels.keys()) != set(columns):
            return json_error(start_response, "400 Bad Request", "column_labels keys must exactly match columns")

        config = load_config()
        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            update_section_config(conn, section_key, columns, column_labels)

        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps({"ok": True}).encode("utf-8")]
    except ValueError as exc:
        return json_error(start_response, "400 Bad Request", str(exc))
    except Exception as exc:
        return json_error(start_response, "500 Internal Server Error", str(exc))


def route_api_config_section_columns(query: dict[str, list[str]], start_response: Any) -> list[bytes]:
    try:
        section_key = query.get("key", [None])[0]
        if not section_key:
            return json_error(start_response, "400 Bad Request", "key is required")
        config = load_config()
        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            payload = fetch_section_all_columns(conn, section_key)
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps(payload).encode("utf-8")]
    except ValueError as exc:
        return json_error(start_response, "404 Not Found", str(exc))
    except Exception as exc:
        return json_error(start_response, "500 Internal Server Error", str(exc))


def route_index(query: dict[str, list[str]], start_response: Any) -> list[bytes]:
    try:
        config = load_config()
        report_date = query.get("date", [None])[0]
        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            display_configs = fetch_section_display_configs(conn, SECTIONS)
        resolved_sections = resolve_sections(SECTIONS, display_configs)
        body = build_page(config, resolved_sections, report_date)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body.encode("utf-8")]
    except Exception as exc:
        start_response("500 Internal Server Error", [("Content-Type", "text/plain; charset=utf-8")])
        return [f"Failed to render ETF site: {exc}\n".encode("utf-8")]


def _build_pull_stats_command(query: dict[str, list[str]]) -> list[str]:
    command = [sys.executable, "-u", str(PULL_STATS_PATH)]

    symbol = query.get("symbol", [""])[0].strip()
    if symbol:
        command.extend(["--symbol", symbol])

    days_back = query.get("days_back", [""])[0].strip()
    if days_back:
        command.extend(["--days-back", days_back])

    db_target = query.get("db_target", [""])[0].strip()
    if db_target:
        command.extend(["--db-target", db_target])

    if query.get("print_data", [""])[0] in {"1", "true", "yes", "on"}:
        command.append("--print-data")

    if query.get("desc", [""])[0] in {"1", "true", "yes", "on"}:
        command.append("--desc")

    return command


def _stream_pull_stats(query: dict[str, list[str]]) -> Iterable[bytes]:
    command = _build_pull_stats_command(query)
    pretty_command = " ".join(shlex.quote(part) for part in command)
    yield f"$ {pretty_command}\n\n".encode("utf-8")

    if not PULL_STATS_PATH.exists():
        yield f"pull_stats.py not found at {PULL_STATS_PATH}\n".encode("utf-8")
        return

    process = subprocess.Popen(
        command,
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    try:
        for line in process.stdout:
            yield line.encode("utf-8", errors="replace")
    finally:
        process.stdout.close()

    return_code = process.wait()
    yield f"\n[exit code {return_code}]\n".encode("utf-8")
