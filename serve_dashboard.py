#!/usr/bin/env python3
"""Serve ETF report sections dynamically from Postgres."""

from __future__ import annotations

from socketserver import ThreadingMixIn
from typing import Any
from urllib.parse import parse_qs, urlparse
from wsgiref.simple_server import WSGIServer, make_server

from dashboard.config import load_config
from dashboard.routes import (
    route_api_config_get,
    route_api_config_section_columns,
    route_api_config_update,
    route_api_latest_date,
    route_api_section,
    route_api_sections,
    route_config_page,
    route_healthz,
    route_index,
    route_pull_stats_page,
    route_pull_stats_stream,
    route_static_file,
)


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def app(environ: dict[str, Any], start_response: Any) -> Any:
    parsed = urlparse(environ.get("PATH_INFO", "/"))
    path = parsed.path

    if path == "/healthz":
        return route_healthz(start_response)

    if path.startswith("/static/"):
        return route_static_file(path, start_response)

    if path == "/pull_stats":
        return route_pull_stats_page(start_response)

    if path == "/config":
        return route_config_page(start_response)

    if path == "/api/config/section/columns":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        return route_api_config_section_columns(query, start_response)

    if path == "/api/config/section":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        method = environ.get("REQUEST_METHOD", "GET").upper()
        if method == "POST":
            return route_api_config_update(environ, start_response)
        return route_api_config_get(query, start_response)

    if path == "/pull_stats/stream":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        return route_pull_stats_stream(query, start_response)

    if path == "/api/latest-date":
        return route_api_latest_date(start_response)

    if path == "/api/sections":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        return route_api_sections(query, start_response)

    if path == "/api/section":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        return route_api_section(query, start_response)

    query = parse_qs(environ.get("QUERY_STRING", ""))
    return route_index(query, start_response)


def main() -> int:
    config = load_config()
    server_config = config.get("server", {})
    host = server_config.get("host", "127.0.0.1")
    port = int(server_config.get("port", 8000))
    with make_server(host, port, app, server_class=ThreadingWSGIServer) as httpd:
        print(f"Serving ETF site on http://{host}:{port}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
