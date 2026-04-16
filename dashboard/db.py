"""Database query helpers for the dashboard."""

from __future__ import annotations

import json
from typing import Any, Iterable

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from .sections import (
    DEFAULT_SECTION_LIMIT,
    SECTIONS,
    SectionConfig,
    SectionDisplayConfig,
    ResolvedSectionConfig,
)


def serialize_date(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def parse_limit(raw_value: str | None) -> int:
    try:
        limit = int(raw_value) if raw_value is not None else DEFAULT_SECTION_LIMIT
    except (TypeError, ValueError):
        return DEFAULT_SECTION_LIMIT
    return limit if limit > 0 else DEFAULT_SECTION_LIMIT


def get_section(section_key: str) -> SectionConfig | None:
    return next((section for section in SECTIONS if section.key == section_key), None)


def _parse_json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Expected a JSON object for dashboard column labels")


def fetch_section_display_configs(
    conn: psycopg2.extensions.connection,
    sections: Iterable[SectionConfig],
) -> dict[str, SectionDisplayConfig]:
    section_list = tuple(sections)
    section_keys = [section.key for section in section_list if section.type == "table"]
    if not section_keys:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT section_key, columns, column_labels
            FROM config.etf_dashboard_section_config
            WHERE section_key = ANY(%s)
            """,
            [section_keys],
        )
        config_rows = {
            str(section_key): SectionDisplayConfig(
                columns=tuple(columns or ()),
                column_labels={str(key): str(value) for key, value in _parse_json_object(column_labels).items()},
            )
            for section_key, columns, column_labels in cur.fetchall()
        }

    view_names = [section.source for section in section_list if section.type == "table"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
            """,
            [view_names],
        )
        available_columns: dict[str, set[str]] = {}
        for table_name, column_name in cur.fetchall():
            available_columns.setdefault(str(table_name), set()).add(str(column_name))

    missing_sections = [section.key for section in section_list if section.type == "table" and section.key not in config_rows]
    if missing_sections:
        raise ValueError(f"Missing dashboard config for section(s): {', '.join(sorted(missing_sections))}")

    for section in section_list:
        if section.type != "table":
            continue
        display_config = config_rows[section.key]
        if not display_config.columns:
            raise ValueError(f"Dashboard config for section '{section.key}' must include at least one column")

        valid_columns = available_columns.get(section.source, set())
        if not valid_columns:
            raise ValueError(f"Source view '{section.source}' for section '{section.key}' has no discoverable columns")

        invalid_columns = [column for column in display_config.columns if column not in valid_columns]
        if invalid_columns:
            raise ValueError(
                f"Dashboard config for section '{section.key}' references invalid column(s): {', '.join(invalid_columns)}"
            )

        label_keys = set(display_config.column_labels)
        expected_keys = set(display_config.columns)
        if label_keys != expected_keys:
            missing_labels = sorted(expected_keys - label_keys)
            extra_labels = sorted(label_keys - expected_keys)
            problems: list[str] = []
            if missing_labels:
                problems.append(f"missing labels for {', '.join(missing_labels)}")
            if extra_labels:
                problems.append(f"extra labels for {', '.join(extra_labels)}")
            raise ValueError(f"Dashboard config for section '{section.key}' has invalid labels: {'; '.join(problems)}")

    return config_rows


def resolve_sections(
    sections: Iterable[SectionConfig],
    display_configs: dict[str, SectionDisplayConfig],
) -> tuple[ResolvedSectionConfig, ...]:
    resolved_sections = []
    for section in sections:
        display_config = display_configs.get(section.key)
        if section.type == "table" and display_config is None:
            raise ValueError(f"Missing dashboard display config for section '{section.key}'")
        resolved_sections.append(
            ResolvedSectionConfig(
                key=section.key,
                title=section.title,
                description=section.description,
                type=section.type,
                source=section.source,
                columns=display_config.columns if display_config else (),
                column_labels=display_config.column_labels if display_config else {},
            )
        )
    return tuple(resolved_sections)


def fetch_macro_summary(
    conn: psycopg2.extensions.connection,
    report_date: str | None,
) -> dict[str, Any]:
    try:
        date_sql = sql.SQL("%s") if report_date else sql.SQL("(SELECT max(date) FROM public.vw_macro_signal_dashboard)")
        query = sql.SQL(
            """
            SELECT *
            FROM public.vw_macro_signal_dashboard
            WHERE date = {date_sql}
            LIMIT 1
            """
        ).format(date_sql=date_sql)

        params: list[Any] = []
        if report_date:
            params.append(report_date)

        with conn.cursor() as cur:
            cur.execute(query, params)
            names = [desc[0] for desc in cur.description]
            macro_row = cur.fetchone()
            macro = dict(zip(names, macro_row)) if macro_row else None

        summary_date = serialize_date(macro["date"]) if macro and macro.get("date") else serialize_date(report_date)
        leaders: list[dict[str, Any]] = []
        if summary_date:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rank, symbol, display_name, bond_bucket, composite_score, date
                    FROM public.vw_etf_report_bond_credit_performance
                    WHERE date = %s
                    ORDER BY rank
                    LIMIT 3
                    """,
                    [summary_date],
                )
                names = [desc[0] for desc in cur.description]
                leaders = [dict(zip(names, row)) for row in cur.fetchall()]

        if macro and macro.get("date") is not None:
            macro["date"] = serialize_date(macro["date"])
        for row in leaders:
            if row.get("date") is not None:
                row["date"] = serialize_date(row["date"])

        return {
            "date": summary_date,
            "macro": macro,
            "bond_leaders": leaders,
        }
    except Exception:
        return {
            "date": serialize_date(report_date),
            "macro": None,
            "bond_leaders": [],
        }


def fetch_section_rows(
    conn: psycopg2.extensions.connection,
    section: SectionConfig,
    limit: int,
    report_date: str | None,
) -> list[dict[str, Any]]:
    if section.type != "table":
        return []

    date_sql = sql.SQL("%s") if report_date else sql.SQL("(SELECT max(date) FROM public.{view})").format(
        view=sql.Identifier(section.source)
    )
    query = sql.SQL(
        """
        SELECT *
        FROM public.{view}
        WHERE date = {date_sql}
        ORDER BY rank
        LIMIT %s
        """
    ).format(view=sql.Identifier(section.source), date_sql=date_sql)

    params: list[Any] = []
    if report_date:
        params.append(report_date)
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        names = [desc[0] for desc in cur.description]
        rows = [dict(zip(names, row)) for row in cur.fetchall()]
        for row in rows:
            if "date" in row:
                row["date"] = serialize_date(row["date"])
        return rows


def fetch_latest_report_date(conn: psycopg2.extensions.connection) -> Any:
    view_queries = [
        sql.SQL("SELECT max(date) AS latest_date FROM public.{view}").format(view=sql.Identifier(section.source))
        for section in SECTIONS
        if section.type == "table"
    ]
    if not view_queries:
        return None

    query = sql.SQL("SELECT max(latest_date) FROM ({views}) AS latest_dates").format(
        views=sql.SQL(" UNION ALL ").join(view_queries)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row[0] if row else None


def fetch_all_section_configs(
    conn: psycopg2.extensions.connection,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT section_key, columns, column_labels
            FROM config.etf_dashboard_section_config
            ORDER BY section_key
            """
        )
        return [
            {
                "section_key": section_key,
                "columns": list(columns or []),
                "column_labels": _parse_json_object(column_labels),
            }
            for section_key, columns, column_labels in cur.fetchall()
        ]


def update_section_config(
    conn: psycopg2.extensions.connection,
    section_key: str,
    columns: list[str],
    column_labels: dict[str, str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE config.etf_dashboard_section_config
            SET columns = %s,
                column_labels = %s::jsonb,
                updated_at = now()
            WHERE section_key = %s
            """,
            [columns, json.dumps(column_labels), section_key],
        )
        if cur.rowcount == 0:
            raise ValueError(f"No section found with key: {section_key!r}")
    conn.commit()


def fetch_section_all_columns(
    conn: psycopg2.extensions.connection,
    section_key: str,
) -> dict[str, Any]:
    section = next((s for s in SECTIONS if s.key == section_key), None)
    if section is None:
        raise ValueError(f"Unknown section: {section_key!r}")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            [section.source],
        )
        all_columns = [row[0] for row in cur.fetchall()]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT columns, column_labels
            FROM config.etf_dashboard_section_config
            WHERE section_key = %s
            """,
            [section_key],
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError(f"No config found for section: {section_key!r}")

    return {
        "section_key": section_key,
        "all_columns": all_columns,
        "active_columns": list(row[0] or []),
        "column_labels": _parse_json_object(row[1]),
    }


def fetch_analysis_row(
    conn: psycopg2.extensions.connection,
    report_date: str | None,
) -> dict[str, Any] | None:
    if report_date:
        query = """
            SELECT created, data
            FROM public.etf_analysis
            WHERE created::date = %s
            ORDER BY created DESC
            LIMIT 1
        """
        params = [report_date]
    else:
        query = """
            SELECT created, data
            FROM public.etf_analysis
            ORDER BY created DESC
            LIMIT 1
        """
        params = []

    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        if not row:
            return None

    created, data = row
    date_value = created.date().isoformat() if hasattr(created, "date") else serialize_date(created)
    return {
        "date": date_value,
        "created": serialize_date(created),
        "data": data,
    }
