"""HTML rendering helpers for the dashboard."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Iterable

from .config import resolve_database_config
from .sections import (
    DECIMAL_2_COLUMNS,
    DECIMAL_3_COLUMNS,
    PERCENT_COLUMNS,
    SIGNED_COLUMNS,
    ResolvedSectionConfig,
)

_STATIC_DIR = Path(__file__).resolve().parent / "static"


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def sort_value(column: str, value: Any) -> str:
    if value is None:
        return ""
    if column in PERCENT_COLUMNS:
        return f"{float(value) * 100.0:.8f}"
    if isinstance(value, (int, float)):
        return f"{float(value):.8f}"
    return str(value)


def format_value(column: str, value: Any) -> str:
    if value is None:
        return "—"
    if column == "rank":
        return str(value)
    if column in PERCENT_COLUMNS:
        return f"{float(value) * 100.0:+.2f}%"
    if column in DECIMAL_2_COLUMNS:
        return f"{float(value):.2f}"
    if column in DECIMAL_3_COLUMNS:
        return f"{float(value):.3f}"
    return str(value)


def value_class(column: str, value: Any) -> str:
    if value is None or column not in SIGNED_COLUMNS.union(PERCENT_COLUMNS):
        return ""
    number = float(value)
    if number > 0:
        return " etf-report__value--pos"
    if number < 0:
        return " etf-report__value--neg"
    return ""


def render_cell(column: str, row: dict[str, Any]) -> str:
    if column == "symbol":
        return (
            f"<td data-sort-value='{escape(sort_value(column, row['symbol']))}'><span class='etf-report__symbol'>{escape(row['symbol'])}</span>"
            f"<span class='etf-report__name'>{escape(row.get('asset_class', ''))}</span></td>"
        )
    if column == "display_name":
        return f"<td data-sort-value='{escape(sort_value(column, row[column]))}'>{escape(row[column])}</td>"
    classes = "etf-report__value" + value_class(column, row.get(column))
    return (
        f"<td class='{classes.strip()}' data-sort-value='{escape(sort_value(column, row.get(column)))}'>"
        f"{escape(format_value(column, row.get(column)))}</td>"
    )


def render_table_section(section: ResolvedSectionConfig, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return (
            "<div class='etf-report__card'>"
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
            f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
            "<div class='etf-report__empty'>No qualifying rows for the selected date.</div>"
            "</div>"
        )

    as_of_date = rows[0]["date"]
    market_regime = rows[0].get("market_regime")
    header_html = "".join(
        f"<th data-sort-index='{idx}' data-sort-direction=''>{escape(section.column_labels.get(col, col))}<span class='etf-report__sort-indicator'></span></th>"
        for idx, col in enumerate(section.columns)
    )
    body_rows = []
    for row in rows:
        cell_html = "".join(render_cell(col, row) for col in section.columns)
        body_rows.append(f"<tr>{cell_html}</tr>")

    badges = [
        f"<span class='etf-report__badge'>As of {escape(as_of_date)}</span>",
        f"<span class='etf-report__badge'>{len(rows)} rows</span>",
    ]
    if market_regime:
        badges.insert(1, f"<span class='etf-report__badge'>Market Regime: {escape(market_regime)}</span>")

    return f"""
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">{escape(section.title)}</h2>
        <p class="etf-report__card-desc">{escape(section.description)}</p>
        <div class="etf-report__card-meta">{''.join(badges)}</div>
      </div>
      <div class="etf-report__table-wrap">
        <table class="etf-report__table">
          <thead><tr>{header_html}</tr></thead>
          <tbody>{''.join(body_rows)}</tbody>
        </table>
      </div>
    </div>
    """


def render_section(section: ResolvedSectionConfig, data: list[dict[str, Any]]) -> str:
    if section.type == "table":
        return render_table_section(section, data)
    return (
        "<div class='etf-report__card'>"
        f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
        f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
        "<div class='etf-report__empty'>Section type not implemented yet.</div>"
        "</div>"
    )


def render_fragment(
    site_config: dict[str, Any],
    sections_config: Iterable[ResolvedSectionConfig],
    selected_date: str | None,
    db_config: dict[str, Any] | None = None,
) -> str:
    date_value = escape(selected_date or "")
    sections_json = escape(
        json.dumps(
            [
                {
                    "key": section.key,
                    "title": section.title,
                    "description": section.description,
                    "columns": list(section.columns),
                    "column_labels": section.column_labels,
                }
                for section in sections_config
            ]
        )
    )
    loading_sections = "".join(
        (
            "<div class='etf-report__card'>"
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
            f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
            "<div class='etf-report__empty'>Loading data…</div>"
            "</div>"
        )
        for section in sections_config
    )

    return f"""
<link rel="stylesheet" href="/static/dashboard.css">
<section class="etf-report" data-etf-report data-sections='{sections_json}' data-initial-date="{date_value}">
  <div class="etf-report__utility-bar">
    <div class="etf-report__utility-label">Operations</div>
    <div class="etf-report__utility-links">
      <a class="etf-report__utility-link" href="/pull_stats">Pull Stats</a>
      <a class="etf-report__utility-link" href="/config">Config</a>
    </div>
  </div>
  <div class="etf-report__hero">
    <div>
      <div class="etf-report__eyebrow">{escape(site_config.get('eyebrow', 'Momentum Snapshot'))}</div>
      <h1 class="etf-report__title">{escape(site_config.get('title', 'ETF Ranking Dashboard'))}</h1>
      <p class="etf-report__subtitle">{escape(site_config.get('subtitle', ''))}</p>
    </div>
    <div class="etf-report__meta">
      {f'<span class="etf-report__env-badge etf-report__env-badge--db">{escape((db_config or {}).get("dbname", ""))}</span>' if (db_config or {}).get("dbname") else ""}
      <span class="etf-report__env-badge etf-report__env-badge--{escape(site_config.get('environment', 'dev'))}">{escape(site_config.get('environment', 'dev'))}</span>
      <form class="etf-report__filter-form" method="get" data-etf-filter-form>
        <input class="etf-report__date-input" type="date" name="date" value="{date_value}" onchange="this.form.submit()">
      </form>
    </div>
  </div>
  <div class="etf-report__summary" data-etf-summary>
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">Macro Summary</h2>
        <p class="etf-report__card-desc">Compact macro regime, leadership, bond internals, and top bond momentum.</p>
      </div>
      <div class="etf-report__empty">Loading data…</div>
    </div>
  </div>
  <div class="etf-report__grid" data-etf-grid>
    {loading_sections}
  </div>
  <div class="etf-report__analysis" data-etf-analysis>
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">Analysis</h2>
        <p class="etf-report__card-desc">Notes derived from the etf_analysis table.</p>
      </div>
      <div class="etf-report__empty">Loading analysis…</div>
    </div>
  </div>
</section>
<script src="/static/dashboard.js"></script>
""".strip()


def render_document(fragment: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
</head>
<body style="margin:0; padding:24px; background:#030712;">
{fragment}
</body>
</html>
"""


def build_page(
    config: dict[str, Any],
    resolved_sections: Iterable[ResolvedSectionConfig],
    report_date: str | None,
) -> str:
    fragment = render_fragment(config.get("site", {}), resolved_sections, report_date, resolve_database_config(config))
    return render_document(fragment, config.get("site", {}).get("title", "ETF Ranking Dashboard"))
