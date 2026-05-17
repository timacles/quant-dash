"""HTML rendering helpers for the dashboard."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Iterable

from .config import resolve_database_config
from .sections import ResolvedSectionConfig

_STATIC_DIR = Path(__file__).resolve().parent / "static"


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def render_fragment(
    site_config: dict[str, Any],
    sections_config: Iterable[ResolvedSectionConfig],
    selected_date: str | None,
    db_config: dict[str, Any] | None = None,
) -> str:
    sections = list(sections_config)
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
                for section in sections
            ]
        )
    )
    loading_sections = "".join(
        (
            f"<div class='etf-report__card' id='section-{escape(section.key)}'>"
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2></div>"
            "<div class='etf-report__empty'>Loading data…</div>"
            "</div>"
        )
        for section in sections
    )
    section_links = "".join(
        f"<a class='etf-report__utility-link' href='#section-{escape(section.key)}'>{escape(section.title)}</a>"
        for section in sections
    )

    return f"""
<link rel="stylesheet" href="/static/dashboard.css">
<section class="etf-report" data-etf-report data-sections='{sections_json}' data-initial-date="{date_value}">
  <div class="etf-report__utility-bar">
    <div class="etf-report__utility-links">
      <a class="etf-report__utility-link" href="/macro-signals">Macro Signals</a>
      <span class="etf-report__utility-divider" aria-hidden="true"></span>
      {section_links}
      <span class="etf-report__utility-divider" aria-hidden="true"></span>
      <a class="etf-report__utility-link" href="/pull_stats">Pull Stats</a>
      <a class="etf-report__utility-link" href="/config">Config</a>
    </div>
  </div>
  <div class="etf-report__hero">
    <div>
      <h1 class="etf-report__title">{escape(site_config.get('title', 'ETF Ranking Dashboard'))}</h1>
    </div>
  </div>
  <div class="etf-report__status-bar">
    <span class="etf-report__status-time" data-status-time>--</span>
    <span class="etf-report__status-sep" aria-hidden="true">·</span>
    <span class="etf-report__status-item">
      <span class="etf-report__status-key">DB</span>
      <span class="etf-report__status-value">{escape((db_config or {}).get("dbname", "") or "--")}</span>
    </span>
    <span class="etf-report__status-sep" aria-hidden="true">·</span>
    <span class="etf-report__status-item">
      <span class="etf-report__status-key">ENV</span>
      <span class="etf-report__status-value etf-report__status-value--{escape(site_config.get('environment', 'dev'))}">{escape(site_config.get('environment', 'dev'))}</span>
    </span>
    <span class="etf-report__status-sep" aria-hidden="true">·</span>
    <form class="etf-report__filter-form" method="get" data-etf-filter-form>
      <input class="etf-report__date-input" type="date" name="date" value="{date_value}" onchange="this.form.submit()">
    </form>
  </div>
  <div class="etf-report__grid" data-etf-grid>
    {loading_sections}
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
  <link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
</head>
<body>
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
