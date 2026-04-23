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
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
            f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
            "<div class='etf-report__empty'>Loading data…</div>"
            "</div>"
        )
        for section in sections
    )
    toc_links = "".join(
        f"<a class='etf-report__toc-link' href='#section-{escape(section.key)}'>{escape(section.title)}</a>"
        for section in sections
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
  <nav class="etf-report__toc" aria-label="Table of contents">
    <div class="etf-report__toc-label">Sections</div>
    <div class="etf-report__toc-links">
      {toc_links}
    </div>
  </nav>
  <div class="etf-report__summary" data-etf-summary>
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">Macro Signal Table</h2>
        <p class="etf-report__card-desc">Cross-asset macro signals grouped by category with change, DMA, and interpretation columns.</p>
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
