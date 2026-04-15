# AGENTS.md

This directory is for a quant momentum dashboard to assist with trading.
The backbone of this structure is the database table `etf_flows`, which contains
daily OHLCV data for a universe of ETFs defined in `etf_universe`.
These ETFs should cover all industries of the market, including bonds and treasuries.

## Name

qDash, Quant Dashboard.

---

## Core Principles
- Prefer **simple, modular architecture**
- Avoid unnecessary abstraction
- Make **small, incremental changes**
- Always keep the repo in a **working state**

---

## Project Architecture
- Data ingest:
  - `pull_stats.py`: pulls OHLCV data from the Twelve Data API and upserts into `etf_flows`. Runs daily.
    - Flags: `--symbol SYMBOL`, `--days-back N` (default 7), `--db-target dev|prod`, `--print-data`, `--desc`
    - Run: `.venv/bin/python3 pull_stats.py --symbol SPY --days-back 30 --db-target dev`
- Backend: Python 3.12 (`venv` at `.venv/`; always use `.venv/bin/python3`)
  - `serve_dashboard.py`: thin WSGI entrypoint — app router + `main()` only.
    - Run: `.venv/bin/python3 serve_dashboard.py`
  - `dashboard/` package — all dashboard logic lives here:
    - `config.py` — `load_config`, `resolve_database_config`, `build_connection_kwargs`
    - `sections.py` — `SectionConfig` dataclasses, `SECTIONS` registry, column classification sets
    - `db.py` — all `fetch_*` functions, `update_section_config`, `serialize_date`, `parse_limit`, `get_section`
    - `render.py` — `render_*`, `format_*`, `value_*` helpers, `build_page`
    - `routes.py` — one handler per URL path; static file serving
    - `static/` — `dashboard.css`, `dashboard.js` (served at `/static/`)
    - `templates/` — `pull_stats.html`, `config.html`
- Database: PostgreSQL
  - DEV: `host=192.168.50.237 dbname=financials_dev`
  - PROD: `host=192.168.50.5 dbname=financials`
  - Connect via psycopg2 (credentials in `config.toml`)
- Schema Definitions
  - `sql/` directory — apply files manually via psql or psycopg2
    - `config_app.sql` — `config.etf_dashboard_section_config` DDL + seed data
    - `etf_ranking_views.sql` — `etf_metadata` DDL + all `vw_etf_report_*` views
    - `macro_signal_views.sql` — macro signal views (`vw_macro_signal_dashboard`, etc.)
    - `DDLs/` — base table seeds (`etf_universe`, `etf_metadata`) and legacy views
- Config: `config.toml` (see `config.example.toml` for structure)
  - `[twelvedata]` — `api_key`
  - `[database.dev]` / `[database.prod]` — `host`, `dbname`, `user`
  - `[server]` — `host`, `port`
  - `[site]` — `title`, `eyebrow`, `subtitle`, `environment` (`dev` or `prod`)
  - `[deploy.prod]` — deployment settings

Structure:
- Data ingest -> Database -> Dashboard

---

## Environment Safety
- Default all investigation and development work to DEV.
- Never touch PROD for writes, schema changes, backfills, or destructive actions.
- PROD may only be used to check schema status or inspect data when needed.
- Prefer read-only queries first when investigating data issues.

## Coding Standards
- Use clear, explicit naming
- No dead code or unused imports
- Keep functions small and composable
- All Python files use type hints throughout
- Every module starts with `from __future__ import annotations`

---

## Data Contracts
- `etf_flows`
  - Base daily OHLCV table. Grain: one row per `(etf, date)`.
  - Columns: `etf TEXT`, `date DATE`, `open`, `high`, `low`, `close`, `volume` (all `DOUBLE PRECISION`)
  - Unique constraint: `(date, etf)`
  - Note: the base column is `etf`; ranking views alias it as `symbol`
- `etf_universe`
  - Defines the active ETF universe. Grain: one row per `etf`.
  - Columns: `etf TEXT PRIMARY KEY`, `active BOOLEAN DEFAULT TRUE`
- `etf_metadata`
  - Descriptive attributes per ETF. Grain: one row per `symbol`.
  - Columns: `symbol TEXT PRIMARY KEY`, `display_name`, `asset_class`, `theme_type`, `sector`,
    `industry`, `region`, `country`, `style`, `commodity_group`, `duration_bucket`,
    `credit_bucket`, `risk_bucket`, `benchmark_group`, `benchmark_symbol`,
    `is_macro_reference BOOLEAN`
  - DDL source: `sql/etf_ranking_views.sql`
- `config.etf_dashboard_section_config`
  - Controls which columns each dashboard section displays. Grain: one row per `section_key`.
  - Columns: `section_key TEXT PRIMARY KEY`, `columns TEXT[]`, `column_labels JSONB`,
    `created_at TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`
  - `columns`: ordered list of column names to display
  - `column_labels`: JSON object mapping column name → header label (must match `columns` exactly)
  - DDL + seed data: `sql/config_app.sql`
- Join conventions
  - `symbol` (or `etf`) is the identifier key across tables. Use `etf` for raw table joins; use `symbol` when joining through views.
  - `date` is the daily time key for market data in `etf_flows` and all ranking views.

---

## Extension Recipes

### Add a new dashboard section
1. Add a `SectionConfig` entry to `SECTIONS` in `dashboard/sections.py`
2. Create the backing SQL view in `sql/` (follow the naming pattern `vw_etf_report_<name>.sql`) and apply it to DEV
3. Insert a row into `config.etf_dashboard_section_config`:
   ```sql
   INSERT INTO config.etf_dashboard_section_config (section_key, columns, column_labels)
   VALUES (
     'my_section',
     ARRAY['rank', 'symbol', 'display_name', ...],
     '{"rank":"Rank","symbol":"Ticker","display_name":"Name",...}'::jsonb
   );
   ```
   `columns` and `column_labels` must have the same keys. Source: `sql/config_app.sql` for examples.

### Add a new route
1. Add a handler function in `dashboard/routes.py`
2. Register it with one `if path == "..."` line in `app()` in `serve_dashboard.py`
3. For routes that support both GET and POST, dispatch on `environ.get("REQUEST_METHOD")` inside the `app()` block

### Edit section config via UI
- Navigate to `/config` in the browser
- Select a section from the dropdown
- Drag columns to reorder; edit label inputs inline
- Click **Save** — changes are written to `config.etf_dashboard_section_config` immediately
- API: `GET /api/config/section?key=<section_key>` · `POST /api/config/section` (JSON body: `{section_key, columns, column_labels}`)

### Change styles or client-side behaviour
- Edit `dashboard/static/dashboard.css` or `dashboard/static/dashboard.js` directly
- No Python changes needed

### Add a new column format
- Add the column name to the appropriate set in `dashboard/sections.py`
  (`PERCENT_COLUMNS`, `DECIMAL_2_COLUMNS`, `DECIMAL_3_COLUMNS`, `SIGNED_COLUMNS`)

---

## LLM Extension Guide

When using an LLM to extend this project, provide only the files relevant to the task:

| Task | Files to provide |
|------|-----------------|
| New section | `AGENTS.md` + `dashboard/sections.py` |
| New route | `AGENTS.md` + `dashboard/routes.py` + `serve_dashboard.py` |
| DB query | `AGENTS.md` + `dashboard/db.py` + relevant SQL view |
| Styling | `dashboard/static/dashboard.css` only |
| Full feature | `AGENTS.md` + affected modules |

Do **not** feed the entire package on every request — it adds noise without value.

---

## Workflow Rules
When given a task:

1. Understand the request
2. Check existing code before adding new files
3. Make minimal necessary changes
4. Ensure consistency with architecture
5. Run or suggest relevant commands
6. Prefer additive, low-risk changes over broad rewrites
7. Validate affected code, SQL, or pages before finishing

---

## Testing (lightweight)
- No test framework. Verification is done by running imports and spot-checking output.
- Verify the specific area changed before finishing:
  - Python: `.venv/bin/python3 -c "import serve_dashboard; from dashboard import config, db, render, routes, sections; print('OK')"`
  - SQL: run the affected view or query against DEV and inspect sample rows

---

## Verification
- For SQL changes:
  - Apply the file to DEV using psycopg2 or `psql -h 192.168.50.237 -d financials_dev -f sql/your_file.sql`
  - Run the affected view against DEV and inspect sample rows
- For ingest changes:
  - Test with a single symbol first: `.venv/bin/python3 pull_stats.py --symbol SPY --days-back 5 --db-target dev`
  - Confirm inserted rows match expected fields and grain in `etf_flows`
- For dashboard changes:
  - Confirm imports are clean: `.venv/bin/python3 -c "import serve_dashboard; print('OK')"`
  - Confirm the server starts: `.venv/bin/python3 serve_dashboard.py` (check `/healthz`)
- If full verification is not possible, state what was not run and why

---

## Anti-Patterns (Avoid)
- Overengineering
- Large, monolithic files
- Duplicated logic
- Breaking existing functionality

---

## When Unsure
- Ask for clarification OR
- Choose the simplest correct implementation

---

## Execution Mode Behavior
- In auto/full-auto: proceed without asking unless destructive
- In suggest mode: explain changes briefly

---

## Output Style
- Be concise
- No long explanations
- Focus on actionable changes
