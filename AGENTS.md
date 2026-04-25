 AGENTS.md

This directory is for a quant momentum dashboard to assist with trading.
The backbone of this structure is the database table `etf_flows`, which contains
daily OHLCV data for a universe of ETFs defined in `etf_universe`.

## Name

qDash, Quant Dashboard.

---

## Project Architecture
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
  - `[database.dev]` / `[database.prod]` — `host`, `dbname`, `user`
  - `[site]` — `title`, `eyebrow`, `subtitle`, `environment` (`dev` or `prod`)

Structure:
- Data ingest -> Database -> Dashboard

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


