# AGENTS
This is a SQL directory for schema definitions.

## General Idea

Materialized Views -> Views ->  Source Tables

# Rules

 - DO NOT EVER make sql changes without requesting permission and presenting a DIFF for review.
 - Schema changes have to be applied via patch files, which will be reviewed. 
 - Once the patch is accepted by user, it can be requested to be ran on a target DB.
 - The schema change must be applied to the schema files in this directory, which hold the original definitions.


# Schema

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


- Schema Definitions
  - `sql/` directory — apply files manually via psql or psycopg2
    - `config_app.sql` — `config.etf_dashboard_section_config` DDL + seed data
    - `etf_ranking_views.sql` — `etf_metadata` DDL + all `vw_etf_report_*` views
    - `macro_signal_views.sql` — macro signal views (`vw_macro_signal_dashboard`, etc.)
    - `DDLs/` — base table seeds (`etf_universe`, `etf_metadata`) and legacy views
