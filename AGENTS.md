# AGENTS.md

This directory is for a quant momentum dashboard to assist with trading.
The backbone of this structure is the database table `etf_flows`, which contains
daily OHLCV data for a universe of ETFs defined in `etf_universe`.

# Name

qDash, Quant Dashboard.

# Rules

 - Small logical changes only.
 - All changes must be presented with a DIFF and reviewed for approval.
 - 


---

# Project Architecture
- Backend: Python 3.12 (`venv` at `.venv/`; always use `.venv/bin/python3`)
  - `serve_dashboard.py`: thin WSGI entrypoint — app router + `main()` only.
    - Run: `.venv/bin/python3 serve_dashboard.py`
- Database: PostgreSQL
  - DEV: `host=192.168.50.237 dbname=financials_dev`
  - PROD: `host=192.168.50.5 dbname=financials`
  - Connect via psycopg2 (credentials in `config.toml`)
- Config: `config.toml` (see `config.example.toml` for structure)
  - `[database.dev]` / `[database.prod]` — `host`, `dbname`, `user`

---

### Refresh materialized views

After any data pull (e.g., `pull_stats.py`), materialized views must be refreshed
so the dashboard shows current data. A single PL/pgSQL function handles this:

```sql
SELECT refresh_etf_matviews();
```

Definition: `sql/materialized_views.sql` (search for
`CREATE OR REPLACE FUNCTION public.refresh_etf_matviews`).



