# AGENTS.md

This directory is for a quant momentum dashboard to assist with trading.


# Structure

## Front End 

  - serve_dashboard.py 
  - `dashboard/` 

## Database Schema

  - `sql/` directory

## Other

  - `docs/` human readable documenetation stored in HTML files. Not for LLM consumption, output only.
  - `instructions/` various instruction files for LLM agents. This will be explicitly asked to be read.
  - `helpers/` scripts to assist with maintenance.

# Name

qDash, Quant Dashboard.

# Rules

 - Small logical changes only.
 - All changes must be presented with a DIFF and reviewed for approval.
 - use `git diff --` and `git status --short`  to present changes

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

---

### Refresh materialized views

After any data pull (e.g., `pull_stats.py`), materialized views must be refreshed
so the dashboard shows current data. A single PL/pgSQL function handles this:

```sql
SELECT refresh_etf_matviews();
```





