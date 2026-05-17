# AGENTS.md

This directory is for a quant momentum dashboard to assist with trading.


# Structure

## Repository Layout

  - `serve_dashboard.py`: WSGI entrypoint for the dashboard application.
  - `dashboard/`: dashboard application code and static frontend assets.
  - `sql/`: database schema and SQL support files.
  - `docs/`: generated human-readable HTML documentation. Output only; not a source of agent instructions.
  - `instructions/`: task-specific instruction files for LLM agents. Read only when explicitly directed.
  - `helpers/`: maintenance and support scripts.



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
