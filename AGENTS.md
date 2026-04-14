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
  - pull_stats.py: pulls data from an API and runs daily.
- Backend: python
  - serve_dashboard.py: main dashboard site.
    - serves server-side HTML.
    - keep it simple, modular, and extendable for new features.
- Database: PostgreSQL
  - CLI: `psql`
  - DEV:
    - host: 192.168.50.237
    - dbname: financials_dev
  - PROD:
    - host: 192.168.50.5
    - dbname: financials
- Schema Definitions
  - `sql/` directory
  - contains DDLs and views which implement a system of features and signals
  - main tables: `etf_flows, etf_universe, etf_metadata`
    - `etf_flows`: base OHLCV data
- Config: `config.toml`

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

---

## Data Contracts
- `etf_flows`
  - Base daily OHLCV table.
  - Grain: one row per `symbol, trade_date`.
  - Expected fields include open, high, low, close, volume.
  - Used as the primary source for downstream signals, features, and dashboard views.
- `etf_universe`
  - Defines the ETF universe covered by the system.
  - Should include the active symbols expected to appear in `etf_flows`.
  - Universe should span market sectors, industries, and bonds/treasuries.
- `etf_metadata`
  - Stores descriptive attributes for ETFs keyed by `symbol`.
  - Used to enrich dashboard views and joins against `etf_flows` and `etf_universe`.
- Join conventions
  - Use `symbol` as the primary join key across `etf_flows`, `etf_universe`, and `etf_metadata`.
  - Assume `trade_date` is the daily time key for market data in `etf_flows`.

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
- Prefer simple, readable tests
- Verify the specific area changed before finishing

---

## Verification
- For SQL changes:
  - run the affected query or view in DEV
  - inspect sample rows for correctness
- For ingest changes:
  - test a limited symbol set or date range before broader runs
  - confirm inserted or updated data matches expected fields and grain
- For dashboard changes:
  - confirm `serve_dashboard.py` still loads correctly
  - confirm the server-rendered HTML returns expected sections and data
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
