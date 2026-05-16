# Unused SQL Objects Catalog

Every object defined in `sql/` (excluding `DDLs/`) — cataloged as **used** (queried directly by `serve_dashboard.py` or as a transitive SQL dependency of something that is) or **unused** (not reachable from the dashboard).

## Legend

| Status | Meaning |
|--------|---------|
| **DIRECT** | Queried directly by `serve_dashboard.py` / Python `dashboard/` code |
| **DEP** | SQL dependency of a DIRECT object (transitive closure) |
| **REFRESHED** | Refreshed by `refresh_etf_matviews()` but not queried by the dashboard. Exists because of the refresh-all function. |
| **UNUSED** | Not queried by the dashboard and not a dependency of anything that is. Deployed in the database but dead code from the dashboard's perspective. |
| **UTILITY** | DML script, not a persistent schema object |

---

## `sql/etf_ranking_views.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `etf_metadata` | TABLE | **DEP** | Upstream of `vw_etf_daily_features` (external) |
| `etf_ranking_config` | TABLE | **DEP** | Joined in `vw_etf_daily_scores` |
| `vw_etf_prices` | VIEW | **DEP** | Thin wrapper → feeds external `vw_etf_daily_features` |
| `vw_etf_daily_scores` | VIEW | **DEP** | Backing view for `mv_etf_daily_scores` |
| `vw_market_regime` | VIEW | **DEP** | Backing view for `mv_market_regime` |

## `sql/macro_signal_views.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `vw_macro_cluster_momentum` | VIEW | **UNUSED** | Upstream of `vw_macro_signal_dashboard`, but that dashboard view is also unused by the dashboard |
| `vw_macro_bond_treasury_buckets` | VIEW | **UNUSED** | Upstream of `vw_macro_bond_treasury_summary` |
| `vw_macro_bond_treasury_summary` | VIEW | **UNUSED** | Upstream of `mv_macro_bond_treasury_summary` which is only used by `mv_etf_report_bond_credit_performance` (UNUSED) |
| `vw_macro_ratio_signals` | VIEW | **DEP** | Joined in `vw_macro_signal_table` → `mv_macro_signal_table` (DIRECT) |
| `vw_macro_signal_dashboard` | VIEW | **UNUSED** | Upstream of `mv_macro_signal_dashboard` — not queried by dashboard |

## `sql/macro_signal_table.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `vw_macro_signal_table` | VIEW | **DEP** | Backing view for `mv_macro_signal_table` |

## `sql/materialized_views.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `mv_etf_daily_scores` | MATVIEW | **DEP** | Base for all 5 report mv's |
| `mv_market_regime` | MATVIEW | **DEP** | Joined by all 5 report mv's |
| `mv_macro_signal_dashboard` | MATVIEW | **REFRESHED** | Refreshed but only queried by `mv_etf_report_bond_credit_performance` which is UNUSED |
| `mv_macro_bond_treasury_summary` | MATVIEW | **REFRESHED** | Same as above |
| `mv_macro_signal_table` | MATVIEW | **DIRECT** | Queried by `fetch_macro_summary()` |
| `mv_etf_report_momentum_longs` | MATVIEW | **DIRECT** | Section `momentum_longs` |
| `mv_etf_report_momentum_shorts` | MATVIEW | **DIRECT** | Section `momentum_shorts` |
| `mv_etf_report_oversold_mean_reversion` | MATVIEW | **DIRECT** | Section `oversold_mean_reversion` |
| `mv_etf_report_overbought_mean_reversion` | MATVIEW | **DIRECT** | Section `overbought_mean_reversion` |
| `mv_etf_report_range_compression` | MATVIEW | **DIRECT** | Section `range_compression` |
| `mv_etf_report_bond_credit_performance` | MATVIEW | **REFRESHED** | Refreshed but not registered in `sections.py` — not in the dashboard |
| `refresh_etf_matviews()` | FUNCTION | **UTILITY** | Called externally (cron/data pipeline), not by the dashboard |

## `sql/config_app.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `config.etf_dashboard_section_config` | TABLE | **DIRECT** | Column labels per section |

## `sql/sync_etf_reference_tables.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| (all) | UTILITY | **UTILITY** | DML script for syncing CSV → tables; defines temp tables only |

## `sql/fix_missing_universe_metadata.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| (all) | UTILITY | **UTILITY** | One-off seed data inserts |

---

## Summary

| Category | Count | Objects |
|----------|-------|---------|
| **DIRECT** | 7 | `mv_etf_report_momentum_longs`, `mv_etf_report_momentum_shorts`, `mv_etf_report_oversold_mean_reversion`, `mv_etf_report_overbought_mean_reversion`, `mv_etf_report_range_compression`, `mv_macro_signal_table`, `config.etf_dashboard_section_config` |
| **DEP** | 6 | `etf_metadata`, `etf_ranking_config`, `vw_etf_prices`, `vw_etf_daily_scores`, `vw_market_regime`, `vw_macro_ratio_signals`, `vw_macro_signal_table`, `mv_etf_daily_scores`, `mv_market_regime` |
| **REFRESHED** | 3 | `mv_macro_signal_dashboard`, `mv_macro_bond_treasury_summary`, `mv_etf_report_bond_credit_performance` |
| **UNUSED** | 4 | `vw_macro_cluster_momentum`, `vw_macro_bond_treasury_buckets`, `vw_macro_bond_treasury_summary`, `vw_macro_signal_dashboard` |
| **UTILITY** | 3 | `sync_etf_reference_tables.sql`, `fix_missing_universe_metadata.sql`, `refresh_etf_matviews()` |

### Why these exist despite being unused

These remaining objects are upstream dependencies of the REFRESHED matviews `mv_macro_signal_dashboard` and `mv_macro_bond_treasury_summary`:

- **`vw_macro_signal_dashboard`** is a richer regime view that feeds `mv_etf_report_bond_credit_performance` (also REFRESHED-only). The dashboard uses the simpler `mv_macro_signal_table` instead.
- **`vw_macro_cluster_momentum`**, **`vw_macro_bond_treasury_buckets`**, **`vw_macro_bond_treasury_summary`** are upstream dependencies of the above.
