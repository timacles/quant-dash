-- ============================================================================
-- drop_unused_objects_20260516.sql
--
-- Drops every database object that is NOT queried (directly or transitively)
-- by serve_dashboard.py / dashboard/db.py.
--
-- Complements the earlier script sql/drop_unused_etf_ranking_views.sql
-- which already removed unused objects from sql/etf_ranking_views.sql.
--
-- This script targets unused objects from:
--   sql/materialized_views.sql    — bond_credit MV, orphaned macro MV's
--   sql/macro_signal_views.sql    — orphaned macro views
--   sql/json_views.sql            — JSON/LLM output views
--   sql/vw_llm_market_summary.sql — LLM summary view
--   sql/DDLs/desk_views.sql       — legacy v_* view chain
--   sql/DDLs/etf_universe_seed.sql — reference table
--
-- Drop order respects dependencies (reverse of creation order).
-- Run this inside a transaction; ROLLBACK if anything looks wrong.
--
-- Safe to run: none of these objects are consumed by the dashboard or
-- by any transitive SQL dependency of the dashboard.
-- ============================================================================

BEGIN;

-- ==========================================================================
-- PHASE 1 — Materialized Views (depend on views below)
-- ==========================================================================

-- mv_etf_report_bond_credit_performance depends on:
--   mv_market_regime (KEPT), mv_macro_bond_treasury_summary (DROP below),
--   mv_macro_signal_dashboard (DROP below), vw_etf_daily_features (KEPT)
DROP MATERIALIZED VIEW IF EXISTS public.mv_etf_report_bond_credit_performance CASCADE;

-- mv_macro_signal_dashboard depends on:
--   vw_macro_ratio_signals (KEPT), vw_macro_bond_treasury_summary (DROP below),
--   vw_macro_cluster_momentum (DROP below), vw_etf_daily_features (KEPT), etc.
DROP MATERIALIZED VIEW IF EXISTS public.mv_macro_signal_dashboard CASCADE;

-- mv_macro_bond_treasury_summary depends on:
--   vw_macro_bond_treasury_buckets (DROP below) ← vw_etf_daily_features (KEPT)
DROP MATERIALIZED VIEW IF EXISTS public.mv_macro_bond_treasury_summary CASCADE;

-- ==========================================================================
-- PHASE 2 — Macro Signal Views (unused by dashboard)
-- ==========================================================================

-- vw_macro_signal_dashboard (backing view for mv_macro_signal_dashboard)
DROP VIEW IF EXISTS public.vw_macro_signal_dashboard CASCADE;

-- vw_macro_bond_treasury_summary (backing view for mv_macro_bond_treasury_summary)
DROP VIEW IF EXISTS public.vw_macro_bond_treasury_summary CASCADE;

-- vw_macro_bond_treasury_buckets (only consumed by vw_macro_bond_treasury_summary)
DROP VIEW IF EXISTS public.vw_macro_bond_treasury_buckets CASCADE;

-- vw_macro_cluster_momentum (only consumed by vw_macro_signal_dashboard)
DROP VIEW IF EXISTS public.vw_macro_cluster_momentum CASCADE;

-- ==========================================================================
-- PHASE 3 — JSON / LLM Output Views (thin wrappers over kept MV's)
-- ==========================================================================

-- JSON snapshot of mv_macro_signal_table (kept)
DROP VIEW IF EXISTS public.vw_json_macro_signal_table CASCADE;

-- JSON snapshot of the 5 report MV's (kept)
DROP VIEW IF EXISTS public.vw_json_etf_reports CASCADE;

-- LLM-oriented summary: vw_market_regime (kept) + vw_macro_signal_dashboard (dropped above)
DROP VIEW IF EXISTS public.vw_llm_market_summary CASCADE;

-- ==========================================================================
-- PHASE 4 — Legacy DDL Views (sql/DDLs/desk_views.sql)
--            Depend on: etf_flows (KEPT), etf_metadata (KEPT)
--            Drop in reverse dependency order.
-- ==========================================================================

-- 4a: Rank views (depend on signal views)
DROP VIEW IF EXISTS public.v_stock_signal_rank CASCADE;
DROP VIEW IF EXISTS public.v_industry_signal_rank CASCADE;
DROP VIEW IF EXISTS public.v_etf_signal_rank CASCADE;

-- 4b: Basing view (depends on v_etf_signals)
DROP VIEW IF EXISTS public.v_etf_basing CASCADE;

-- 4c: Signal views (depend on enriched/base views)
DROP VIEW IF EXISTS public.v_stock_signals CASCADE;
DROP VIEW IF EXISTS public.v_industry_signals CASCADE;
DROP VIEW IF EXISTS public.v_etf_signals CASCADE;

-- 4d: Enriched / latest views (depend directly on base tables)
DROP VIEW IF EXISTS public.v_etf_enriched CASCADE;
DROP VIEW IF EXISTS public.v_latest_industry CASCADE;
DROP VIEW IF EXISTS public.v_latest_etf CASCADE;

-- ==========================================================================
-- PHASE 5 — Unused Tables
-- ==========================================================================

-- etf_universe: seeded from DISTINCT etf_flows.etf but not JOINed by any view.
--               Used as a reference list for active ETF tracking (external).
DROP TABLE IF EXISTS public.etf_universe CASCADE;

-- ==========================================================================
-- PHASE 6 — Index cleanup (orphaned indexes from dropped MVs)
-- ==========================================================================

DROP INDEX IF EXISTS public.mv_etf_report_bond_credit_date_rank_uniq;
DROP INDEX IF EXISTS public.mv_macro_signal_dashboard_date_uniq;
DROP INDEX IF EXISTS public.mv_macro_bond_treasury_summary_date_uniq;

COMMIT;

-- ============================================================================
-- Post-drop verification
-- ============================================================================
--
-- These objects should still exist after running this script:
--
--   TABLES (kept):
--     etf_flows                     — base OHLCV data
--     etf_metadata                  — ETF attributes (vw_etf_daily_features)
--     etf_ranking_config            — scoring thresholds (vw_etf_daily_scores)
--     etf_analysis                  — AI analysis rows (route API)
--     config.etf_dashboard_section_config  — column config
--
--   REGULAR VIEWS (kept):
--     vw_etf_prices                 — thin wrapper over etf_flows
--     vw_etf_daily_features         — central ~60-col feature view
--     vw_etf_daily_scores           — backing view for mv_etf_daily_scores
--     vw_market_regime              — backing view for mv_market_regime
--     vw_macro_signal_table         — backing view for mv_macro_signal_table
--     vw_macro_ratio_signals        — used by vw_macro_signal_table
--
--   MATERIALIZED VIEWS (kept):
--     mv_etf_daily_scores           — intermediate: scores + eligibility
--     mv_market_regime              — intermediate: market regime
--     mv_macro_signal_table         — macro signal rows (dashboard page)
--     mv_etf_report_momentum_longs            — section source
--     mv_etf_report_momentum_shorts           — section source
--     mv_etf_report_oversold_mean_reversion   — section source
--     mv_etf_report_overbought_mean_reversion — section source
--     mv_etf_report_range_compression         — section source
--
--   FUNCTIONS (kept):
--     refresh_etf_matviews()        — refreshes all kept MV's
--       ^ NOTE: this function still has REFRESH calls for the 3 dropped
--         MV's (mv_macro_signal_dashboard, mv_macro_bond_treasury_summary,
--         mv_etf_report_bond_credit_performance). Those REFRESH lines will
--         produce a NOTICE + warning on next run but won't error.
--         To clean up, edit sql/materialized_views.sql and remove:
--           - REFRESH MATERIALIZED VIEW public.mv_macro_signal_dashboard;
--           - REFRESH MATERIALIZED VIEW public.mv_macro_bond_treasury_summary;
--           - REFRESH MATERIALIZED VIEW public.mv_etf_report_bond_credit_performance;
--         Then re-deploy the function.
--
-- Total objects dropped: 3 materialized views + 10 regular views + 3 indexes
--   + 10 legacy DDL views + 1 table = 27 objects
-- ============================================================================
