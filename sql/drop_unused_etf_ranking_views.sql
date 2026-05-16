-- ============================================================================
-- drop_unused_etf_ranking_views.sql
--
-- Drops every object in sql/etf_ranking_views.sql that is marked as UNUSED
-- by the dashboard (see instructions/unused_sql_objects.md).
--
-- Safe to run: none of these objects are queried by serve_dashboard.py or
-- any transitive SQL dependency of it.
--
-- Drop order respects dependencies (reverse of creation order).
-- ============================================================================

BEGIN;

-- ── Layer 1: Views that depend on other views/tables being dropped ──────

DROP VIEW IF EXISTS public.vw_etf_daily_report_payload_latest CASCADE;
DROP VIEW IF EXISTS public.vw_etf_daily_report_payload CASCADE;
DROP VIEW IF EXISTS public.vw_etf_report_json_snapshot_latest CASCADE;

-- ── Layer 2: Latest-date ranking views (no cross-deps) ──────────────────

DROP VIEW IF EXISTS public.vw_etf_risk_adjusted_momentum_rankings CASCADE;
DROP VIEW IF EXISTS public.vw_etf_oversold_mean_reversion_rankings CASCADE;
DROP VIEW IF EXISTS public.vw_etf_overbought_mean_reversion_rankings CASCADE;
DROP VIEW IF EXISTS public.vw_etf_range_compression_rankings CASCADE;

-- ── Layer 3: Shared ranking view (deprecated by mv_etf_report_* matviews) ─

DROP VIEW IF EXISTS public.vw_etf_ranked_lists CASCADE;

-- ── Layer 4: Theme/group aggregator (not used by any dashboard dependency) ─

DROP VIEW IF EXISTS public.vw_etf_theme_group_metrics CASCADE;

-- ── Layer 5: Function that writes to the unused snapshot table ───────────

DROP FUNCTION IF EXISTS public.refresh_etf_report_json_snapshot CASCADE;

-- ── Layer 6: Snapshot table (last — nothing left depends on it) ──────────

DROP TABLE IF EXISTS public.etf_report_json_snapshot CASCADE;

COMMIT;

-- ============================================================================
-- Verification: these objects should still exist (dashboard dependencies)
-- ============================================================================
-- Should KEEP:
--   etf_metadata          (TABLE)  — dependency of vw_etf_daily_features
--   etf_ranking_config    (TABLE)  — dependency of vw_etf_daily_scores
--   vw_etf_prices         (VIEW)   — dependency of vw_etf_daily_features
--   vw_etf_daily_scores   (VIEW)   — backing view for mv_etf_daily_scores
--   vw_market_regime      (VIEW)   — backing view for mv_market_regime
-- ============================================================================
