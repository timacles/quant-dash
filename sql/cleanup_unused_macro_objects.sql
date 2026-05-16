-- ============================================================================
-- cleanup_unused_macro_objects.sql
--
-- Drops every object from sql/macro_signal_views.sql and sql/materialized_views.sql
-- that is marked as UNUSED or REFRESHED by the dashboard (see
-- instructions/unused_sql_objects.md).
--
-- Also strips the dropped matviews from refresh_etf_matviews() so they are no
-- longer refreshed on every cycle.
--
-- Drop order respects dependencies:
--   1. mv_etf_report_bond_credit_performance  (depends on both intermediates)
--   2. mv_macro_signal_dashboard               (depends on vw_macro_signal_dashboard)
--   3. mv_macro_bond_treasury_summary          (depends on vw_macro_bond_treasury_summary)
--   4. vw_macro_signal_dashboard               (depends on vw_macro_cluster_momentum)
--   5. vw_macro_bond_treasury_summary          (depends on vw_macro_bond_treasury_buckets)
--   6. vw_macro_cluster_momentum                (leaf — no further deps)
--   7. vw_macro_bond_treasury_buckets           (leaf — no further deps)
-- ============================================================================

BEGIN;

-- ── Step 1: Strip dropped matviews from refresh_etf_matviews() ──────────
-- Remove mv_macro_signal_dashboard, mv_macro_bond_treasury_summary, and
-- mv_etf_report_bond_credit_performance from the refresh-all function so
-- they aren't referenced after being dropped.

CREATE OR REPLACE FUNCTION public.refresh_etf_matviews()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    t_start timestamptz;
    t_step  timestamptz;
BEGIN
    t_start := clock_timestamp();

    -- Intermediates first (order matters: report matviews depend on these)
    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_daily_scores;
    RAISE NOTICE 'mv_etf_daily_scores refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_market_regime;
    RAISE NOTICE 'mv_market_regime refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_macro_signal_table;
    RAISE NOTICE 'mv_macro_signal_table refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    -- Report matviews (depend on intermediates above)
    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_report_momentum_longs;
    RAISE NOTICE 'mv_etf_report_momentum_longs refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_report_momentum_shorts;
    RAISE NOTICE 'mv_etf_report_momentum_shorts refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_report_oversold_mean_reversion;
    RAISE NOTICE 'mv_etf_report_oversold_mean_reversion refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_report_overbought_mean_reversion;
    RAISE NOTICE 'mv_etf_report_overbought_mean_reversion refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    t_step := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_etf_report_range_compression;
    RAISE NOTICE 'mv_etf_report_range_compression refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_step)::int;

    RAISE NOTICE 'All matviews refreshed in % ms',
        extract(millisecond FROM clock_timestamp() - t_start)::int;
END;
$$;

-- ── Step 2: Drop matviews (reverse dependency order) ────────────────────

DROP MATERIALIZED VIEW IF EXISTS public.mv_etf_report_bond_credit_performance CASCADE;

DROP MATERIALIZED VIEW IF EXISTS public.mv_macro_signal_dashboard CASCADE;

DROP MATERIALIZED VIEW IF EXISTS public.mv_macro_bond_treasury_summary CASCADE;

-- ── Step 3: Drop views (reverse dependency order) ───────────────────────

DROP VIEW IF EXISTS public.vw_macro_signal_dashboard CASCADE;

DROP VIEW IF EXISTS public.vw_macro_bond_treasury_summary CASCADE;

DROP VIEW IF EXISTS public.vw_macro_cluster_momentum CASCADE;

DROP VIEW IF EXISTS public.vw_macro_bond_treasury_buckets CASCADE;

COMMIT;

-- ============================================================================
-- Verification: these objects should still exist (dashboard dependencies)
-- ============================================================================
-- Should KEEP:
--   mv_etf_daily_scores             (MATVIEW)  — base for all 5 report mv's
--   mv_market_regime                (MATVIEW)  — joined by all 5 report mv's
--   mv_macro_signal_table           (MATVIEW)  — queried by fetch_macro_summary()
--   mv_etf_report_momentum_longs    (MATVIEW)  — section momentum_longs
--   mv_etf_report_momentum_shorts   (MATVIEW)  — section momentum_shorts
--   mv_etf_report_oversold_mean_reversion  (MATVIEW)  — section oversold_mean_reversion
--   mv_etf_report_overbought_mean_reversion (MATVIEW)  — section overbought_mean_reversion
--   mv_etf_report_range_compression (MATVIEW)  — section range_compression
--   vw_macro_ratio_signals          (VIEW)     — dependency of mv_macro_signal_table
--   vw_macro_signal_table           (VIEW)     — backing view for mv_macro_signal_table
--   refresh_etf_matviews()          (FUNCTION) — still used by cron/data pipeline
-- ============================================================================
