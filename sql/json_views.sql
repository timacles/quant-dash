-- sql/json_views.sql
-- JSON views for LLM / API consumption.
-- Each view returns one row with a jsonb column containing a self-contained
-- snapshot of the latest data.
--
-- Run this file via psql to create the views.  Views read from mv_*
-- materialized views, so refresh those first.

BEGIN;

-- ============================================================
-- vw_json_macro_signal_table
-- Latest-day snapshot of all macro signals, grouped by category.
--
-- Returns one row:
-- {
--   "report_date": "2026-05-12",
--   "signal_count": 39,
--   "categories": {
--     "Breadth": [
--       { "signal_name": "...", "source": "...", "chg_1d": ..., ... },
--       ...
--     ],
--     "Rates": [ ... ],
--     ...
--   }
-- }
-- ============================================================

CREATE OR REPLACE VIEW public.vw_json_macro_signal_table AS
WITH latest AS (
    SELECT MAX(date) AS max_date
    FROM public.mv_macro_signal_table
),
signal_rows AS (
    SELECT
        s.category,
        s.signal_name,
        s.source,
        round(s.chg_1d::numeric,  6)  AS chg_1d,
        round(s.chg_5d::numeric,  6)  AS chg_5d,
        round(s.chg_10d::numeric, 6)  AS chg_10d,
        round(s.chg_20d::numeric, 6)  AS chg_20d,
        round(s.vs_dma_20::numeric,  6) AS vs_dma_20,
        round(s.vs_dma_50::numeric,  6) AS vs_dma_50,
        round(s.vs_dma_200::numeric, 6) AS vs_dma_200,
        round(s.wk_rvol::numeric,    2) AS wk_rvol,
        s.interpretation
    FROM public.mv_macro_signal_table s
    CROSS JOIN latest l
    WHERE s.date = l.max_date
),
grouped AS (
    SELECT
        s.category,
        jsonb_agg(
            jsonb_strip_nulls(
                jsonb_build_object(
                    'signal_name',    s.signal_name,
                    'source',         s.source,
                    'chg_1d',         s.chg_1d,
                    'chg_5d',         s.chg_5d,
                    'chg_10d',        s.chg_10d,
                    'chg_20d',        s.chg_20d,
                    'vs_dma_20',      s.vs_dma_20,
                    'vs_dma_50',      s.vs_dma_50,
                    'vs_dma_200',     s.vs_dma_200,
                    'wk_rvol',        s.wk_rvol,
                    'interpretation', s.interpretation
                )
            ) ORDER BY s.signal_name
        ) AS signals
    FROM signal_rows s
    GROUP BY s.category
)
SELECT
    l.max_date AS report_date,
    jsonb_build_object(
        'report_date',   l.max_date,
        'signal_count',  (SELECT count(*)::int FROM signal_rows),
        'categories',    jsonb_object_agg(
                            g.category,
                            jsonb_build_object(
                                'signal_count', jsonb_array_length(g.signals),
                                'signals',      g.signals
                            )
                            ORDER BY g.category
                         )
    ) AS data
FROM latest l
CROSS JOIN grouped g
GROUP BY l.max_date;


-- ============================================================
-- vw_json_etf_reports
-- Latest-day snapshot of all ranked ETF reports (momentum, mean
-- reversion, range compression).  Each report is a section with
-- a ranked holdings array ordered by rank.
--
-- Returns one row:
-- {
--   "report_date": "2026-05-12",
--   "reports": {
--     "momentum_longs": {
--       "holdings_count": 15,
--       "holdings": [
--         { "rank": 1, "symbol": "...", "composite_score": ..., ... },
--         ...
--       ]
--     },
--     "momentum_shorts": { ... },
--     "oversold_mean_reversion": { ... },
--     "overbought_mean_reversion": { ... },
--     "range_compression": { ... }
--   }
-- }
-- ============================================================

CREATE OR REPLACE VIEW public.vw_json_etf_reports AS
WITH latest AS (
    SELECT date AS max_date
    FROM (
        SELECT date FROM public.mv_etf_report_momentum_longs
        UNION
        SELECT date FROM public.mv_etf_report_momentum_shorts
        UNION
        SELECT date FROM public.mv_etf_report_oversold_mean_reversion
        UNION
        SELECT date FROM public.mv_etf_report_overbought_mean_reversion
        UNION
        SELECT date FROM public.mv_etf_report_range_compression
    ) d
    ORDER BY date DESC
    LIMIT 1
),
all_rows AS (
    SELECT
        s.date,
        s.report_name,
        s.rank,
        s.symbol,
        s.display_name,
        s.asset_class,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.risk_bucket,
        s.direction_flag,
        s.mean_reversion_direction,
        s.market_regime,
        round(s.composite_score::numeric,     4) AS composite_score,
        round(s.ret_1d::numeric,               4) AS ret_1d,
        round(s.ret_3d::numeric,               4) AS ret_3d,
        round(s.ret_5d::numeric,               4) AS ret_5d,
        round(s.ret_10d::numeric,              4) AS ret_10d,
        round(s.rs_5::numeric,                 4) AS rs_5,
        round(s.rs_10::numeric,                4) AS rs_10,
        round(s.zscore_close_20::numeric,      4) AS zscore_close_20,
        round(s.atr_stretch_20::numeric,       4) AS atr_stretch_20,
        round(s.range_compression_5_20::numeric,6) AS range_compression_5_20,
        round(s.range_compression_5_60::numeric,6) AS range_compression_5_60,
        round(s.atr_compression_5_20::numeric, 6) AS atr_compression_5_20,
        round(s.volume::numeric,               0) AS volume,
        round(s.avg_volume_3::numeric,         0) AS avg_volume_3,
        round(s.avg_volume_5::numeric,         0) AS avg_volume_5,
        round(s.avg_volume_20::numeric,        0) AS avg_volume_20,
        round(s.rvol_20::numeric,              4) AS rvol_20,
        round(s.volume_ratio_5_20::numeric,    4) AS volume_ratio_5_20,
        round(s.close_location_20::numeric,    4) AS close_location_20,
        round(s.avg_dollar_volume_20::numeric, 0) AS avg_dollar_volume_20,
        s.benchmark_symbol
    FROM public.mv_etf_report_momentum_longs s
    CROSS JOIN latest l
    WHERE s.date = l.max_date

    UNION ALL

    SELECT
        s.date,
        s.report_name,
        s.rank,
        s.symbol,
        s.display_name,
        s.asset_class,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.risk_bucket,
        s.direction_flag,
        s.mean_reversion_direction,
        s.market_regime,
        round(s.composite_score::numeric,     4),
        round(s.ret_1d::numeric,               4),
        round(s.ret_3d::numeric,               4),
        round(s.ret_5d::numeric,               4),
        round(s.ret_10d::numeric,              4),
        round(s.rs_5::numeric,                 4),
        round(s.rs_10::numeric,                4),
        round(s.zscore_close_20::numeric,      4),
        round(s.atr_stretch_20::numeric,       4),
        round(s.range_compression_5_20::numeric,6),
        round(s.range_compression_5_60::numeric,6),
        round(s.atr_compression_5_20::numeric, 6),
        round(s.volume::numeric,               0),
        round(s.avg_volume_3::numeric,         0),
        round(s.avg_volume_5::numeric,         0),
        round(s.avg_volume_20::numeric,        0),
        round(s.rvol_20::numeric,              4),
        round(s.volume_ratio_5_20::numeric,    4),
        round(s.close_location_20::numeric,    4),
        round(s.avg_dollar_volume_20::numeric, 0),
        NULL::text  -- no benchmark_symbol on shorts
    FROM public.mv_etf_report_momentum_shorts s
    CROSS JOIN latest l
    WHERE s.date = l.max_date

    UNION ALL

    SELECT
        s.date,
        s.report_name,
        s.rank,
        s.symbol,
        s.display_name,
        s.asset_class,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.risk_bucket,
        s.direction_flag,
        s.mean_reversion_direction,
        s.market_regime,
        round(s.composite_score::numeric,     4),
        round(s.ret_1d::numeric,               4),
        round(s.ret_3d::numeric,               4),
        round(s.ret_5d::numeric,               4),
        round(s.ret_10d::numeric,              4),
        round(s.rs_5::numeric,                 4),
        round(s.rs_10::numeric,                4),
        round(s.zscore_close_20::numeric,      4),
        round(s.atr_stretch_20::numeric,       4),
        round(s.range_compression_5_20::numeric,6),
        round(s.range_compression_5_60::numeric,6),
        round(s.atr_compression_5_20::numeric, 6),
        round(s.volume::numeric,               0),
        round(s.avg_volume_3::numeric,         0),
        round(s.avg_volume_5::numeric,         0),
        round(s.avg_volume_20::numeric,        0),
        round(s.rvol_20::numeric,              4),
        round(s.volume_ratio_5_20::numeric,    4),
        round(s.close_location_20::numeric,    4),
        round(s.avg_dollar_volume_20::numeric, 0),
        NULL::text
    FROM public.mv_etf_report_oversold_mean_reversion s
    CROSS JOIN latest l
    WHERE s.date = l.max_date

    UNION ALL

    SELECT
        s.date,
        s.report_name,
        s.rank,
        s.symbol,
        s.display_name,
        s.asset_class,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.risk_bucket,
        s.direction_flag,
        s.mean_reversion_direction,
        s.market_regime,
        round(s.composite_score::numeric,     4),
        round(s.ret_1d::numeric,               4),
        round(s.ret_3d::numeric,               4),
        round(s.ret_5d::numeric,               4),
        round(s.ret_10d::numeric,              4),
        round(s.rs_5::numeric,                 4),
        round(s.rs_10::numeric,                4),
        round(s.zscore_close_20::numeric,      4),
        round(s.atr_stretch_20::numeric,       4),
        round(s.range_compression_5_20::numeric,6),
        round(s.range_compression_5_60::numeric,6),
        round(s.atr_compression_5_20::numeric, 6),
        round(s.volume::numeric,               0),
        round(s.avg_volume_3::numeric,         0),
        round(s.avg_volume_5::numeric,         0),
        round(s.avg_volume_20::numeric,        0),
        round(s.rvol_20::numeric,              4),
        round(s.volume_ratio_5_20::numeric,    4),
        round(s.close_location_20::numeric,    4),
        round(s.avg_dollar_volume_20::numeric, 0),
        NULL::text
    FROM public.mv_etf_report_overbought_mean_reversion s
    CROSS JOIN latest l
    WHERE s.date = l.max_date

    UNION ALL

    SELECT
        s.date,
        s.report_name,
        s.rank,
        s.symbol,
        s.display_name,
        s.asset_class,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.risk_bucket,
        s.direction_flag,
        s.mean_reversion_direction,
        s.market_regime,
        round(s.composite_score::numeric,     4),
        round(s.ret_1d::numeric,               4),
        round(s.ret_3d::numeric,               4),
        round(s.ret_5d::numeric,               4),
        round(s.ret_10d::numeric,              4),
        round(s.rs_5::numeric,                 4),
        round(s.rs_10::numeric,                4),
        round(s.zscore_close_20::numeric,      4),
        round(s.atr_stretch_20::numeric,       4),
        round(s.range_compression_5_20::numeric,6),
        round(s.range_compression_5_60::numeric,6),
        round(s.atr_compression_5_20::numeric, 6),
        round(s.volume::numeric,               0),
        round(s.avg_volume_3::numeric,         0),
        round(s.avg_volume_5::numeric,         0),
        round(s.avg_volume_20::numeric,        0),
        round(s.rvol_20::numeric,              4),
        round(s.volume_ratio_5_20::numeric,    4),
        round(s.close_location_20::numeric,    4),
        round(s.avg_dollar_volume_20::numeric, 0),
        NULL::text
    FROM public.mv_etf_report_range_compression s
    CROSS JOIN latest l
    WHERE s.date = l.max_date
),
grouped AS (
    SELECT
        a.report_name,
        jsonb_agg(
            jsonb_strip_nulls(
                jsonb_build_object(
                    'rank',                   a.rank,
                    'symbol',                 a.symbol,
                    'display_name',           a.display_name,
                    'asset_class',            a.asset_class,
                    'theme_type',             a.theme_type,
                    'sector',                 a.sector,
                    'industry',               a.industry,
                    'region',                 a.region,
                    'risk_bucket',            a.risk_bucket,
                    'direction_flag',         a.direction_flag,
                    'mean_reversion_direction', a.mean_reversion_direction,
                    'market_regime',          a.market_regime,
                    'composite_score',        a.composite_score,
                    'ret_1d',                 a.ret_1d,
                    'ret_3d',                 a.ret_3d,
                    'ret_5d',                 a.ret_5d,
                    'ret_10d',                a.ret_10d,
                    'rs_5',                   a.rs_5,
                    'rs_10',                  a.rs_10,
                    'zscore_close_20',        a.zscore_close_20,
                    'atr_stretch_20',         a.atr_stretch_20,
                    'range_compression_5_20', a.range_compression_5_20,
                    'range_compression_5_60', a.range_compression_5_60,
                    'atr_compression_5_20',   a.atr_compression_5_20,
                    'volume',                 a.volume,
                    'avg_volume_3',           a.avg_volume_3,
                    'avg_volume_5',           a.avg_volume_5,
                    'avg_volume_20',          a.avg_volume_20,
                    'rvol_20',                a.rvol_20,
                    'volume_ratio_5_20',      a.volume_ratio_5_20,
                    'close_location_20',      a.close_location_20,
                    'avg_dollar_volume_20',   a.avg_dollar_volume_20,
                    'benchmark_symbol',       a.benchmark_symbol
                )
            ) ORDER BY a.rank
        ) AS holdings
    FROM all_rows a
    GROUP BY a.report_name
)
SELECT
    l.max_date AS report_date,
    jsonb_build_object(
        'report_date', l.max_date,
        'reports',    jsonb_object_agg(
                          g.report_name,
                          jsonb_build_object(
                              'holdings_count', jsonb_array_length(g.holdings),
                              'holdings',        g.holdings
                          )
                          ORDER BY g.report_name
                      )
    ) AS data
FROM latest l
CROSS JOIN grouped g
GROUP BY l.max_date;

COMMIT;
