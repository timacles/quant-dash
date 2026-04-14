CREATE OR REPLACE VIEW public.vw_etf_report_overbought_mean_reversion AS
WITH base AS (
    SELECT
        s.date,
        'overbought_mean_reversion'::text AS report_name,
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
        mr.market_regime,
        s.score_mean_reversion AS composite_score,
        s.ret_1d,
        s.ret_3d,
        s.ret_5d,
        s.ret_10d,
        s.rs_5,
        s.rs_10,
        s.zscore_close_20,
        s.atr_stretch_20,
        s.range_compression_5_20,
        s.range_compression_5_60,
        s.atr_compression_5_20,
        s.volume,
        s.avg_volume_3,
        s.avg_volume_5,
        s.avg_volume_20,
        s.volume_ratio_5_20,
        s.close_location_20,
        s.avg_dollar_volume_20
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.eligible_mean_reversion
      AND s.sufficient_cross_section
      AND s.mean_reversion_direction = 'short_reversion'
),
ranked AS (
    SELECT
        b.*,
        row_number() OVER (
            PARTITION BY b.date
            ORDER BY b.composite_score DESC, b.zscore_close_20 DESC, b.atr_stretch_20 DESC, b.avg_dollar_volume_20 DESC, b.symbol
        ) AS rank
    FROM base b
)
SELECT
    r.date,
    r.report_name,
    r.rank,
    r.symbol,
    r.display_name,
    r.asset_class,
    r.theme_type,
    r.sector,
    r.industry,
    r.region,
    r.risk_bucket,
    r.direction_flag,
    r.mean_reversion_direction,
    r.market_regime,
    r.composite_score,
    r.ret_1d,
    r.ret_3d,
    r.ret_5d,
    r.ret_10d,
    r.rs_5,
    r.rs_10,
    r.zscore_close_20,
    r.atr_stretch_20,
    r.range_compression_5_20,
    r.range_compression_5_60,
    r.atr_compression_5_20,
    r.volume,
    r.avg_volume_3,
    r.avg_volume_5,
    r.avg_volume_20,
    CASE
        WHEN r.avg_volume_20 IS NULL OR r.avg_volume_20 = 0 THEN NULL
        ELSE r.volume / r.avg_volume_20
    END AS rvol_20,
    r.volume_ratio_5_20,
    r.close_location_20,
    r.avg_dollar_volume_20
FROM ranked r
WHERE r.rank <= 15;
