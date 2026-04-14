CREATE OR REPLACE VIEW public.vw_etf_report_bond_credit_performance AS
WITH universe(symbol) AS (
    VALUES
        ('TLT'), ('TLH'), ('EDV'), ('ZROZ'),
        ('IEF'), ('IEI'), ('VGIT'), ('SCHR'),
        ('SHY'), ('VGSH'), ('SCHO'), ('BIL'), ('SHV'), ('SGOV'),
        ('TIP'), ('VTIP'), ('SCHP'),
        ('AGG'), ('BND'),
        ('LQD'), ('VCIT'), ('IGIB'), ('SPIB'), ('SPSB'), ('IGSB'), ('JPST'),
        ('HYG'), ('JNK'), ('USHY'), ('ANGL'), ('SJNK'),
        ('EMB'), ('VWOB'),
        ('MUB'), ('VTEB'), ('HYD'),
        ('BKLN'), ('SRLN'),
        ('PFF'), ('PGX'),
        ('CWB'), ('ICVT'),
        ('BSV')
),
base AS (
    SELECT
        f.date,
        'bond_credit_performance'::text AS report_name,
        f.symbol,
        f.display_name,
        f.asset_class,
        f.theme_type,
        f.sector,
        f.industry,
        f.region,
        f.risk_bucket,
        f.duration_bucket,
        f.credit_bucket,
        f.benchmark_symbol,
        CASE
            WHEN f.symbol IN ('TLT', 'TLH', 'EDV', 'ZROZ') THEN 'treasury_long'
            WHEN f.symbol IN ('IEF', 'IEI', 'VGIT', 'SCHR') THEN 'treasury_intermediate'
            WHEN f.symbol IN ('SHY', 'VGSH', 'SCHO', 'BIL', 'SHV', 'SGOV', 'BSV') THEN 'treasury_short'
            WHEN f.symbol IN ('TIP', 'VTIP', 'SCHP') THEN 'tips'
            WHEN f.symbol IN ('HYG', 'JNK', 'USHY', 'ANGL', 'SJNK', 'BKLN', 'SRLN') THEN 'credit_high_yield'
            WHEN f.symbol IN ('LQD', 'VCIT', 'IGIB', 'SPIB', 'SPSB', 'IGSB', 'JPST') THEN 'credit_investment_grade'
            WHEN f.symbol IN ('AGG', 'BND') THEN 'core_bond'
            WHEN f.symbol IN ('EMB', 'VWOB') THEN 'em_bond'
            WHEN f.symbol IN ('MUB', 'VTEB', 'HYD') THEN 'municipal_bond'
            WHEN f.symbol IN ('PFF', 'PGX') THEN 'preferreds'
            WHEN f.symbol IN ('CWB', 'ICVT') THEN 'convertibles'
            WHEN f.credit_bucket IS NOT NULL AND lower(f.credit_bucket) LIKE '%high%' THEN 'credit_high_yield'
            WHEN f.credit_bucket IS NOT NULL AND (
                lower(f.credit_bucket) LIKE '%invest%'
                OR lower(f.credit_bucket) LIKE '%ig%'
            ) THEN 'credit_investment_grade'
            WHEN f.credit_bucket IS NOT NULL AND lower(f.credit_bucket) LIKE '%government%' THEN
                CASE
                    WHEN f.duration_bucket IS NOT NULL AND lower(f.duration_bucket) LIKE '%long%' THEN 'treasury_long'
                    WHEN f.duration_bucket IS NOT NULL AND lower(f.duration_bucket) LIKE '%short%' THEN 'treasury_short'
                    ELSE 'treasury_intermediate'
                END
            WHEN f.duration_bucket IS NOT NULL AND (
                lower(f.duration_bucket) LIKE '%long%'
                OR lower(f.duration_bucket) LIKE '%20%'
                OR lower(f.duration_bucket) LIKE '%10%'
            ) THEN 'treasury_long'
            WHEN f.duration_bucket IS NOT NULL AND (
                lower(f.duration_bucket) LIKE '%intermediate%'
                OR lower(f.duration_bucket) LIKE '%7%'
                OR lower(f.duration_bucket) LIKE '%5%'
                OR lower(f.duration_bucket) LIKE '%3%'
            ) THEN 'treasury_intermediate'
            WHEN f.duration_bucket IS NOT NULL AND (
                lower(f.duration_bucket) LIKE '%short%'
                OR lower(f.duration_bucket) LIKE '%1%'
                OR lower(f.duration_bucket) LIKE '%0-3%'
            ) THEN 'treasury_short'
            ELSE 'other_bond'
        END AS bond_bucket,
        mr.market_regime,
        msd.macro_regime,
        bts.credit_spread_proxy_20d,
        bts.duration_spread_proxy_20d,
        bts.credit_risk_on_flag,
        bts.duration_bid_flag,
        bts.inflation_bid_flag,
        msd.hyg_lqd_ratio_ret_20d,
        msd.tlt_ief_ratio_ret_20d,
        f.ret_1d,
        f.ret_3d,
        f.ret_5d,
        f.ret_10d,
        f.ret_20d,
        f.ret_60d,
        f.rs_5,
        f.rs_10,
        f.rs_20,
        f.rs_60,
        f.close_vs_sma_20,
        f.close_vs_sma_50,
        f.close_vs_sma_200,
        f.trend_persistence_10,
        f.downtrend_persistence_10,
        f.close_location_20,
        f.zscore_close_20,
        f.atr_stretch_20,
        f.distance_from_short_mean,
        f.volume,
        f.avg_volume_20,
        f.volume_ratio_5_20,
        f.avg_dollar_volume_20,
        (f.ret_5d - (f.ret_20d / 4.0)) AS ret_5d_accel_vs_20d,
        (f.rs_5 - (f.rs_20 / 4.0)) AS rs_5_accel_vs_20d,
        CASE
            WHEN coalesce(f.ret_20d, 0) >= 0 AND coalesce(f.rs_20, 0) >= 0 THEN 'long'
            WHEN coalesce(f.ret_20d, 0) < 0 AND coalesce(f.rs_20, 0) < 0 THEN 'short'
            WHEN coalesce(f.ret_20d, 0) >= 0 THEN 'long'
            ELSE 'short'
        END AS direction_flag
    FROM public.vw_etf_daily_features f
    JOIN universe u
        ON u.symbol = f.symbol
    JOIN public.vw_market_regime mr
        ON mr.date = f.date
    LEFT JOIN public.vw_macro_bond_treasury_summary bts
        ON bts.date = f.date
    LEFT JOIN public.vw_macro_signal_dashboard msd
        ON msd.date = f.date
    WHERE f.has_min_history
      AND f.has_min_liquidity
      AND lower(coalesce(f.asset_class, '')) IN ('bond', 'bonds', 'fixed_income')
),
normalized AS (
    SELECT
        b.*,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.ret_20d) AS ret_20d_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.ret_60d) AS ret_60d_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.rs_20) AS rs_20_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.ret_5d_accel_vs_20d) AS accel_ret_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.rs_5_accel_vs_20d) AS accel_rs_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.close_vs_sma_50) AS close_vs_sma_50_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.close_vs_sma_200) AS close_vs_sma_200_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.trend_persistence_10) AS trend_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.downtrend_persistence_10) AS downtrend_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.volume_ratio_5_20) AS volume_ratio_rank_pct,
        percent_rank() OVER (PARTITION BY b.date ORDER BY b.avg_dollar_volume_20) AS liquidity_rank_pct
    FROM base b
),
scored AS (
    SELECT
        n.*,
        (
            0.22 * coalesce(n.ret_20d_rank_pct, 0) +
            0.18 * coalesce(n.ret_60d_rank_pct, 0) +
            0.20 * coalesce(n.rs_20_rank_pct, 0) +
            0.12 * coalesce(n.accel_ret_rank_pct, 0) +
            0.08 * coalesce(n.accel_rs_rank_pct, 0) +
            0.08 * coalesce(n.close_vs_sma_50_rank_pct, 0) +
            0.05 * coalesce(n.close_vs_sma_200_rank_pct, 0) +
            0.05 * CASE
                WHEN n.direction_flag = 'long' THEN coalesce(n.trend_rank_pct, 0)
                ELSE 1.0 - coalesce(n.downtrend_rank_pct, 0)
            END +
            0.01 * coalesce(n.volume_ratio_rank_pct, 0) +
            0.01 * coalesce(n.liquidity_rank_pct, 0)
        ) AS composite_score
    FROM normalized n
),
ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.date
            ORDER BY s.composite_score DESC, s.avg_dollar_volume_20 DESC, s.rs_20 DESC NULLS LAST, s.symbol
        ) AS rank
    FROM scored s
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
    r.duration_bucket,
    r.credit_bucket,
    r.bond_bucket,
    r.benchmark_symbol,
    r.direction_flag,
    r.market_regime,
    r.macro_regime,
    r.composite_score,
    r.ret_1d,
    r.ret_3d,
    r.ret_5d,
    r.ret_10d,
    r.ret_20d,
    r.ret_60d,
    r.rs_5,
    r.rs_10,
    r.rs_20,
    r.rs_60,
    r.ret_5d_accel_vs_20d,
    r.rs_5_accel_vs_20d,
    r.close_vs_sma_20,
    r.close_vs_sma_50,
    r.close_vs_sma_200,
    r.trend_persistence_10,
    r.downtrend_persistence_10,
    r.close_location_20,
    r.zscore_close_20,
    r.atr_stretch_20,
    r.distance_from_short_mean,
    r.volume,
    r.avg_volume_20,
    CASE
        WHEN r.avg_volume_20 IS NULL OR r.avg_volume_20 = 0 THEN NULL
        ELSE r.volume / r.avg_volume_20
    END AS rvol_20,
    r.volume_ratio_5_20,
    r.avg_dollar_volume_20,
    r.credit_spread_proxy_20d,
    r.duration_spread_proxy_20d,
    r.credit_risk_on_flag,
    r.duration_bid_flag,
    r.inflation_bid_flag,
    r.hyg_lqd_ratio_ret_20d,
    r.tlt_ief_ratio_ret_20d
FROM ranked r
WHERE r.rank <= 15;
