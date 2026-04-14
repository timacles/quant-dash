CREATE OR REPLACE VIEW public.vw_macro_cluster_momentum AS
WITH base AS (
    SELECT
        f.date,
        f.symbol,
        f.display_name,
        f.asset_class,
        f.theme_type,
        f.sector,
        f.industry,
        f.region,
        f.country,
        f.style,
        f.commodity_group,
        f.duration_bucket,
        f.credit_bucket,
        f.risk_bucket,
        f.benchmark_group,
        f.is_macro_reference,
        f.ret_5d,
        f.ret_10d,
        f.ret_20d,
        f.ret_60d,
        f.rs_5,
        f.rs_20,
        f.close_vs_sma_20,
        f.close_vs_sma_50,
        f.close_vs_sma_200,
        f.volume_ratio_5_20,
        f.avg_dollar_volume_20
    FROM public.vw_etf_daily_features f
),
grouped AS (
    SELECT date, symbol, display_name, 'asset_class'::text AS cluster_kind, asset_class AS cluster_value, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE asset_class IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'theme_type', theme_type, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE theme_type IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'sector', sector, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE sector IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'industry', industry, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE industry IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'region', region, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE region IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'country', country, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE country IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'style', style, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE style IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'commodity_group', commodity_group, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE commodity_group IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'duration_bucket', duration_bucket, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE duration_bucket IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'credit_bucket', credit_bucket, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE credit_bucket IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'risk_bucket', risk_bucket, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE risk_bucket IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'benchmark_group', benchmark_group, ret_5d, ret_10d, ret_20d, ret_60d, rs_5, rs_20, close_vs_sma_20, close_vs_sma_50, close_vs_sma_200, volume_ratio_5_20, avg_dollar_volume_20, is_macro_reference FROM base WHERE benchmark_group IS NOT NULL
),
aggregated AS (
    SELECT
        g.date,
        g.cluster_kind,
        g.cluster_value,
        count(*) AS etf_count,
        count(*) FILTER (WHERE g.is_macro_reference) AS macro_reference_count,
        avg(g.ret_5d) AS avg_ret_5d,
        avg(g.ret_10d) AS avg_ret_10d,
        avg(g.ret_20d) AS avg_ret_20d,
        avg(g.ret_60d) AS avg_ret_60d,
        avg(g.rs_5) AS avg_rs_5,
        avg(g.rs_20) AS avg_rs_20,
        avg(g.ret_5d - (g.ret_20d / 4.0)) AS avg_accel_5d_vs_20d,
        avg(g.rs_5 - (g.rs_20 / 4.0)) AS avg_relative_accel_5d_vs_20d,
        avg(CASE WHEN g.ret_20d > 0 THEN 1.0 ELSE 0.0 END) AS pct_positive_ret_20d,
        avg(CASE WHEN g.close_vs_sma_20 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma20,
        avg(CASE WHEN g.close_vs_sma_50 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma50,
        avg(CASE WHEN g.close_vs_sma_200 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma200,
        avg(g.volume_ratio_5_20) AS avg_volume_ratio_5_20,
        avg(g.avg_dollar_volume_20) AS avg_dollar_volume_20
    FROM grouped g
    GROUP BY g.date, g.cluster_kind, g.cluster_value
)
SELECT
    a.*,
    rank() OVER (
        PARTITION BY a.date, a.cluster_kind
        ORDER BY a.avg_ret_20d DESC NULLS LAST, a.avg_rs_20 DESC NULLS LAST, a.cluster_value
    ) AS momentum_rank_in_kind,
    rank() OVER (
        PARTITION BY a.date, a.cluster_kind
        ORDER BY a.avg_accel_5d_vs_20d DESC NULLS LAST, a.avg_relative_accel_5d_vs_20d DESC NULLS LAST, a.cluster_value
    ) AS acceleration_rank_in_kind,
    rank() OVER (
        PARTITION BY a.date, a.cluster_kind
        ORDER BY a.pct_above_sma50 DESC NULLS LAST, a.pct_positive_ret_20d DESC NULLS LAST, a.cluster_value
    ) AS breadth_rank_in_kind
FROM aggregated a;


CREATE OR REPLACE VIEW public.vw_macro_bond_treasury_buckets AS
WITH classified AS (
    SELECT
        f.date,
        f.symbol,
        f.display_name,
        f.asset_class,
        f.duration_bucket,
        f.credit_bucket,
        f.ret_5d,
        f.ret_10d,
        f.ret_20d,
        f.ret_60d,
        f.rs_5,
        f.rs_20,
        f.close_vs_sma_20,
        f.close_vs_sma_50,
        f.close_vs_sma_200,
        f.volume_ratio_5_20,
        f.avg_dollar_volume_20,
        CASE
            WHEN f.symbol IN ('TLT', 'TLH', 'EDV', 'ZROZ') THEN 'treasury_long'
            WHEN f.symbol IN ('IEF', 'IEI', 'VGIT', 'SCHR') THEN 'treasury_intermediate'
            WHEN f.symbol IN ('SHY', 'VGSH', 'SCHO', 'BIL', 'SHV', 'SGOV') THEN 'treasury_short'
            WHEN f.symbol IN ('TIP', 'VTIP', 'SCHP') THEN 'tips'
            WHEN f.symbol IN ('HYG', 'JNK', 'USHY', 'ANGL') THEN 'credit_high_yield'
            WHEN f.symbol IN ('LQD', 'VCIT', 'IGIB', 'SPIB') THEN 'credit_investment_grade'
            WHEN f.symbol IN ('AGG', 'BND') THEN 'core_bond'
            WHEN f.credit_bucket IS NOT NULL AND lower(f.credit_bucket) LIKE '%high%' THEN 'credit_high_yield'
            WHEN f.credit_bucket IS NOT NULL AND (
                lower(f.credit_bucket) LIKE '%invest%'
                OR lower(f.credit_bucket) LIKE '%ig%'
            ) THEN 'credit_investment_grade'
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
            ELSE NULL
        END AS bond_bucket
    FROM public.vw_etf_daily_features f
    WHERE f.duration_bucket IS NOT NULL
       OR f.credit_bucket IS NOT NULL
       OR f.symbol IN (
            'TLT', 'TLH', 'EDV', 'ZROZ',
            'IEF', 'IEI', 'VGIT', 'SCHR',
            'SHY', 'VGSH', 'SCHO', 'BIL', 'SHV', 'SGOV',
            'TIP', 'VTIP', 'SCHP',
            'HYG', 'JNK', 'USHY', 'ANGL',
            'LQD', 'VCIT', 'IGIB', 'SPIB',
            'AGG', 'BND'
       )
),
aggregated AS (
    SELECT
        c.date,
        c.bond_bucket,
        count(*) AS etf_count,
        avg(c.ret_5d) AS avg_ret_5d,
        avg(c.ret_10d) AS avg_ret_10d,
        avg(c.ret_20d) AS avg_ret_20d,
        avg(c.ret_60d) AS avg_ret_60d,
        avg(c.rs_5) AS avg_rs_5,
        avg(c.rs_20) AS avg_rs_20,
        avg(c.close_vs_sma_20) AS avg_close_vs_sma_20,
        avg(c.close_vs_sma_50) AS avg_close_vs_sma_50,
        avg(c.close_vs_sma_200) AS avg_close_vs_sma_200,
        avg(CASE WHEN c.close_vs_sma_50 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma50,
        avg(c.volume_ratio_5_20) AS avg_volume_ratio_5_20,
        avg(c.avg_dollar_volume_20) AS avg_dollar_volume_20
    FROM classified c
    WHERE c.bond_bucket IS NOT NULL
    GROUP BY c.date, c.bond_bucket
)
SELECT
    a.*,
    avg(a.avg_ret_5d) OVER (
        PARTITION BY a.bond_bucket
        ORDER BY a.date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_ret_5d_smooth_20,
    avg(a.avg_ret_20d) OVER (
        PARTITION BY a.bond_bucket
        ORDER BY a.date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_ret_20d_smooth_20,
    avg(a.avg_rs_20) OVER (
        PARTITION BY a.bond_bucket
        ORDER BY a.date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_rs_20_smooth_20
FROM aggregated a;


CREATE OR REPLACE VIEW public.vw_macro_bond_treasury_summary AS
SELECT
    b.date,
    max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_ret_5d END) AS treasury_long_ret_5d,
    max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_ret_20d END) AS treasury_long_ret_20d,
    max(CASE WHEN b.bond_bucket = 'treasury_intermediate' THEN b.avg_ret_20d END) AS treasury_intermediate_ret_20d,
    max(CASE WHEN b.bond_bucket = 'treasury_short' THEN b.avg_ret_20d END) AS treasury_short_ret_20d,
    max(CASE WHEN b.bond_bucket = 'tips' THEN b.avg_ret_20d END) AS tips_ret_20d,
    max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_ret_5d END) AS credit_high_yield_ret_5d,
    max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_ret_20d END) AS credit_high_yield_ret_20d,
    max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_ret_5d END) AS credit_investment_grade_ret_5d,
    max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_ret_20d END) AS credit_investment_grade_ret_20d,
    max(CASE WHEN b.bond_bucket = 'core_bond' THEN b.avg_ret_20d END) AS core_bond_ret_20d,
    max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_ret_20d_smooth_20 END) AS treasury_long_ret_20d_smooth_20,
    max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_ret_20d_smooth_20 END) AS credit_high_yield_ret_20d_smooth_20,
    max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_ret_20d_smooth_20 END) AS credit_investment_grade_ret_20d_smooth_20,
    max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_close_vs_sma_50 END) AS treasury_long_close_vs_sma50,
    max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_close_vs_sma_50 END) AS credit_high_yield_close_vs_sma50,
    max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_close_vs_sma_50 END) AS credit_investment_grade_close_vs_sma50,
    max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_ret_20d END)
        - max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_ret_20d END) AS credit_spread_proxy_20d,
    max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_ret_20d END)
        - max(CASE WHEN b.bond_bucket = 'treasury_short' THEN b.avg_ret_20d END) AS duration_spread_proxy_20d,
    max(CASE WHEN b.bond_bucket = 'tips' THEN b.avg_ret_20d END)
        - max(CASE WHEN b.bond_bucket = 'treasury_intermediate' THEN b.avg_ret_20d END) AS tips_vs_intermediate_treasury_20d,
    CASE
        WHEN (
            max(CASE WHEN b.bond_bucket = 'credit_high_yield' THEN b.avg_ret_20d END)
            - max(CASE WHEN b.bond_bucket = 'credit_investment_grade' THEN b.avg_ret_20d END)
        ) > 0
        THEN true
        ELSE false
    END AS credit_risk_on_flag,
    CASE
        WHEN (
            max(CASE WHEN b.bond_bucket = 'treasury_long' THEN b.avg_ret_20d END)
            - max(CASE WHEN b.bond_bucket = 'treasury_short' THEN b.avg_ret_20d END)
        ) > 0
        THEN true
        ELSE false
    END AS duration_bid_flag,
    CASE
        WHEN (
            max(CASE WHEN b.bond_bucket = 'tips' THEN b.avg_ret_20d END)
            - max(CASE WHEN b.bond_bucket = 'treasury_intermediate' THEN b.avg_ret_20d END)
        ) > 0
        THEN true
        ELSE false
    END AS inflation_bid_flag
FROM public.vw_macro_bond_treasury_buckets b
GROUP BY b.date;


CREATE OR REPLACE VIEW public.vw_macro_ratio_signals AS
WITH ratio_pairs AS (
    SELECT 'IWM/SPY'::text AS ratio_name, 'IWM'::text AS numerator_symbol, 'SPY'::text AS denominator_symbol, 'domestic_growth'::text AS signal_family, 'Domestic growth vs large-cap quality'::text AS signal_description
    UNION ALL
    SELECT 'QQQ/SPY', 'QQQ', 'SPY', 'growth_leadership', 'Growth leadership vs broad market'
    UNION ALL
    SELECT 'SMH/SPY', 'SMH', 'SPY', 'tech_leadership', 'Semiconductor leadership vs broad market'
    UNION ALL
    SELECT 'XLF/SPY', 'XLF', 'SPY', 'credit_expansion', 'Financials leadership vs broad market'
    UNION ALL
    SELECT 'HYG/LQD', 'HYG', 'LQD', 'risk_appetite', 'High yield vs investment grade credit'
    UNION ALL
    SELECT 'DBC/SPY', 'DBC', 'SPY', 'inflation_pressure', 'Broad commodities vs broad market'
    UNION ALL
    SELECT 'XLE/SPY', 'XLE', 'SPY', 'energy_inflation', 'Energy leadership vs broad market'
    UNION ALL
    SELECT 'TLT/IEF', 'TLT', 'IEF', 'duration_curve', 'Long duration vs intermediate treasury'
),
joined AS (
    SELECT
        n.date,
        rp.ratio_name,
        rp.signal_family,
        rp.signal_description,
        rp.numerator_symbol,
        rp.denominator_symbol,
        n.display_name AS numerator_name,
        d.display_name AS denominator_name,
        n.close AS numerator_close,
        d.close AS denominator_close,
        n.ret_5d AS numerator_ret_5d,
        n.ret_20d AS numerator_ret_20d,
        d.ret_5d AS denominator_ret_5d,
        d.ret_20d AS denominator_ret_20d
    FROM ratio_pairs rp
    JOIN public.vw_etf_daily_features n
        ON n.symbol = rp.numerator_symbol
    JOIN public.vw_etf_daily_features d
        ON d.symbol = rp.denominator_symbol
       AND d.date = n.date
),
base AS (
    SELECT
        j.*,
        CASE
            WHEN j.denominator_close IS NULL OR j.denominator_close = 0 THEN NULL
            ELSE j.numerator_close / j.denominator_close
        END AS ratio_close,
        j.numerator_ret_5d - j.denominator_ret_5d AS spread_ret_5d,
        j.numerator_ret_20d - j.denominator_ret_20d AS spread_ret_20d
    FROM joined j
),
enriched AS (
    SELECT
        b.*,
        lag(b.ratio_close) OVER w AS ratio_close_1d_ago,
        lag(b.ratio_close, 5) OVER w AS ratio_close_5d_ago,
        lag(b.ratio_close, 20) OVER w AS ratio_close_20d_ago,
        lag(b.ratio_close, 60) OVER w AS ratio_close_60d_ago,
        avg(b.ratio_close) OVER w20 AS ratio_sma_20,
        avg(b.ratio_close) OVER w50 AS ratio_sma_50,
        avg(b.spread_ret_5d) OVER w20 AS spread_ret_5d_smooth_20,
        avg(b.spread_ret_20d) OVER w20 AS spread_ret_20d_smooth_20
    FROM base b
    WINDOW
        w AS (PARTITION BY b.ratio_name ORDER BY b.date),
        w20 AS (PARTITION BY b.ratio_name ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w50 AS (PARTITION BY b.ratio_name ORDER BY b.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
)
SELECT
    e.date,
    e.ratio_name,
    e.signal_family,
    e.signal_description,
    e.numerator_symbol,
    e.denominator_symbol,
    e.numerator_name,
    e.denominator_name,
    e.ratio_close,
    CASE
        WHEN e.ratio_close_1d_ago IS NULL OR e.ratio_close_1d_ago = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_close_1d_ago - 1
    END AS ratio_ret_1d,
    CASE
        WHEN e.ratio_close_5d_ago IS NULL OR e.ratio_close_5d_ago = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_close_5d_ago - 1
    END AS ratio_ret_5d,
    CASE
        WHEN e.ratio_close_20d_ago IS NULL OR e.ratio_close_20d_ago = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_close_20d_ago - 1
    END AS ratio_ret_20d,
    CASE
        WHEN e.ratio_close_60d_ago IS NULL OR e.ratio_close_60d_ago = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_close_60d_ago - 1
    END AS ratio_ret_60d,
    e.spread_ret_5d,
    e.spread_ret_20d,
    e.spread_ret_5d_smooth_20,
    e.spread_ret_20d_smooth_20,
    e.ratio_sma_20,
    e.ratio_sma_50,
    CASE
        WHEN e.ratio_sma_20 IS NULL OR e.ratio_sma_20 = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_sma_20 - 1
    END AS ratio_vs_sma_20,
    CASE
        WHEN e.ratio_sma_50 IS NULL OR e.ratio_sma_50 = 0 THEN NULL
        ELSE e.ratio_close / e.ratio_sma_50 - 1
    END AS ratio_vs_sma_50,
    CASE
        WHEN e.ratio_close > e.ratio_sma_20
         AND e.spread_ret_20d_smooth_20 > 0
        THEN true
        ELSE false
    END AS bullish_flag,
    CASE
        WHEN e.ratio_close < e.ratio_sma_20
         AND e.spread_ret_20d_smooth_20 < 0
        THEN true
        ELSE false
    END AS bearish_flag
FROM enriched e;


CREATE OR REPLACE VIEW public.vw_macro_signal_dashboard AS
WITH ratios AS (
    SELECT
        r.date,
        max(CASE WHEN r.ratio_name = 'IWM/SPY' THEN r.ratio_ret_20d END) AS iwm_spy_ratio_ret_20d,
        max(CASE WHEN r.ratio_name = 'QQQ/SPY' THEN r.ratio_ret_20d END) AS qqq_spy_ratio_ret_20d,
        max(CASE WHEN r.ratio_name = 'HYG/LQD' THEN r.ratio_ret_20d END) AS hyg_lqd_ratio_ret_20d,
        max(CASE WHEN r.ratio_name = 'DBC/SPY' THEN r.ratio_ret_20d END) AS dbc_spy_ratio_ret_20d,
        max(CASE WHEN r.ratio_name = 'TLT/IEF' THEN r.ratio_ret_20d END) AS tlt_ief_ratio_ret_20d,
        max(CASE WHEN r.ratio_name = 'IWM/SPY' THEN r.spread_ret_20d_smooth_20 END) AS iwm_spy_spread_smooth_20,
        max(CASE WHEN r.ratio_name = 'QQQ/SPY' THEN r.spread_ret_20d_smooth_20 END) AS qqq_spy_spread_smooth_20,
        max(CASE WHEN r.ratio_name = 'HYG/LQD' THEN r.spread_ret_20d_smooth_20 END) AS hyg_lqd_spread_smooth_20,
        max(CASE WHEN r.ratio_name = 'DBC/SPY' THEN r.spread_ret_20d_smooth_20 END) AS dbc_spy_spread_smooth_20,
        bool_or(CASE WHEN r.ratio_name = 'IWM/SPY' THEN r.bullish_flag END) AS iwm_spy_bullish,
        bool_or(CASE WHEN r.ratio_name = 'QQQ/SPY' THEN r.bullish_flag END) AS qqq_spy_bullish,
        bool_or(CASE WHEN r.ratio_name = 'HYG/LQD' THEN r.bullish_flag END) AS hyg_lqd_bullish,
        bool_or(CASE WHEN r.ratio_name = 'DBC/SPY' THEN r.bullish_flag END) AS dbc_spy_bullish
    FROM public.vw_macro_ratio_signals r
    GROUP BY r.date
),
cluster_strength AS (
    SELECT
        c.date,
        max(CASE WHEN c.cluster_kind = 'risk_bucket' AND c.cluster_value = 'offensive' THEN c.avg_ret_20d END) AS offensive_cluster_ret_20d,
        max(CASE WHEN c.cluster_kind = 'risk_bucket' AND c.cluster_value = 'defensive' THEN c.avg_ret_20d END) AS defensive_cluster_ret_20d,
        max(CASE WHEN c.cluster_kind = 'asset_class' AND c.cluster_value IN ('bond', 'fixed_income') THEN c.avg_ret_20d END) AS bond_cluster_ret_20d,
        max(CASE WHEN c.cluster_kind = 'asset_class' AND c.cluster_value IN ('commodity', 'commodities') THEN c.avg_ret_20d END) AS commodity_cluster_ret_20d
    FROM public.vw_macro_cluster_momentum c
    GROUP BY c.date
)
SELECT
    r.date,
    r.iwm_spy_ratio_ret_20d,
    r.qqq_spy_ratio_ret_20d,
    r.hyg_lqd_ratio_ret_20d,
    r.dbc_spy_ratio_ret_20d,
    r.tlt_ief_ratio_ret_20d,
    r.iwm_spy_spread_smooth_20,
    r.qqq_spy_spread_smooth_20,
    r.hyg_lqd_spread_smooth_20,
    r.dbc_spy_spread_smooth_20,
    r.iwm_spy_bullish,
    r.qqq_spy_bullish,
    r.hyg_lqd_bullish,
    r.dbc_spy_bullish,
    b.credit_spread_proxy_20d,
    b.duration_spread_proxy_20d,
    b.tips_vs_intermediate_treasury_20d,
    b.credit_risk_on_flag,
    b.duration_bid_flag,
    b.inflation_bid_flag,
    c.offensive_cluster_ret_20d,
    c.defensive_cluster_ret_20d,
    c.bond_cluster_ret_20d,
    c.commodity_cluster_ret_20d,
    CASE
        WHEN coalesce(r.iwm_spy_bullish, false)
         AND coalesce(r.qqq_spy_bullish, false)
         AND coalesce(r.hyg_lqd_bullish, false)
         AND coalesce(b.credit_risk_on_flag, false)
        THEN 'risk_on'
        WHEN coalesce(b.duration_bid_flag, false)
         AND NOT coalesce(r.hyg_lqd_bullish, false)
         AND coalesce(c.defensive_cluster_ret_20d, -1) >= coalesce(c.offensive_cluster_ret_20d, -1)
        THEN 'risk_off'
        WHEN coalesce(r.dbc_spy_bullish, false)
         OR coalesce(b.inflation_bid_flag, false)
         OR coalesce(c.commodity_cluster_ret_20d, 0) > coalesce(c.bond_cluster_ret_20d, 0)
        THEN 'inflationary'
        WHEN coalesce(b.duration_bid_flag, false)
         AND coalesce(r.tlt_ief_ratio_ret_20d, 0) > 0
        THEN 'liquidity_supportive'
        ELSE 'mixed'
    END AS macro_regime
FROM ratios r
LEFT JOIN public.vw_macro_bond_treasury_summary b
    ON b.date = r.date
LEFT JOIN cluster_strength c
    ON c.date = r.date;
