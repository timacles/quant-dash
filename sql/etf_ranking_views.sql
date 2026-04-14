BEGIN;

DROP VIEW IF EXISTS public.vw_etf_report_momentum_positive;

CREATE TABLE IF NOT EXISTS public.etf_metadata (
    symbol text PRIMARY KEY,
    display_name text,
    asset_class text,
    theme_type text,
    sector text,
    industry text,
    region text,
    country text,
    style text,
    commodity_group text,
    duration_bucket text,
    credit_bucket text,
    risk_bucket text,
    benchmark_group text,
    benchmark_symbol text,
    is_macro_reference boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS public.etf_ranking_config (
    config_name text PRIMARY KEY,
    min_history_days integer NOT NULL DEFAULT 180,
    min_avg_dollar_volume_20 double precision NOT NULL DEFAULT 1000000,
    min_cross_section_count integer NOT NULL DEFAULT 15,
    momentum_positive_close_location_min double precision NOT NULL DEFAULT 0.65,
    mean_reversion_abs_zscore_min double precision NOT NULL DEFAULT 1.5,
    mean_reversion_abs_atr_stretch_min double precision NOT NULL DEFAULT 1.5
);

INSERT INTO public.etf_ranking_config (config_name)
VALUES ('default')
ON CONFLICT (config_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.etf_report_json_snapshot (
    report_date date NOT NULL,
    report_name text NOT NULL,
    payload jsonb NOT NULL,
    datecreated timestamptz NOT NULL DEFAULT now(),
    dateupdated timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, report_name)
);

CREATE INDEX IF NOT EXISTS etf_metadata_theme_type_idx
    ON public.etf_metadata (theme_type);

CREATE INDEX IF NOT EXISTS etf_metadata_benchmark_symbol_idx
    ON public.etf_metadata (benchmark_symbol);

CREATE OR REPLACE VIEW public.vw_etf_prices AS
SELECT
    etf AS symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM public.etf_flows;


CREATE OR REPLACE VIEW public.vw_etf_daily_scores AS
WITH config AS (
    SELECT *
    FROM public.etf_ranking_config
    WHERE config_name = 'default'
),
features AS (
    SELECT
        f.*,
        abs(f.ret_5d) AS abs_ret_5d,
        abs(f.ret_10d) AS abs_ret_10d,
        abs(f.rs_5) AS abs_rs_5,
        abs(f.rs_10) AS abs_rs_10,
        abs(f.zscore_close_20) AS abs_zscore_close_20,
        abs(f.atr_stretch_20) AS abs_atr_stretch_20
    FROM public.vw_etf_daily_features f
),
norm AS (
    SELECT
        f.*,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.avg_dollar_volume_20) AS liquidity_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.dollar_volume) AS dollar_volume_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_ret_5d) AS abs_ret_5d_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_ret_10d) AS abs_ret_10d_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_rs_5) AS abs_rs_5_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_rs_10) AS abs_rs_10_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.ret_5d) AS ret_5d_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.ret_10d) AS ret_10d_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.rs_5) AS rs_5_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.rs_10) AS rs_10_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.trend_persistence_10) AS trend_persistence_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.downtrend_persistence_10) AS downtrend_persistence_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.volume_ratio_5_20) AS volume_expansion_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.close_location_20) AS close_location_20_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_zscore_close_20) AS abs_zscore_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.abs_atr_stretch_20) AS abs_atr_stretch_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY abs(f.distance_from_short_mean)) AS short_mean_extension_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.hl_range_pct) AS range_expansion_rank_pct,
        1.0 - percent_rank() OVER (PARTITION BY f.date ORDER BY f.range_compression_5_20) AS range_compression_5_20_rank_pct,
        1.0 - percent_rank() OVER (PARTITION BY f.date ORDER BY f.range_compression_5_60) AS range_compression_5_60_rank_pct,
        1.0 - percent_rank() OVER (PARTITION BY f.date ORDER BY f.atr_compression_5_20) AS atr_compression_5_20_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.close_location_60) AS close_location_60_rank_pct,
        percent_rank() OVER (
            PARTITION BY f.date, COALESCE(f.benchmark_group, '__all__')
            ORDER BY f.rs_5
        ) AS rs_group_rank_pct,
        percent_rank() OVER (PARTITION BY f.date ORDER BY f.acc_dist_flow_5) AS acc_dist_flow_5_rank_pct,
        count(*) FILTER (WHERE f.has_min_history) OVER (PARTITION BY f.date) AS history_ready_count
    FROM features f
),
scored AS (
    SELECT
        n.*,
        CASE
            WHEN coalesce(n.ret_5d, 0) >= 0 AND coalesce(n.rs_5, 0) >= 0 THEN 'long'
            WHEN coalesce(n.ret_5d, 0) < 0 AND coalesce(n.rs_5, 0) < 0 THEN 'short'
            WHEN coalesce(n.ret_5d, 0) >= 0 THEN 'long'
            ELSE 'short'
        END AS direction_flag,
        CASE
            WHEN n.zscore_close_20 <= 0 THEN 'long_reversion'
            ELSE 'short_reversion'
        END AS mean_reversion_direction,
        CASE
            WHEN n.has_min_history
             AND n.has_min_liquidity
             AND n.ret_5d > 0
             AND n.rs_5 > 0
             AND n.close > n.sma_20
             AND coalesce(n.close_location_20, 0) >= c.momentum_positive_close_location_min
            THEN true
            ELSE false
        END AS eligible_momentum_positive,
        CASE
            WHEN n.has_min_history
             AND n.has_min_liquidity
             AND n.abs_zscore_close_20 >= c.mean_reversion_abs_zscore_min
             AND n.abs_atr_stretch_20 >= c.mean_reversion_abs_atr_stretch_min
            THEN true
            ELSE false
        END AS eligible_mean_reversion,
        CASE
            WHEN n.has_min_history
             AND n.has_min_liquidity
             AND n.range_compression_5_20 IS NOT NULL
             AND n.range_compression_5_60 IS NOT NULL
             AND n.atr_compression_5_20 IS NOT NULL
            THEN true
            ELSE false
        END AS eligible_tightening_base
    FROM norm n
    CROSS JOIN config c
)
SELECT
    s.*,
    (
        0.30 * coalesce(s.abs_ret_5d_rank_pct, 0) +
        0.20 * coalesce(s.abs_ret_10d_rank_pct, 0) +
        0.20 * GREATEST(coalesce(s.abs_rs_5_rank_pct, 0), coalesce(s.abs_rs_10_rank_pct, 0)) +
        0.15 * CASE WHEN s.direction_flag = 'long' THEN coalesce(s.trend_persistence_rank_pct, 0) ELSE coalesce(s.downtrend_persistence_rank_pct, 0) END +
        0.10 * coalesce(s.volume_expansion_rank_pct, 0) +
        0.05 * coalesce(s.liquidity_rank_pct, 0)
    ) AS score_momentum_any,
    (
        0.25 * coalesce(s.ret_5d_rank_pct, 0) +
        0.20 * coalesce(s.ret_10d_rank_pct, 0) +
        0.20 * coalesce(s.rs_5_rank_pct, 0) +
        0.15 * coalesce(s.trend_persistence_rank_pct, 0) +
        0.10 * coalesce(s.volume_expansion_rank_pct, 0) +
        0.10 * coalesce(s.close_location_20_rank_pct, 0)
    ) AS score_momentum_positive,
    (
        0.35 * coalesce(s.abs_zscore_rank_pct, 0) +
        0.20 * coalesce(s.abs_atr_stretch_rank_pct, 0) +
        0.20 * coalesce(s.short_mean_extension_rank_pct, 0) +
        0.15 * coalesce(s.range_expansion_rank_pct, 0) +
        0.10 * (
            1.0 - CASE
                WHEN sign(coalesce(s.zscore_close_20, 0)) = sign(coalesce(s.rs_5, 0))
                 AND coalesce(s.volume_ratio_5_20, 0) > 1
                THEN 1.0
                ELSE 0.0
            END
        )
    ) AS score_mean_reversion,
    (
        0.30 * coalesce(s.range_compression_5_20_rank_pct, 0) +
        0.20 * coalesce(s.range_compression_5_60_rank_pct, 0) +
        0.20 * coalesce(s.atr_compression_5_20_rank_pct, 0) +
        0.15 * coalesce(s.rs_group_rank_pct, 0) +
        0.10 * coalesce(s.close_location_20_rank_pct, 0) +
        0.05 * coalesce(s.acc_dist_flow_5_rank_pct, 0)
    ) AS score_tightening_base,
    CASE
        WHEN s.history_ready_count >= c.min_cross_section_count THEN true
        ELSE false
    END AS sufficient_cross_section
FROM scored s
CROSS JOIN config c;

CREATE OR REPLACE VIEW public.vw_etf_theme_group_metrics AS
WITH base AS (
    SELECT
        s.date,
        s.symbol,
        s.display_name,
        s.theme_type,
        s.sector,
        s.industry,
        s.region,
        s.asset_class,
        s.risk_bucket,
        s.ret_5d,
        s.ret_10d,
        s.ret_20d,
        s.rs_5,
        s.rs_10,
        s.close_vs_sma_20,
        s.close_vs_sma_50,
        s.volume_ratio_5_20,
        s.range_compression_5_20,
        s.score_momentum_any,
        s.score_momentum_positive,
        s.score_mean_reversion,
        s.score_tightening_base
    FROM public.vw_etf_daily_scores s
),
grouped AS (
    SELECT date, symbol, display_name, 'theme_type'::text AS group_kind, theme_type AS group_value, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE theme_type IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'sector', sector, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE sector IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'industry', industry, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE industry IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'region', region, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE region IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'asset_class', asset_class, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE asset_class IS NOT NULL
    UNION ALL
    SELECT date, symbol, display_name, 'risk_bucket', risk_bucket, ret_5d, ret_10d, ret_20d, rs_5, rs_10, close_vs_sma_20, close_vs_sma_50, volume_ratio_5_20, range_compression_5_20, score_momentum_any, score_momentum_positive, score_mean_reversion, score_tightening_base FROM base WHERE risk_bucket IS NOT NULL
)
SELECT
    g.date,
    g.group_kind,
    g.group_value,
    count(*) AS etf_count,
    avg(g.ret_5d) AS avg_ret_5d,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY g.ret_5d) AS median_ret_5d,
    avg(g.ret_10d) AS avg_ret_10d,
    avg(g.ret_20d) AS avg_ret_20d,
    avg(g.rs_5) AS avg_rs_5,
    avg(g.rs_10) AS avg_rs_10,
    avg(CASE WHEN g.close_vs_sma_20 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma20,
    avg(CASE WHEN g.close_vs_sma_50 > 0 THEN 1.0 ELSE 0.0 END) AS pct_above_sma50,
    avg(g.volume_ratio_5_20) AS avg_volume_ratio_5_20,
    avg(g.range_compression_5_20) AS avg_range_compression_5_20,
    avg(g.score_momentum_any) AS avg_score_momentum_any,
    avg(g.score_momentum_positive) AS avg_score_momentum_positive,
    avg(g.score_mean_reversion) AS avg_score_mean_reversion,
    avg(g.score_tightening_base) AS avg_score_tightening_base
FROM grouped g
GROUP BY g.date, g.group_kind, g.group_value;

CREATE OR REPLACE VIEW public.vw_market_regime AS
WITH breadth AS (
    SELECT
        date,
        avg(CASE WHEN close_vs_sma_20 > 0 THEN 1.0 ELSE 0.0 END) AS breadth_above_sma20,
        avg(CASE WHEN close_vs_sma_50 > 0 THEN 1.0 ELSE 0.0 END) AS breadth_above_sma50,
        avg(rs_5) AS avg_rs_5_all,
        avg(volume_ratio_5_20) AS avg_volume_ratio_5_20_all
    FROM public.vw_etf_daily_features
    GROUP BY date
),
leadership AS (
    SELECT
        f.date,
        avg(
            CASE
                WHEN f.risk_bucket = 'offensive'
                  OR f.symbol IN ('XLK', 'XLY', 'XLI', 'XLF', 'XLC', 'SMH', 'SOXX')
                THEN f.rs_5
            END
        ) AS offensive_rs_5,
        avg(
            CASE
                WHEN f.risk_bucket = 'defensive'
                  OR f.symbol IN ('XLP', 'XLU', 'XLV', 'TLT', 'IEF')
                THEN f.rs_5
            END
        ) AS defensive_rs_5,
        avg(
            CASE
                WHEN f.credit_bucket IS NOT NULL
                  OR f.symbol IN ('HYG', 'JNK', 'LQD')
                THEN f.ret_5d
            END
        ) AS credit_ret_5d,
        avg(
            CASE
                WHEN f.duration_bucket IS NOT NULL
                  OR f.symbol IN ('TLT', 'IEF', 'AGG', 'BND', 'TIP')
                THEN f.ret_5d
            END
        ) AS duration_ret_5d,
        avg(
            CASE
                WHEN f.asset_class = 'commodity'
                  OR f.symbol IN ('GLD', 'SLV', 'DBC', 'USO', 'UNG', 'DBA')
                THEN f.ret_5d
            END
        ) AS commodity_ret_5d,
        avg(
            CASE
                WHEN f.region IS NOT NULL
                  OR f.symbol IN ('EFA', 'EEM', 'VEA', 'VWO', 'FXI', 'EWJ', 'EWZ', 'EWG', 'EWU', 'INDA', 'IEFA', 'EFXT')
                THEN f.rs_5
            END
        ) AS international_rs_5
    FROM public.vw_etf_daily_features f
    GROUP BY f.date
),
benchmarks AS (
    SELECT
        f.date,
        max(CASE WHEN f.symbol = 'SPY' THEN f.close_vs_sma_20 END) AS spy_close_vs_sma20,
        max(CASE WHEN f.symbol = 'SPY' THEN f.close_vs_sma_50 END) AS spy_close_vs_sma50,
        max(CASE WHEN f.symbol = 'SPY' THEN f.ret_5d END) AS spy_ret_5d,
        max(CASE WHEN f.symbol = 'SPY' THEN f.ret_20d END) AS spy_ret_20d,
        max(CASE WHEN f.symbol = 'VIX' THEN f.ret_5d END) AS vix_ret_5d
    FROM public.vw_etf_daily_features f
    GROUP BY f.date
)
SELECT
    b.date,
    CASE
        WHEN coalesce(b.spy_close_vs_sma20, -1) > 0
         AND coalesce(b.spy_close_vs_sma50, -1) > 0
         AND coalesce(br.breadth_above_sma20, 0) >= 0.60
         AND coalesce(l.offensive_rs_5, -1) > coalesce(l.defensive_rs_5, -1)
         AND coalesce(l.credit_ret_5d, -1) >= coalesce(l.duration_ret_5d, -1)
         AND coalesce(b.vix_ret_5d, 0) <= 0
        THEN 'risk_on'
        WHEN coalesce(b.spy_close_vs_sma20, 1) < 0
         AND coalesce(b.spy_close_vs_sma50, 1) < 0
         AND coalesce(br.breadth_above_sma20, 1) <= 0.40
         AND coalesce(l.defensive_rs_5, 1) >= coalesce(l.offensive_rs_5, 1)
         AND coalesce(l.duration_ret_5d, 1) >= coalesce(l.credit_ret_5d, 1)
        THEN 'risk_off'
        WHEN abs(coalesce(l.offensive_rs_5, 0) - coalesce(l.defensive_rs_5, 0)) >= 0.02
         AND coalesce(br.breadth_above_sma20, 0) BETWEEN 0.40 AND 0.60
        THEN 'rotational'
        ELSE 'mixed'
    END AS market_regime,
    b.spy_close_vs_sma20,
    b.spy_close_vs_sma50,
    b.spy_ret_5d,
    b.spy_ret_20d,
    b.vix_ret_5d,
    br.breadth_above_sma20,
    br.breadth_above_sma50,
    br.avg_rs_5_all,
    br.avg_volume_ratio_5_20_all,
    l.offensive_rs_5,
    l.defensive_rs_5,
    l.credit_ret_5d,
    l.duration_ret_5d,
    l.commodity_ret_5d,
    l.international_rs_5,
    CASE
        WHEN coalesce(br.avg_rs_5_all, 0) > 0 AND coalesce(br.breadth_above_sma20, 0) > 0.55 THEN 'broadening'
        WHEN coalesce(br.avg_rs_5_all, 0) > 0 AND coalesce(br.breadth_above_sma20, 0) <= 0.55 THEN 'narrowing'
        ELSE 'mixed'
    END AS momentum_participation
FROM benchmarks b
JOIN breadth br
    ON br.date = b.date
JOIN leadership l
    ON l.date = b.date;

CREATE OR REPLACE VIEW public.vw_etf_ranked_lists AS
WITH candidate_lists AS (
    SELECT
        s.date,
        'momentum_any'::text AS list_type,
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
        s.score_momentum_any AS composite_score,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        true AS eligible
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.has_min_history
      AND s.has_min_liquidity

    UNION ALL

    SELECT
        s.date,
        'momentum_longs',
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
        s.score_momentum_any,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        true AS eligible
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.has_min_history
      AND s.has_min_liquidity
      AND s.direction_flag = 'long'

    UNION ALL

    SELECT
        s.date,
        'momentum_shorts',
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
        s.score_momentum_any,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        true AS eligible
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.has_min_history
      AND s.has_min_liquidity
      AND s.direction_flag = 'short'

    UNION ALL

    SELECT
        s.date,
        'oversold_mean_reversion',
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
        s.score_mean_reversion,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        s.eligible_mean_reversion
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.mean_reversion_direction = 'long_reversion'

    UNION ALL

    SELECT
        s.date,
        'overbought_mean_reversion',
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
        s.score_mean_reversion,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        s.eligible_mean_reversion
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
    WHERE s.mean_reversion_direction = 'short_reversion'

    UNION ALL

    SELECT
        s.date,
        'range_compression',
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
        s.score_tightening_base,
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
        s.avg_dollar_volume_20,
        s.sufficient_cross_section,
        s.eligible_tightening_base
    FROM public.vw_etf_daily_scores s
    JOIN public.vw_market_regime mr
        ON mr.date = s.date
),
ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.date, c.list_type
            ORDER BY c.composite_score DESC, c.avg_dollar_volume_20 DESC, c.rs_5 DESC, c.symbol
        ) AS rank
    FROM candidate_lists c
    WHERE c.eligible
      AND c.sufficient_cross_section
)
SELECT
    r.*,
    CASE
        WHEN r.avg_volume_20 IS NULL OR r.avg_volume_20 = 0 THEN NULL
        ELSE r.volume / r.avg_volume_20
    END AS rvol_20
FROM ranked r
WHERE r.rank <= 15;

CREATE OR REPLACE FUNCTION public.refresh_etf_report_json_snapshot(p_report_date date DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_report_date date;
    v_rowcount integer := 0;
BEGIN
    SELECT COALESCE(p_report_date, max(date))
    INTO v_report_date
    FROM public.vw_etf_ranked_lists;

    IF v_report_date IS NULL THEN
        RETURN 0;
    END IF;

    DELETE FROM public.etf_report_json_snapshot
    WHERE report_date = v_report_date
      AND report_name NOT IN (
          'momentum_longs',
          'momentum_shorts',
          'oversold_mean_reversion',
          'overbought_mean_reversion',
          'range_compression'
      );

    WITH report_names AS (
        SELECT unnest(
            ARRAY[
                'momentum_longs',
                'momentum_shorts',
                'oversold_mean_reversion',
                'overbought_mean_reversion',
                'range_compression'
            ]::text[]
        ) AS report_name
    ),
    aggregated_payloads AS (
        SELECT
            date AS report_date,
            list_type AS report_name,
            jsonb_agg(
                jsonb_build_object(
                    'rank', rank,
                    'symbol', symbol,
                    'display_name', display_name,
                    'ret_1d', ret_1d,
                    'ret_3d', ret_3d,
                    'ret_5d', ret_5d,
                    'ret_10d', ret_10d,
                    'rs_5', rs_5,
                    'rs_10', rs_10,
                    'volume', volume,
                    'avg_volume_3', avg_volume_3,
                    'avg_volume_5', avg_volume_5,
                    'rvol_20', rvol_20,
                    'asset_class', asset_class,
                    'theme_type', theme_type,
                    'sector', sector,
                    'industry', industry,
                    'region', region,
                    'risk_bucket', risk_bucket,
                    'direction_flag', direction_flag,
                    'mean_reversion_direction', mean_reversion_direction,
                    'market_regime', market_regime,
                    'composite_score', composite_score,
                    'zscore_close_20', zscore_close_20,
                    'atr_stretch_20', atr_stretch_20,
                    'range_compression_5_20', range_compression_5_20,
                    'range_compression_5_60', range_compression_5_60,
                    'atr_compression_5_20', atr_compression_5_20,
                    'volume_ratio_5_20', volume_ratio_5_20,
                    'close_location_20', close_location_20,
                    'avg_dollar_volume_20', avg_dollar_volume_20
                )
                ORDER BY rank
            ) AS payload
        FROM public.vw_etf_ranked_lists
        WHERE date = v_report_date
          AND list_type IN (
              'momentum_longs',
              'momentum_shorts',
              'oversold_mean_reversion',
              'overbought_mean_reversion',
              'range_compression'
          )
        GROUP BY date, list_type
    ),
    snapshot_source AS (
        SELECT
            v_report_date AS report_date,
            rn.report_name,
            COALESCE(ap.payload, '[]'::jsonb) AS payload
        FROM report_names rn
        LEFT JOIN aggregated_payloads ap
            ON ap.report_name = rn.report_name
    )
    INSERT INTO public.etf_report_json_snapshot (
        report_date,
        report_name,
        payload,
        datecreated,
        dateupdated
    )
    SELECT
        report_date,
        report_name,
        payload,
        now(),
        now()
    FROM snapshot_source
    ON CONFLICT (report_date, report_name) DO UPDATE
    SET
        payload = excluded.payload,
        dateupdated = now();

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    RETURN v_rowcount;
END;
$$;

CREATE OR REPLACE VIEW public.vw_etf_report_json_snapshot_latest AS
SELECT
    report_date,
    report_name,
    payload,
    datecreated,
    dateupdated
FROM public.etf_report_json_snapshot
WHERE report_date = (SELECT max(report_date) FROM public.etf_report_json_snapshot);

CREATE OR REPLACE VIEW public.vw_etf_daily_report_payload AS
WITH leadership AS (
    SELECT
        t.date,
        jsonb_agg(
            jsonb_build_object(
                'group_kind', t.group_kind,
                'group_value', t.group_value,
                'etf_count', t.etf_count,
                    'avg_ret_5d', CASE WHEN t.avg_ret_5d IS NULL THEN NULL ELSE t.avg_ret_5d * 100.0 END,
                    'avg_ret_20d', CASE WHEN t.avg_ret_20d IS NULL THEN NULL ELSE t.avg_ret_20d * 100.0 END,
                    'avg_rs_5', CASE WHEN t.avg_rs_5 IS NULL THEN NULL ELSE t.avg_rs_5 * 100.0 END,
                'pct_above_sma20', t.pct_above_sma20,
                'avg_volume_ratio_5_20', t.avg_volume_ratio_5_20
            )
            ORDER BY t.avg_rs_5 DESC NULLS LAST, t.avg_ret_5d DESC NULLS LAST
        ) FILTER (WHERE t.rn <= 5) AS strongest_groups
    FROM (
        SELECT
            g.*,
            row_number() OVER (
                PARTITION BY g.date
                ORDER BY g.avg_rs_5 DESC NULLS LAST, g.avg_ret_5d DESC NULLS LAST
            ) AS rn
        FROM public.vw_etf_theme_group_metrics g
        WHERE g.group_kind IN ('theme_type', 'sector', 'region')
    ) t
    GROUP BY t.date
),
weakness AS (
    SELECT
        t.date,
        jsonb_agg(
            jsonb_build_object(
                'group_kind', t.group_kind,
                'group_value', t.group_value,
                'etf_count', t.etf_count,
                    'avg_ret_5d', CASE WHEN t.avg_ret_5d IS NULL THEN NULL ELSE t.avg_ret_5d * 100.0 END,
                    'avg_ret_20d', CASE WHEN t.avg_ret_20d IS NULL THEN NULL ELSE t.avg_ret_20d * 100.0 END,
                    'avg_rs_5', CASE WHEN t.avg_rs_5 IS NULL THEN NULL ELSE t.avg_rs_5 * 100.0 END,
                'pct_above_sma20', t.pct_above_sma20,
                'avg_volume_ratio_5_20', t.avg_volume_ratio_5_20
            )
            ORDER BY t.avg_rs_5 ASC NULLS LAST, t.avg_ret_5d ASC NULLS LAST
        ) FILTER (WHERE t.rn <= 5) AS weakest_groups
    FROM (
        SELECT
            g.*,
            row_number() OVER (
                PARTITION BY g.date
                ORDER BY g.avg_rs_5 ASC NULLS LAST, g.avg_ret_5d ASC NULLS LAST
            ) AS rn
        FROM public.vw_etf_theme_group_metrics g
        WHERE g.group_kind IN ('theme_type', 'sector', 'region')
    ) t
    GROUP BY t.date
),
lists AS (
    SELECT
        r.date,
        jsonb_object_agg(
            r.list_type,
            r.rows_json
        ) AS ranked_lists
    FROM (
        SELECT
            date,
            list_type,
            jsonb_agg(
                jsonb_build_object(
                    'rank', rank,
                    'symbol', symbol,
                    'display_name', display_name,
                    'ret_1d', CASE WHEN ret_1d IS NULL THEN NULL ELSE ret_1d * 100.0 END,
                    'ret_3d', CASE WHEN ret_3d IS NULL THEN NULL ELSE ret_3d * 100.0 END,
                    'ret_5d', CASE WHEN ret_5d IS NULL THEN NULL ELSE ret_5d * 100.0 END,
                    'ret_10d', CASE WHEN ret_10d IS NULL THEN NULL ELSE ret_10d * 100.0 END,
                    'rs_5', CASE WHEN rs_5 IS NULL THEN NULL ELSE rs_5 * 100.0 END,
                    'rs_10', CASE WHEN rs_10 IS NULL THEN NULL ELSE rs_10 * 100.0 END,
                    'volume', volume,
                    'avg_volume_3', avg_volume_3,
                    'avg_volume_5', avg_volume_5,
                    'rvol_20', rvol_20,
                    'asset_class', asset_class,
                    'theme_type', theme_type,
                    'sector', sector,
                    'industry', industry,
                    'region', region,
                    'risk_bucket', risk_bucket,
                    'direction_flag', direction_flag,
                    'mean_reversion_direction', mean_reversion_direction,
                    'market_regime', market_regime,
                    'composite_score', composite_score,
                    'zscore_close_20', zscore_close_20,
                    'atr_stretch_20', atr_stretch_20,
                    'range_compression_5_20', range_compression_5_20,
                    'range_compression_5_60', range_compression_5_60,
                    'atr_compression_5_20', atr_compression_5_20,
                    'volume_ratio_5_20', volume_ratio_5_20,
                    'close_location_20', close_location_20,
                    'avg_dollar_volume_20', avg_dollar_volume_20
                )
                ORDER BY rank
            ) AS rows_json
        FROM public.vw_etf_ranked_lists
        WHERE list_type IN (
            'momentum_any',
            'momentum_longs',
            'momentum_shorts',
            'oversold_mean_reversion',
            'overbought_mean_reversion',
            'range_compression'
        )
        GROUP BY date, list_type
    ) r
    GROUP BY r.date
)
SELECT
    mr.date AS report_date,
    jsonb_build_object(
        'report_date', mr.date,
        'market_regime', mr.market_regime,
        'macro_dashboard', jsonb_build_object(
            'spy_close_vs_sma20', mr.spy_close_vs_sma20,
            'spy_close_vs_sma50', mr.spy_close_vs_sma50,
            'spy_ret_5d', CASE WHEN mr.spy_ret_5d IS NULL THEN NULL ELSE mr.spy_ret_5d * 100.0 END,
            'spy_ret_20d', CASE WHEN mr.spy_ret_20d IS NULL THEN NULL ELSE mr.spy_ret_20d * 100.0 END,
            'vix_ret_5d', CASE WHEN mr.vix_ret_5d IS NULL THEN NULL ELSE mr.vix_ret_5d * 100.0 END,
            'breadth_above_sma20', mr.breadth_above_sma20,
            'breadth_above_sma50', mr.breadth_above_sma50,
            'offensive_rs_5', CASE WHEN mr.offensive_rs_5 IS NULL THEN NULL ELSE mr.offensive_rs_5 * 100.0 END,
            'defensive_rs_5', CASE WHEN mr.defensive_rs_5 IS NULL THEN NULL ELSE mr.defensive_rs_5 * 100.0 END,
            'credit_ret_5d', CASE WHEN mr.credit_ret_5d IS NULL THEN NULL ELSE mr.credit_ret_5d * 100.0 END,
            'duration_ret_5d', CASE WHEN mr.duration_ret_5d IS NULL THEN NULL ELSE mr.duration_ret_5d * 100.0 END,
            'commodity_ret_5d', CASE WHEN mr.commodity_ret_5d IS NULL THEN NULL ELSE mr.commodity_ret_5d * 100.0 END,
            'international_rs_5', CASE WHEN mr.international_rs_5 IS NULL THEN NULL ELSE mr.international_rs_5 * 100.0 END,
            'momentum_participation', mr.momentum_participation
        ),
        'theme_leadership', coalesce(l.strongest_groups, '[]'::jsonb),
        'theme_weakness', coalesce(w.weakest_groups, '[]'::jsonb),
        'ranked_lists', coalesce(ls.ranked_lists, '{}'::jsonb),
        'warnings', jsonb_build_array(),
        'data_quality', jsonb_build_object(
            'requires_metadata', true,
            'min_history_days', (SELECT min_history_days FROM public.etf_ranking_config WHERE config_name = 'default'),
            'note', 'Scores requiring metadata or sufficient history will be null or excluded until inputs are complete.'
        )
    ) AS report_payload
FROM public.vw_market_regime mr
LEFT JOIN leadership l
    ON l.date = mr.date
LEFT JOIN weakness w
    ON w.date = mr.date
LEFT JOIN lists ls
    ON ls.date = mr.date;

CREATE OR REPLACE VIEW public.vw_etf_daily_report_payload_latest AS
SELECT
    report_date,
    report_payload
FROM public.vw_etf_daily_report_payload
WHERE report_date = (SELECT max(report_date) FROM public.vw_etf_daily_report_payload);

CREATE OR REPLACE VIEW public.vw_etf_risk_adjusted_momentum_rankings AS
WITH latest_date AS (
    SELECT max(date) AS as_of_date
    FROM public.vw_etf_daily_features
),
feature_window AS (
    SELECT
        f.date,
        f.symbol,
        f.display_name,
        f.asset_class,
        f.region,
        f.close,
        f.volume,
        f.ret_1d,
        f.ret_3d,
        f.ret_5d,
        f.hl_range_pct AS vol_1d,
        stddev_samp(f.ret_1d) OVER (
            PARTITION BY f.symbol
            ORDER BY f.date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) * sqrt(3.0) AS vol_3d,
        stddev_samp(f.ret_1d) OVER (
            PARTITION BY f.symbol
            ORDER BY f.date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) * sqrt(5.0) AS vol_5d,
        f.range_5,
        f.rvol_20 AS rvol,
        avg(f.volume) OVER (
            PARTITION BY f.symbol
            ORDER BY f.date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_volume_5d,
        avg(f.volume) OVER (
            PARTITION BY f.symbol
            ORDER BY f.date
            ROWS BETWEEN 9 PRECEDING AND 5 PRECEDING
        ) AS avg_volume_prev_5d,
        f.has_min_history,
        f.has_min_liquidity
    FROM public.vw_etf_daily_features f
),
scored AS (
    SELECT
        fw.date,
        fw.symbol,
        fw.display_name,
        fw.asset_class,
        fw.region,
        fw.ret_1d,
        fw.ret_3d,
        fw.ret_5d,
        fw.vol_1d,
        fw.vol_3d,
        fw.vol_5d,
        CASE
            WHEN fw.close IS NULL OR fw.close = 0 OR fw.range_5 IS NULL OR fw.range_5 = 0 THEN NULL
            ELSE fw.vol_5d / NULLIF(fw.range_5 / fw.close, 0)
        END AS volatility_range_ratio,
        fw.rvol,
        CASE
            WHEN fw.avg_volume_prev_5d IS NULL OR fw.avg_volume_prev_5d = 0 THEN NULL
            ELSE fw.avg_volume_5d / fw.avg_volume_prev_5d
        END AS rel_vol_5d_vs_prev_5d,
        (
            0.20 * COALESCE(fw.ret_1d / NULLIF(fw.vol_1d, 0), 0) +
            0.35 * COALESCE(fw.ret_3d / NULLIF(fw.vol_3d, 0), 0) +
            0.45 * COALESCE(fw.ret_5d / NULLIF(fw.vol_5d, 0), 0) +
            0.10 * COALESCE(ln(GREATEST(fw.rvol, 0.25)), 0) +
            0.05 * COALESCE(
                ln(
                    GREATEST(
                        CASE
                            WHEN fw.avg_volume_prev_5d IS NULL OR fw.avg_volume_prev_5d = 0 THEN NULL
                            ELSE fw.avg_volume_5d / fw.avg_volume_prev_5d
                        END,
                        0.25
                    )
                ),
                0
            )
        ) AS risk_adjusted_momentum_score
    FROM feature_window fw
    JOIN latest_date ld
        ON fw.date = ld.as_of_date
    WHERE fw.has_min_history
      AND fw.has_min_liquidity
      AND fw.ret_5d IS NOT NULL
      AND fw.vol_1d IS NOT NULL
      AND fw.vol_3d IS NOT NULL
      AND fw.vol_5d IS NOT NULL
)
SELECT
    row_number() OVER (
        ORDER BY s.risk_adjusted_momentum_score DESC, s.ret_5d DESC, s.rvol DESC NULLS LAST, s.symbol
    ) AS rank,
    s.date AS as_of_date,
    s.symbol,
    s.display_name,
    s.asset_class,
    s.region,
    round((s.ret_1d * 100)::numeric, 2) AS ret_1d_pct,
    round((s.ret_3d * 100)::numeric, 2) AS ret_3d_pct,
    round((s.ret_5d * 100)::numeric, 2) AS ret_5d_pct,
    round((s.vol_1d * 100)::numeric, 2) AS vol_1d_pct,
    round((s.vol_3d * 100)::numeric, 2) AS vol_3d_pct,
    round((s.vol_5d * 100)::numeric, 2) AS vol_5d_pct,
    round(s.volatility_range_ratio::numeric, 3) AS volatility_range_ratio,
    round(s.rvol::numeric, 2) AS rvol,
    round(s.rel_vol_5d_vs_prev_5d::numeric, 2) AS rel_vol_5d_vs_prev_5d,
    round(s.risk_adjusted_momentum_score::numeric, 4) AS risk_adjusted_momentum_score
FROM scored s;

CREATE OR REPLACE VIEW public.vw_etf_oversold_mean_reversion_rankings AS
WITH latest_date AS (
    SELECT max(date) AS as_of_date
    FROM public.vw_etf_daily_scores
)
SELECT
    row_number() OVER (
        ORDER BY s.score_mean_reversion DESC, s.zscore_close_20 ASC, s.atr_stretch_20 ASC, s.avg_dollar_volume_20 DESC, s.symbol
    ) AS rank,
    s.date AS as_of_date,
    s.symbol,
    s.display_name,
    s.asset_class,
    s.region,
    round((s.ret_1d * 100)::numeric, 2) AS ret_1d_pct,
    round((s.ret_3d * 100)::numeric, 2) AS ret_3d_pct,
    round((s.ret_5d * 100)::numeric, 2) AS ret_5d_pct,
    round(s.zscore_close_20::numeric, 3) AS zscore_close_20,
    round(s.atr_stretch_20::numeric, 3) AS atr_stretch_20,
    round(s.close_location_20::numeric, 3) AS close_location_20,
    round(s.volume_ratio_5_20::numeric, 3) AS volume_ratio_5_20,
    round(s.rvol_20::numeric, 2) AS rvol,
    round(s.score_mean_reversion::numeric, 4) AS mean_reversion_score
FROM public.vw_etf_daily_scores s
JOIN latest_date ld
    ON s.date = ld.as_of_date
WHERE s.eligible_mean_reversion
  AND s.mean_reversion_direction = 'long_reversion'
  AND s.sufficient_cross_section;

CREATE OR REPLACE VIEW public.vw_etf_overbought_mean_reversion_rankings AS
WITH latest_date AS (
    SELECT max(date) AS as_of_date
    FROM public.vw_etf_daily_scores
)
SELECT
    row_number() OVER (
        ORDER BY s.score_mean_reversion DESC, s.zscore_close_20 DESC, s.atr_stretch_20 DESC, s.avg_dollar_volume_20 DESC, s.symbol
    ) AS rank,
    s.date AS as_of_date,
    s.symbol,
    s.display_name,
    s.asset_class,
    s.region,
    round((s.ret_1d * 100)::numeric, 2) AS ret_1d_pct,
    round((s.ret_3d * 100)::numeric, 2) AS ret_3d_pct,
    round((s.ret_5d * 100)::numeric, 2) AS ret_5d_pct,
    round(s.zscore_close_20::numeric, 3) AS zscore_close_20,
    round(s.atr_stretch_20::numeric, 3) AS atr_stretch_20,
    round(s.close_location_20::numeric, 3) AS close_location_20,
    round(s.volume_ratio_5_20::numeric, 3) AS volume_ratio_5_20,
    round(s.rvol_20::numeric, 2) AS rvol,
    round(s.score_mean_reversion::numeric, 4) AS mean_reversion_score
FROM public.vw_etf_daily_scores s
JOIN latest_date ld
    ON s.date = ld.as_of_date
WHERE s.eligible_mean_reversion
  AND s.mean_reversion_direction = 'short_reversion'
  AND s.sufficient_cross_section;

CREATE OR REPLACE VIEW public.vw_etf_range_compression_rankings AS
WITH latest_date AS (
    SELECT max(date) AS as_of_date
    FROM public.vw_etf_daily_scores
)
SELECT
    row_number() OVER (
        ORDER BY s.score_tightening_base DESC, s.rs_5 DESC NULLS LAST, s.avg_dollar_volume_20 DESC, s.symbol
    ) AS rank,
    s.date AS as_of_date,
    s.symbol,
    s.display_name,
    s.asset_class,
    s.region,
    round((s.ret_1d * 100)::numeric, 2) AS ret_1d_pct,
    round((s.ret_3d * 100)::numeric, 2) AS ret_3d_pct,
    round((s.ret_5d * 100)::numeric, 2) AS ret_5d_pct,
    round((s.rs_5 * 100)::numeric, 2) AS rs_5_pct,
    round(s.range_compression_5_20::numeric, 3) AS range_compression_5_20,
    round(s.range_compression_5_60::numeric, 3) AS range_compression_5_60,
    round(s.atr_compression_5_20::numeric, 3) AS atr_compression_5_20,
    round(s.close_location_20::numeric, 3) AS close_location_20,
    round(s.volume_ratio_5_20::numeric, 3) AS volume_ratio_5_20,
    round(s.rvol_20::numeric, 2) AS rvol,
    round(s.score_tightening_base::numeric, 4) AS range_compression_score
FROM public.vw_etf_daily_scores s
JOIN latest_date ld
    ON s.date = ld.as_of_date
WHERE s.eligible_tightening_base
  AND s.sufficient_cross_section;

COMMIT;
