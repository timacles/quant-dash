CREATE OR REPLACE VIEW public.vw_llm_market_summary AS
WITH regime AS (
    SELECT
        mr.date,
        jsonb_build_object(
            'market_regime',         mr.market_regime,
            'macro_regime',          msd.macro_regime,
            'momentum_participation', mr.momentum_participation,
            'breadth_above_sma20',   round(mr.breadth_above_sma20::numeric, 2),
            'breadth_above_sma50',   round(mr.breadth_above_sma50::numeric, 2),
            'spy_ret_5d',            round((mr.spy_ret_5d  * 100)::numeric, 2),
            'spy_ret_20d',           round((mr.spy_ret_20d * 100)::numeric, 2),
            'offensive_rs_5',        round((mr.offensive_rs_5  * 100)::numeric, 2),
            'defensive_rs_5',        round((mr.defensive_rs_5  * 100)::numeric, 2)
        ) AS regime_json
    FROM public.vw_market_regime mr
    LEFT JOIN public.vw_macro_signal_dashboard msd ON msd.date = mr.date
),
macro AS (
    SELECT
        msd.date,
        jsonb_build_object(
            'credit_risk_on',           msd.credit_risk_on_flag,
            'duration_bid',             msd.duration_bid_flag,
            'inflation_bid',            msd.inflation_bid_flag,
            'iwm_spy_bullish',          msd.iwm_spy_bullish,
            'qqq_spy_bullish',          msd.qqq_spy_bullish,
            'hyg_lqd_bullish',          msd.hyg_lqd_bullish,
            'dbc_spy_bullish',          msd.dbc_spy_bullish,
            'credit_spread_proxy_20d',  round(msd.credit_spread_proxy_20d::numeric,  4),
            'duration_spread_proxy_20d', round(msd.duration_spread_proxy_20d::numeric, 4)
        ) AS macro_json
    FROM public.vw_macro_signal_dashboard msd
),
-- Normalize group_value case and deduplicate (e.g. 'Materials' vs 'materials')
theme_deduped AS (
    SELECT DISTINCT ON (date, group_kind, lower(group_value))
        date,
        group_kind,
        lower(group_value)       AS group_value,
        avg_ret_5d,
        avg_rs_5,
        pct_above_sma20,
        avg_volume_ratio_5_20
    FROM public.vw_etf_theme_group_metrics
    WHERE group_kind IN ('sector', 'theme_type', 'region')
    ORDER BY date, group_kind, lower(group_value), avg_rs_5 DESC NULLS LAST
),
theme_ranked AS (
    SELECT
        date,
        group_kind,
        group_value,
        round((avg_ret_5d        * 100)::numeric, 2) AS ret_5d,
        round((avg_rs_5          * 100)::numeric, 2) AS rs_5,
        round(pct_above_sma20         ::numeric, 2) AS breadth_sma20,
        round(avg_volume_ratio_5_20   ::numeric, 2) AS volume_ratio,
        row_number() OVER (PARTITION BY date ORDER BY avg_rs_5 DESC NULLS LAST, avg_ret_5d DESC NULLS LAST) AS rn_top,
        row_number() OVER (PARTITION BY date ORDER BY avg_rs_5 ASC  NULLS LAST, avg_ret_5d ASC  NULLS LAST) AS rn_bottom
    FROM theme_deduped
),
leaders AS (
    SELECT
        date,
        jsonb_agg(
            jsonb_build_object(
                'group_kind',   group_kind,
                'group_value',  group_value,
                'ret_5d',       ret_5d,
                'rs_5',         rs_5,
                'breadth_sma20', breadth_sma20,
                'volume_ratio', volume_ratio
            ) ORDER BY rn_top
        ) FILTER (WHERE rn_top <= 5) AS leaders_json
    FROM theme_ranked
    GROUP BY date
),
laggards AS (
    SELECT
        date,
        jsonb_agg(
            jsonb_build_object(
                'group_kind',   group_kind,
                'group_value',  group_value,
                'ret_5d',       ret_5d,
                'rs_5',         rs_5,
                'breadth_sma20', breadth_sma20,
                'volume_ratio', volume_ratio
            ) ORDER BY rn_bottom
        ) FILTER (WHERE rn_bottom <= 5) AS laggards_json
    FROM theme_ranked
    GROUP BY date
)
SELECT
    r.date,
    jsonb_build_object(
        'report_date',   r.date,
        'regime',        r.regime_json,
        'macro_signals', m.macro_json,
        'leaders',       coalesce(l.leaders_json,  '[]'::jsonb),
        'laggards',      coalesce(lg.laggards_json, '[]'::jsonb)
    ) AS summary
FROM regime r
LEFT JOIN macro    m  ON m.date  = r.date
LEFT JOIN leaders  l  ON l.date  = r.date
LEFT JOIN laggards lg ON lg.date = r.date;
