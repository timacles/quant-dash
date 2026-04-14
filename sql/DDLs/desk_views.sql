DROP VIEW IF EXISTS v_desk_trade_monitor;
DROP VIEW IF EXISTS v_volatility_opportunities;
DROP VIEW IF EXISTS v_sector_opportunities;
DROP VIEW IF EXISTS v_bond_opportunities;
DROP VIEW IF EXISTS v_top_mean_reversion;
DROP VIEW IF EXISTS v_top_momentum_positive;
DROP VIEW IF EXISTS v_top_momentum;
DROP VIEW IF EXISTS v_trade_candidates;
DROP VIEW IF EXISTS v_sleeve_regime;
DROP VIEW IF EXISTS v_market_breadth;
DROP VIEW IF EXISTS v_sleeve_breadth;
DROP VIEW IF EXISTS v_industry_latest_ranked;
DROP VIEW IF EXISTS v_etf_latest_ranked;
DROP VIEW IF EXISTS v_etf_sleeve_map;

CREATE OR REPLACE VIEW v_etf_sleeve_map AS
SELECT
    mapping.etf,
    mapping.sleeve,
    mapping.sleeve_group
FROM (
    VALUES
        ('SPY', 'broad_equity', 'risk'),
        ('EEM', 'broad_equity', 'risk'),
        ('EFA', 'broad_equity', 'risk'),
        ('IEFA', 'broad_equity', 'risk'),
        ('VEA', 'broad_equity', 'risk'),
        ('VWO', 'broad_equity', 'risk'),
        ('AGG', 'bond', 'rates_credit'),
        ('BND', 'bond', 'rates_credit'),
        ('EMB', 'bond', 'rates_credit'),
        ('HYG', 'bond', 'rates_credit'),
        ('JNK', 'bond', 'rates_credit'),
        ('LQD', 'bond', 'rates_credit'),
        ('TIP', 'bond', 'rates_credit'),
        ('TLT', 'bond', 'rates_credit'),
        ('XLB', 'sector', 'equity_sector'),
        ('XLC', 'sector', 'equity_sector'),
        ('XLE', 'sector', 'equity_sector'),
        ('XLF', 'sector', 'equity_sector'),
        ('XLI', 'sector', 'equity_sector'),
        ('XLK', 'sector', 'equity_sector'),
        ('XLP', 'sector', 'equity_sector'),
        ('XLRE', 'sector', 'equity_sector'),
        ('XLU', 'sector', 'equity_sector'),
        ('XLV', 'sector', 'equity_sector'),
        ('XLY', 'sector', 'equity_sector'),
        ('DBC', 'commodity', 'real_assets'),
        ('DBA', 'commodity', 'real_assets'),
        ('DBB', 'commodity', 'real_assets'),
        ('CORN', 'commodity', 'real_assets'),
        ('CPER', 'commodity', 'real_assets'),
        ('GDX', 'commodity', 'real_assets'),
        ('GLD', 'commodity', 'real_assets'),
        ('PALL', 'commodity', 'real_assets'),
        ('PDBC', 'commodity', 'real_assets'),
        ('PPLT', 'commodity', 'real_assets'),
        ('SLV', 'commodity', 'real_assets'),
        ('SOYB', 'commodity', 'real_assets'),
        ('UNG', 'commodity', 'real_assets'),
        ('URA', 'commodity', 'real_assets'),
        ('USO', 'commodity', 'real_assets'),
        ('WEAT', 'commodity', 'real_assets'),
        ('EWG', 'country_region', 'international'),
        ('EWJ', 'country_region', 'international'),
        ('EWU', 'country_region', 'international'),
        ('EWZ', 'country_region', 'international'),
        ('FXI', 'country_region', 'international'),
        ('INDA', 'country_region', 'international'),
        ('VXX', 'volatility', 'volatility'),
        ('UVXY', 'volatility', 'volatility'),
        ('SVXY', 'volatility', 'volatility')
) AS mapping(etf, sleeve, sleeve_group);

CREATE OR REPLACE VIEW v_etf_latest_ranked AS
SELECT DISTINCT ON (r.etf)
    r.date AS trade_date,
    'etf'::TEXT AS asset_type,
    r.etf AS symbol_or_group,
    r.etf_name AS display_name,
    COALESCE(m.sleeve, 'other') AS sleeve,
    COALESCE(m.sleeve_group, 'other') AS sleeve_group,
    r.ret_1d,
    r.ret_3d,
    r.ret_5d,
    r.ret_10d,
    r.vol_ratio_5,
    r.vol_ratio_20,
    r.range_ratio_5,
    r.price_z_20,
    r.ret_z_20,
    r.vol_z_20,
    r.range_z_20,
    r.dist_ma_5_atr,
    r.dist_ma_10_atr,
    r.ret_3d_norm,
    r.ret_5d_norm,
    r.ret_10d_norm,
    r.momentum_factor,
    r.reversion_factor,
    r.activity_factor,
    r.stretch_score,
    r.momentum_score,
    r.mean_reversion_score
FROM v_etf_signal_rank r
LEFT JOIN v_etf_sleeve_map m ON m.etf = r.etf
ORDER BY r.etf, r.date DESC;

CREATE OR REPLACE VIEW v_industry_latest_ranked AS
SELECT DISTINCT ON (r.industry)
    r.as_of_date AS trade_date,
    'industry'::TEXT AS asset_type,
    r.industry AS symbol_or_group,
    r.industry AS display_name,
    'industry'::TEXT AS sleeve,
    'industry'::TEXT AS sleeve_group,
    NULL::DOUBLE PRECISION AS ret_1d,
    NULL::DOUBLE PRECISION AS ret_3d,
    NULL::DOUBLE PRECISION AS ret_5d,
    NULL::DOUBLE PRECISION AS ret_10d,
    r.volume_ratio AS vol_ratio_5,
    r.volume_ratio AS vol_ratio_20,
    NULL::DOUBLE PRECISION AS range_ratio_5,
    NULL::DOUBLE PRECISION AS price_z_20,
    NULL::DOUBLE PRECISION AS ret_z_20,
    r.z_rel_volume AS vol_z_20,
    NULL::DOUBLE PRECISION AS range_z_20,
    NULL::DOUBLE PRECISION AS dist_ma_5_atr,
    NULL::DOUBLE PRECISION AS dist_ma_10_atr,
    NULL::DOUBLE PRECISION AS ret_3d_norm,
    NULL::DOUBLE PRECISION AS ret_5d_norm,
    NULL::DOUBLE PRECISION AS ret_10d_norm,
    r.momentum_factor,
    r.reversion_factor,
    r.activity_factor,
    r.stretch_score,
    r.momentum_score,
    r.mean_reversion_score
FROM v_industry_signal_rank r
ORDER BY r.industry, r.as_of_date DESC;

CREATE OR REPLACE VIEW v_sleeve_breadth AS
WITH latest AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_etf_latest_ranked
    UNION ALL
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_industry_latest_ranked
)
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    COUNT(*) AS name_count,
    AVG(momentum_score) AS avg_momentum_score,
    AVG(mean_reversion_score) AS avg_mean_reversion_score,
    AVG(activity_factor) AS avg_activity_factor,
    AVG(stretch_score) AS avg_stretch_score,
    AVG(CASE WHEN momentum_score > 0.50 THEN 1.0 ELSE 0.0 END) AS positive_momentum_breadth,
    AVG(CASE WHEN momentum_score < -0.50 THEN 1.0 ELSE 0.0 END) AS negative_momentum_breadth,
    AVG(CASE WHEN ABS(mean_reversion_score) > 1.00 THEN 1.0 ELSE 0.0 END) AS reversion_breadth,
    AVG(CASE WHEN activity_factor > 0.50 THEN 1.0 ELSE 0.0 END) AS activity_breadth,
    AVG(CASE WHEN stretch_score > 1.00 THEN 1.0 ELSE 0.0 END) AS stretch_breadth
FROM latest
GROUP BY
    trade_date,
    asset_type,
    sleeve,
    sleeve_group;

CREATE OR REPLACE VIEW v_market_breadth AS
WITH latest AS (
    SELECT
        trade_date,
        asset_type,
        symbol_or_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_etf_latest_ranked
    UNION ALL
    SELECT
        trade_date,
        asset_type,
        symbol_or_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_industry_latest_ranked
)
SELECT
    trade_date,
    asset_type,
    COUNT(*) AS name_count,
    AVG(momentum_score) AS avg_momentum_score,
    AVG(mean_reversion_score) AS avg_mean_reversion_score,
    AVG(activity_factor) AS avg_activity_factor,
    AVG(stretch_score) AS avg_stretch_score,
    AVG(CASE WHEN momentum_score > 0.50 THEN 1.0 ELSE 0.0 END) AS positive_momentum_breadth,
    AVG(CASE WHEN momentum_score < -0.50 THEN 1.0 ELSE 0.0 END) AS negative_momentum_breadth,
    AVG(CASE WHEN ABS(mean_reversion_score) > 1.00 THEN 1.0 ELSE 0.0 END) AS reversion_breadth,
    AVG(CASE WHEN activity_factor > 0.50 THEN 1.0 ELSE 0.0 END) AS activity_breadth,
    AVG(CASE WHEN stretch_score > 1.00 THEN 1.0 ELSE 0.0 END) AS stretch_breadth
FROM latest
GROUP BY
    trade_date,
    asset_type;

CREATE OR REPLACE VIEW v_sleeve_regime AS
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    name_count,
    avg_momentum_score,
    avg_mean_reversion_score,
    avg_activity_factor,
    avg_stretch_score,
    positive_momentum_breadth,
    negative_momentum_breadth,
    reversion_breadth,
    activity_breadth,
    stretch_breadth,
    CASE
        WHEN positive_momentum_breadth >= 0.55
            AND avg_momentum_score > 0.35
            AND activity_breadth >= 0.35 THEN 'trend_up'
        WHEN negative_momentum_breadth >= 0.55
            AND avg_momentum_score < -0.35
            AND activity_breadth >= 0.35 THEN 'trend_down'
        WHEN activity_breadth <= 0.25
            AND avg_stretch_score <= 0.90 THEN 'range_compression'
        WHEN stretch_breadth >= 0.40
            AND positive_momentum_breadth < 0.50
            AND negative_momentum_breadth < 0.50 THEN 'reversal_risk'
        ELSE 'mixed'
    END AS regime_state,
    CASE
        WHEN positive_momentum_breadth >= 0.55
            AND avg_momentum_score > 0.35 THEN 'momentum_long'
        WHEN negative_momentum_breadth >= 0.55
            AND avg_momentum_score < -0.35 THEN 'momentum_short'
        WHEN stretch_breadth >= 0.40
            AND reversion_breadth >= 0.35 THEN 'mean_reversion'
        WHEN activity_breadth <= 0.25 THEN 'wait_for_expansion'
        ELSE 'selective'
    END AS desk_bias
FROM v_sleeve_breadth;

CREATE OR REPLACE VIEW v_trade_candidates AS
WITH latest AS (
    SELECT
        trade_date,
        asset_type,
        symbol_or_group,
        display_name,
        sleeve,
        sleeve_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_etf_latest_ranked
    UNION ALL
    SELECT
        trade_date,
        asset_type,
        symbol_or_group,
        display_name,
        sleeve,
        sleeve_group,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score
    FROM v_industry_latest_ranked
),
joined AS (
    SELECT
        l.trade_date,
        l.asset_type,
        l.symbol_or_group,
        l.display_name,
        l.sleeve,
        l.sleeve_group,
        l.momentum_score,
        l.mean_reversion_score,
        l.activity_factor,
        l.stretch_score,
        r.regime_state,
        r.desk_bias,
        r.positive_momentum_breadth,
        r.negative_momentum_breadth,
        r.reversion_breadth,
        r.activity_breadth,
        r.stretch_breadth
    FROM latest l
    LEFT JOIN v_sleeve_regime r
        ON r.trade_date = l.trade_date
        AND r.asset_type = l.asset_type
        AND r.sleeve = l.sleeve
        AND r.sleeve_group = l.sleeve_group
)
SELECT
    trade_date,
    asset_type,
    symbol_or_group,
    display_name,
    sleeve,
    sleeve_group,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    regime_state,
    desk_bias,
    positive_momentum_breadth,
    negative_momentum_breadth,
    reversion_breadth,
    activity_breadth,
    stretch_breadth,
    CASE
        WHEN momentum_score > 1.00
            AND activity_factor > 0.25
            AND stretch_score < 2.25
            AND mean_reversion_score > -0.75 THEN 'momentum_positive'
        WHEN momentum_score > 0.75
            AND activity_factor > 0.00
            AND stretch_score < 2.25 THEN 'momentum'
        WHEN ABS(mean_reversion_score) > 1.00
            AND stretch_score > 1.00 THEN 'mean_reversion'
        ELSE 'watchlist'
    END AS setup_type
FROM joined;

CREATE OR REPLACE VIEW v_top_momentum AS
WITH ranked AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group,
        display_name,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score,
        regime_state,
        desk_bias,
        ROW_NUMBER() OVER (
            ORDER BY
                momentum_score DESC,
                activity_factor DESC,
                stretch_score ASC,
                symbol_or_group
        ) AS rank_in_view
    FROM v_trade_candidates
    WHERE
        momentum_score > 0.75
        AND activity_factor > 0.00
        AND stretch_score < 2.25
)
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    'momentum'::TEXT AS setup_type,
    desk_bias,
    rank_in_view
FROM ranked;

CREATE OR REPLACE VIEW v_top_momentum_positive AS
WITH ranked AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group,
        display_name,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score,
        regime_state,
        desk_bias,
        ROW_NUMBER() OVER (
            ORDER BY
                momentum_score DESC,
                activity_factor DESC,
                stretch_score ASC,
                symbol_or_group
        ) AS rank_in_view
    FROM v_trade_candidates
    WHERE
        momentum_score > 1.00
        AND activity_factor > 0.25
        AND stretch_score < 2.25
        AND mean_reversion_score > -0.75
)
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    'momentum_positive'::TEXT AS setup_type,
    desk_bias,
    rank_in_view
FROM ranked;

CREATE OR REPLACE VIEW v_top_mean_reversion AS
WITH ranked AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group,
        display_name,
        momentum_score,
        mean_reversion_score,
        activity_factor,
        stretch_score,
        regime_state,
        desk_bias,
        ROW_NUMBER() OVER (
            ORDER BY
                ABS(mean_reversion_score) DESC,
                stretch_score DESC,
                activity_factor DESC,
                symbol_or_group
        ) AS rank_in_view
    FROM v_trade_candidates
    WHERE
        ABS(mean_reversion_score) > 1.00
        AND stretch_score > 1.00
        AND regime_state NOT IN ('trend_up', 'trend_down')
)
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    'mean_reversion'::TEXT AS setup_type,
    desk_bias,
    rank_in_view
FROM ranked;

CREATE OR REPLACE VIEW v_bond_opportunities AS
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    setup_type,
    desk_bias,
    rank_in_view
FROM (
    SELECT * FROM v_top_momentum_positive
    UNION ALL
    SELECT * FROM v_top_mean_reversion
) candidates
WHERE sleeve = 'bond';

CREATE OR REPLACE VIEW v_sector_opportunities AS
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    setup_type,
    desk_bias,
    rank_in_view
FROM (
    SELECT * FROM v_top_momentum_positive
    UNION ALL
    SELECT * FROM v_top_mean_reversion
) candidates
WHERE sleeve = 'sector';

CREATE OR REPLACE VIEW v_volatility_opportunities AS
SELECT
    trade_date,
    asset_type,
    sleeve,
    sleeve_group,
    symbol_or_group,
    display_name,
    momentum_score,
    mean_reversion_score,
    activity_factor,
    stretch_score,
    setup_type,
    desk_bias,
    rank_in_view
FROM (
    SELECT * FROM v_top_momentum_positive
    UNION ALL
    SELECT * FROM v_top_mean_reversion
) candidates
WHERE sleeve = 'volatility';

CREATE OR REPLACE VIEW v_desk_trade_monitor AS
WITH top_momentum AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group AS top_momentum_symbol,
        display_name AS top_momentum_name,
        momentum_score AS top_momentum_score,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, asset_type, sleeve, sleeve_group
            ORDER BY rank_in_view
        ) AS sleeve_rank
    FROM v_top_momentum_positive
),
top_reversion AS (
    SELECT
        trade_date,
        asset_type,
        sleeve,
        sleeve_group,
        symbol_or_group AS top_reversion_symbol,
        display_name AS top_reversion_name,
        mean_reversion_score AS top_reversion_score,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, asset_type, sleeve, sleeve_group
            ORDER BY rank_in_view
        ) AS sleeve_rank
    FROM v_top_mean_reversion
)
SELECT
    r.trade_date,
    r.asset_type,
    r.sleeve,
    r.sleeve_group,
    r.name_count,
    r.regime_state,
    r.desk_bias,
    r.avg_momentum_score,
    r.avg_mean_reversion_score,
    r.avg_activity_factor,
    r.avg_stretch_score,
    r.positive_momentum_breadth,
    r.negative_momentum_breadth,
    r.reversion_breadth,
    r.activity_breadth,
    r.stretch_breadth,
    m.top_momentum_symbol,
    m.top_momentum_name,
    m.top_momentum_score,
    mr.top_reversion_symbol,
    mr.top_reversion_name,
    mr.top_reversion_score
FROM v_sleeve_regime r
LEFT JOIN top_momentum m
    ON m.trade_date = r.trade_date
    AND m.asset_type = r.asset_type
    AND m.sleeve = r.sleeve
    AND m.sleeve_group = r.sleeve_group
    AND m.sleeve_rank = 1
LEFT JOIN top_reversion mr
    ON mr.trade_date = r.trade_date
    AND mr.asset_type = r.asset_type
    AND mr.sleeve = r.sleeve
    AND mr.sleeve_group = r.sleeve_group
    AND mr.sleeve_rank = 1;
