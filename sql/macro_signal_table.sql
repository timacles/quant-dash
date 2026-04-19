-- Macro Signal Table: row-per-signal view for the macro dashboard.
-- Each row is one signal on one date with consistent columns.
-- Depends on: vw_etf_daily_features, vw_macro_ratio_signals

CREATE OR REPLACE VIEW public.vw_macro_signal_table AS

-- ── Single-ticker signals ──────────────────────────────────────────────
WITH single_ticker_signals AS (
    SELECT
        f.date,
        s.category,
        s.signal_name,
        s.source,
        f.ret_1d   AS chg_1d,
        f.ret_5d   AS chg_5d,
        f.ret_10d  AS chg_10d,
        f.ret_20d  AS chg_20d,
        f.close_vs_sma_20  AS vs_dma_20,
        f.close_vs_sma_50  AS vs_dma_50,
        f.close_vs_sma_200 AS vs_dma_200,
        f.volume_ratio_5_20 AS wk_rvol
    FROM public.vw_etf_daily_features f
    JOIN (VALUES
        ('VIXY', 'Volatility',   'VIX Level',                      'VIXY'),
        ('UUP',  'Dollar',       'USD Trend',                       'UUP'),
        ('UUP',  'Dollar',       'USD Momentum',                    'UUP'),
        ('TLT',  'Rates',        'Long-End Trend (20yr+)',          'TLT'),
        ('TIP',  'Rates',        'Real Rate Proxy (TIPS)',          'TIP'),
        ('IEF',  'Rates',        'Duration Momentum (7-10yr)',      'IEF'),
        ('SHY',  'Rates',        'Short-End Trend (2yr)',           'SHY'),
        ('GLD',  'Commodities',  'Gold Trend',                      'GLD'),
        ('USO',  'Commodities',  'Oil Trend',                       'USO'),
        ('DBC',  'Commodities',  'Broad Commodities',               'DBC'),
        ('IWM',  'Breadth',      'Small Cap Breadth',               'IWM'),
        ('IYT',  'Leading Econ', 'Transports (Dow Theory)',         'IYT'),
        ('KRE',  'Leading Econ', 'Regional Banks (Credit)',         'KRE'),
        ('XHB',  'Leading Econ', 'Homebuilders (Housing)',          'XHB'),
        ('WOOD', 'Leading Econ', 'Lumber (Construction)',            'WOOD'),
        ('SMH',  'Leading Econ', 'Semiconductors (Capex)',          'SMH')
    ) AS s(symbol, category, signal_name, source)
      ON f.symbol = s.symbol
),

-- ── Ratio-based signals ────────────────────────────────────────────────
ratio_signals AS (
    SELECT
        r.date,
        s.category,
        s.signal_name,
        r.ratio_name AS source,
        r.ratio_ret_1d   AS chg_1d,
        r.ratio_ret_5d   AS chg_5d,
        r.ratio_ret_10d  AS chg_10d,
        r.ratio_ret_20d  AS chg_20d,
        r.ratio_vs_sma_20  AS vs_dma_20,
        r.ratio_vs_sma_50  AS vs_dma_50,
        r.ratio_vs_sma_200 AS vs_dma_200,
        NULL::double precision AS wk_rvol
    FROM public.vw_macro_ratio_signals r
    JOIN (VALUES
        ('VIXY/VIXM',  'Volatility',   'VIX Term Structure'),
        ('TLT/SHY',    'Rates',        'Yield Curve Proxy'),
        ('HYG/LQD',    'Rates',        'Credit Spread Proxy'),
        ('IWM/SPY',    'Ratios',       'Small vs Large Cap'),
        ('QQQ/IWD',    'Ratios',       'Growth vs Value'),
        ('XLY/XLP',    'Ratios',       'Cyclicals vs Defensives'),
        ('SPY/TLT',    'Ratios',       'Equities vs Bonds'),
        ('CPER/GLD',   'Ratios',       'Copper vs Gold'),
        ('EEM/EFA',    'Ratios',       'EM vs DM'),
        ('SMH/SPY',    'Ratios',       'Semis vs Market'),
        ('RSP/SPY',    'Breadth',      'Equal-Wt vs Cap-Wt'),
        ('SPHB/SPLV',  'Breadth',      'High Beta vs Low Vol'),
        ('XLI/XLU',    'Leading Econ', 'Industrials vs Utilities (ISM proxy)'),
        ('XLY/XLP',    'Leading Econ', 'Consumer Strength'),
        ('CPER/GLD',   'Leading Econ', 'Copper/Gold (Growth)')
    ) AS s(ratio_name, category, signal_name)
      ON r.ratio_name = s.ratio_name
),

-- ── VIX 10d percentile (trailing 252 days) ─────────────────────────────
vix_percentile AS (
    SELECT
        f.date,
        'Volatility'::text AS category,
        'VIX 10d Percentile'::text AS signal_name,
        'VIXY'::text AS source,
        f.ret_1d  AS chg_1d,
        f.ret_5d  AS chg_5d,
        f.ret_10d AS chg_10d,
        f.ret_20d AS chg_20d,
        f.close_vs_sma_20  AS vs_dma_20,
        f.close_vs_sma_50  AS vs_dma_50,
        f.close_vs_sma_200 AS vs_dma_200,
        percent_rank() OVER (
            ORDER BY f.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        )::double precision AS wk_rvol
    FROM public.vw_etf_daily_features f
    WHERE f.symbol = 'VIXY'
),

-- ── Realized vs Implied Vol (SPY realized vol vs VIXY) ─────────────────
-- Approximation: SPY 20d stddev of returns vs VIXY close relative moves
realized_vs_implied AS (
    SELECT
        spy.date,
        'Volatility'::text AS category,
        'Realized vs Implied Vol'::text AS signal_name,
        'SPY rv vs VIXY'::text AS source,
        spy.ret_1d  AS chg_1d,
        spy.ret_5d  AS chg_5d,
        spy.ret_10d AS chg_10d,
        spy.ret_20d AS chg_20d,
        spy.close_vs_sma_20  AS vs_dma_20,
        spy.close_vs_sma_50  AS vs_dma_50,
        spy.close_vs_sma_200 AS vs_dma_200,
        CASE
            WHEN vixy.close IS NULL OR vixy.close = 0 THEN NULL
            ELSE (spy.stddev_close_20 / spy.close * sqrt(252.0)) - (vixy.close_vs_sma_20)
        END AS wk_rvol
    FROM public.vw_etf_daily_features spy
    JOIN public.vw_etf_daily_features vixy
        ON vixy.symbol = 'VIXY' AND vixy.date = spy.date
    WHERE spy.symbol = 'SPY'
),

-- ── USD 1yr percentile ──────────────────────────────────────────────────
usd_percentile AS (
    SELECT
        f.date,
        'Dollar'::text AS category,
        'USD 1yr Percentile'::text AS signal_name,
        'UUP'::text AS source,
        f.ret_1d  AS chg_1d,
        f.ret_5d  AS chg_5d,
        f.ret_10d AS chg_10d,
        f.ret_20d AS chg_20d,
        f.close_vs_sma_20  AS vs_dma_20,
        f.close_vs_sma_50  AS vs_dma_50,
        f.close_vs_sma_200 AS vs_dma_200,
        percent_rank() OVER (
            ORDER BY f.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        )::double precision AS wk_rvol
    FROM public.vw_etf_daily_features f
    WHERE f.symbol = 'UUP'
),

-- ── Sector Breadth Score (count of 11 SPDR sectors above 50-DMA) ───────
sector_breadth AS (
    SELECT
        f.date,
        'Breadth'::text AS category,
        'Sector Breadth Score'::text AS signal_name,
        '11 SPDR sectors'::text AS source,
        NULL::double precision AS chg_1d,
        NULL::double precision AS chg_5d,
        NULL::double precision AS chg_10d,
        NULL::double precision AS chg_20d,
        NULL::double precision AS vs_dma_20,
        (count(*) FILTER (WHERE f.close_vs_sma_50 > 0))::double precision / 11.0 AS vs_dma_50,
        (count(*) FILTER (WHERE f.close_vs_sma_200 > 0))::double precision / 11.0 AS vs_dma_200,
        count(*) FILTER (WHERE f.close_vs_sma_50 > 0) AS wk_rvol
    FROM public.vw_etf_daily_features f
    WHERE f.symbol IN ('XLY','XLP','XLF','XLE','XLK','XLV','XLI','XLU','XLB','XLRE','XLC')
    GROUP BY f.date
),

-- ── Combine all signals ────────────────────────────────────────────────
combined AS (
    SELECT * FROM single_ticker_signals
    UNION ALL
    SELECT * FROM ratio_signals
    UNION ALL
    SELECT * FROM vix_percentile
    UNION ALL
    SELECT * FROM realized_vs_implied
    UNION ALL
    SELECT * FROM usd_percentile
    UNION ALL
    SELECT * FROM sector_breadth
)

SELECT
    c.date,
    c.category,
    c.signal_name,
    c.source,
    round(c.chg_1d::numeric,  4) AS chg_1d,
    round(c.chg_5d::numeric,  4) AS chg_5d,
    round(c.chg_10d::numeric, 4) AS chg_10d,
    round(c.chg_20d::numeric, 4) AS chg_20d,
    round(c.vs_dma_20::numeric,  4) AS vs_dma_20,
    round(c.vs_dma_50::numeric,  4) AS vs_dma_50,
    round(c.vs_dma_200::numeric, 4) AS vs_dma_200,
    round(c.wk_rvol::numeric, 2) AS wk_rvol,
    CASE
        -- Volatility
        WHEN c.signal_name = 'VIX Level' THEN
            CASE
                WHEN c.vs_dma_200 < -0.15 THEN 'Complacent'
                WHEN c.vs_dma_200 BETWEEN -0.15 AND 0.0 THEN 'Normal'
                WHEN c.vs_dma_200 BETWEEN 0.0 AND 0.30 THEN 'Elevated'
                WHEN c.vs_dma_200 > 0.30 THEN 'Crisis'
                ELSE '—'
            END
        WHEN c.signal_name = 'VIX Term Structure' THEN
            CASE
                WHEN c.chg_20d IS NULL THEN '—'
                WHEN c.vs_dma_20 < 0 THEN 'Contango ✓'
                ELSE 'Backwardation ⚠'
            END
        WHEN c.signal_name = 'VIX 10d Percentile' THEN
            CASE
                WHEN c.wk_rvol > 0.80 THEN 'Extreme vol expansion'
                WHEN c.wk_rvol > 0.50 THEN 'Above average'
                ELSE 'Low percentile'
            END
        WHEN c.signal_name = 'Realized vs Implied Vol' THEN
            CASE
                WHEN c.wk_rvol IS NULL THEN '—'
                WHEN c.wk_rvol > 0.05 THEN 'Fear premium'
                WHEN c.wk_rvol < -0.05 THEN 'Complacency'
                ELSE 'Neutral'
            END

        -- Dollar
        WHEN c.signal_name = 'USD Trend' THEN
            CASE
                WHEN c.vs_dma_50 > 0 AND c.vs_dma_200 > 0 THEN 'Strong dollar'
                WHEN c.vs_dma_50 < 0 AND c.vs_dma_200 < 0 THEN 'Weak ✓'
                ELSE 'Mixed'
            END
        WHEN c.signal_name = 'USD Momentum' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Rising (headwind for risk)'
                WHEN c.chg_20d < 0 THEN 'Falling ✓'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'USD 1yr Percentile' THEN
            CASE
                WHEN c.wk_rvol > 0.90 THEN 'Extreme high — reversal?'
                WHEN c.wk_rvol < 0.10 THEN 'Extreme low — reversal?'
                ELSE 'Mid-range'
            END

        -- Rates
        WHEN c.signal_name = 'Long-End Trend (20yr+)' THEN
            CASE
                WHEN c.chg_20d < 0 THEN 'Rates rising'
                WHEN c.chg_20d > 0 THEN 'Rates falling ✓'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Yield Curve Proxy' THEN
            CASE
                WHEN c.chg_20d < 0 THEN 'Flattening ⚠'
                WHEN c.chg_20d > 0 THEN 'Steepening ✓'
                ELSE 'Stable'
            END
        WHEN c.signal_name = 'Credit Spread Proxy' THEN
            CASE
                WHEN c.chg_20d < 0 THEN 'Widening ⚠'
                WHEN c.chg_20d > 0 THEN 'Tightening ✓'
                ELSE 'Stable ✓'
            END
        WHEN c.signal_name = 'Real Rate Proxy (TIPS)' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Real rates falling (easier) ✓'
                WHEN c.chg_20d < 0 THEN 'Real rates rising (tighter)'
                ELSE 'Stable'
            END
        WHEN c.signal_name = 'Duration Momentum (7-10yr)' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Rates easing'
                WHEN c.chg_20d < 0 THEN 'Rates tightening'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Short-End Trend (2yr)' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Short rates falling (dovish)'
                WHEN c.chg_20d < 0 THEN 'Short rates rising (hawkish)'
                ELSE 'Flat'
            END

        -- Ratios
        WHEN c.signal_name = 'Small vs Large Cap' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Broadening ▲'
                WHEN c.chg_20d < 0 THEN 'Narrow/defensive ▼'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Growth vs Value' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Growth regime ▲'
                WHEN c.chg_20d < 0 THEN 'Value rotation ▼'
                ELSE 'Balanced'
            END
        WHEN c.signal_name = 'Cyclicals vs Defensives' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Economic optimism ▲'
                WHEN c.chg_20d < 0 THEN 'Defensive rotation ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'Equities vs Bonds' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Risk-on ▲'
                WHEN c.chg_20d < 0 THEN 'Flight to safety ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name IN ('Copper vs Gold', 'Copper/Gold (Growth)') THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Reflation/growth ▲'
                WHEN c.chg_20d < 0 THEN 'Deflation fear ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'EM vs DM' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'EM outperforming ▲'
                WHEN c.chg_20d < 0 THEN 'DM preferred ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'Semis vs Market' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Tech/growth cycle leading ▲'
                WHEN c.chg_20d < 0 THEN 'Tech lagging ▼'
                ELSE 'Neutral'
            END

        -- Commodities
        WHEN c.signal_name = 'Gold Trend' THEN
            CASE
                WHEN c.vs_dma_50 > 0 AND c.vs_dma_200 > 0 THEN 'Fear/inflation bid ▲'
                WHEN c.vs_dma_50 < 0 AND c.vs_dma_200 < 0 THEN 'Risk-on (gold weak) ▼'
                ELSE 'Mixed'
            END
        WHEN c.signal_name = 'Oil Trend' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Demand/inflation ▲'
                WHEN c.chg_20d < 0 THEN 'Growth concern ▼'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Broad Commodities' THEN
            CASE
                WHEN c.vs_dma_50 > 0 THEN 'Commodity cycle rising ▲'
                WHEN c.vs_dma_50 < 0 THEN 'Commodity cycle falling ▼'
                ELSE 'Flat'
            END

        -- Breadth
        WHEN c.signal_name = 'Equal-Wt vs Cap-Wt' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Broad participation ▲'
                WHEN c.chg_20d < 0 THEN 'Narrow leadership ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'Small Cap Breadth' THEN
            CASE
                WHEN c.vs_dma_200 > 0 THEN 'Healthy breadth ✓'
                ELSE 'Weak breadth ⚠'
            END
        WHEN c.signal_name = 'Sector Breadth Score' THEN
            CASE
                WHEN c.wk_rvol >= 8 THEN 'Strong (' || c.wk_rvol::int || '/11)'
                WHEN c.wk_rvol >= 4 THEN 'Mixed (' || c.wk_rvol::int || '/11)'
                ELSE 'Weak (' || c.wk_rvol::int || '/11)'
            END
        WHEN c.signal_name = 'High Beta vs Low Vol' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Risk appetite broad ▲'
                WHEN c.chg_20d < 0 THEN 'Defensive rotation ▼'
                ELSE 'Neutral'
            END

        -- Leading Econ
        WHEN c.signal_name = 'Industrials vs Utilities (ISM proxy)' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Expansion ▲'
                WHEN c.chg_20d < 0 THEN 'Contraction ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'Transports (Dow Theory)' THEN
            CASE
                WHEN c.vs_dma_50 > 0 AND c.vs_dma_200 > 0 THEN 'Confirms rally ✓'
                WHEN c.vs_dma_50 < 0 AND c.vs_dma_200 < 0 THEN 'Divergence ⚠'
                ELSE 'Mixed'
            END
        WHEN c.signal_name = 'Consumer Strength' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Consumer confidence ▲'
                WHEN c.chg_20d < 0 THEN 'Consumer weakness ▼'
                ELSE 'Neutral'
            END
        WHEN c.signal_name = 'Regional Banks (Credit)' THEN
            CASE
                WHEN c.vs_dma_200 > 0 THEN 'Credit conditions OK ✓'
                ELSE 'Tightening ⚠'
            END
        WHEN c.signal_name = 'Homebuilders (Housing)' THEN
            CASE
                WHEN c.vs_dma_50 > 0 THEN 'Housing expanding ▲'
                WHEN c.vs_dma_50 < 0 THEN 'Housing contracting ▼'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Lumber (Construction)' THEN
            CASE
                WHEN c.chg_20d > 0 THEN 'Building activity expanding ▲'
                WHEN c.chg_20d < 0 THEN 'Construction slowing ▼'
                ELSE 'Flat'
            END
        WHEN c.signal_name = 'Semiconductors (Capex)' THEN
            CASE
                WHEN c.vs_dma_50 > 0 AND c.vs_dma_200 > 0 THEN 'Expanding ▲'
                WHEN c.vs_dma_50 < 0 AND c.vs_dma_200 < 0 THEN 'Contracting ▼'
                ELSE 'Mixed'
            END

        ELSE '—'
    END AS interpretation
FROM combined c;
