DROP VIEW IF EXISTS v_stock_signal_rank;
DROP VIEW IF EXISTS v_etf_basing;
DROP VIEW IF EXISTS v_etf_signal_rank;
DROP VIEW IF EXISTS v_etf_signals;
DROP VIEW IF EXISTS v_etf_enriched;
DROP VIEW IF EXISTS v_latest_etf;
DROP VIEW IF EXISTS v_industry_signal_rank;
DROP VIEW IF EXISTS v_industry_signals;
DROP VIEW IF EXISTS v_latest_industry;
DROP VIEW IF EXISTS v_stock_signals;

CREATE OR REPLACE VIEW v_latest_etf AS
SELECT DISTINCT ON (f.etf)
    f.etf,
    m.name AS etf_name,
    f.date,
    f.open,
    f.high,
    f.low,
    f.close,
    f.volume
FROM etf_flows f
LEFT JOIN etf_metadata m ON m.etf = f.etf
ORDER BY f.etf, f.date DESC;

CREATE OR REPLACE VIEW v_latest_industry AS
SELECT DISTINCT ON (industry)
    as_of_date,
    industry,
    rank,
    perf_week,
    perf_month,
    perf_quart,
    perf_half,
    perf_year,
    perf_ytd,
    avg_volume,
    rel_volume,
    change,
    volume
FROM industry_flows
ORDER BY industry, as_of_date DESC;

CREATE OR REPLACE VIEW v_etf_enriched AS
WITH base AS (
    SELECT
        f.etf,
        m.name AS etf_name,
        f.date,
        f.open,
        f.high,
        f.low,
        f.close,
        f.volume,
        LN(NULLIF(f.volume, 0)) AS log_volume,
        LAG(f.close, 1) OVER (PARTITION BY f.etf ORDER BY f.date) AS prev_close,
        LAG(f.close, 3) OVER (PARTITION BY f.etf ORDER BY f.date) AS close_3d,
        LAG(f.close, 5) OVER (PARTITION BY f.etf ORDER BY f.date) AS close_5d,
        LAG(f.close, 10) OVER (PARTITION BY f.etf ORDER BY f.date) AS close_10d
    FROM etf_flows f
    LEFT JOIN etf_metadata m ON m.etf = f.etf
),
raw AS (
    SELECT
        etf,
        etf_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        log_volume,
        prev_close,
        close_3d,
        close_5d,
        close_10d,
        high - low AS range_1d,
        GREATEST(
            high - low,
            ABS(high - COALESCE(prev_close, close)),
            ABS(low - COALESCE(prev_close, close))
        ) AS true_range,
        COUNT(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5_count,
        AVG(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5_raw,
        COUNT(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS ma_10_count,
        AVG(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS ma_10_raw,
        COUNT(volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_avg_5_count,
        AVG(volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_avg_5_raw,
        COUNT(volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_avg_20_count,
        AVG(volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_avg_20_raw,
        COUNT(*) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS range_avg_5_count,
        AVG(high - low) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS range_avg_5_raw,
        COUNT(*) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14_count,
        AVG(
            GREATEST(
                high - low,
                ABS(high - COALESCE(prev_close, close)),
                ABS(low - COALESCE(prev_close, close))
            )
        ) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14_raw,
        (close - prev_close) / NULLIF(prev_close, 0) AS ret_1d,
        (close - close_3d) / NULLIF(close_3d, 0) AS ret_3d,
        (close - close_5d) / NULLIF(close_5d, 0) AS ret_5d,
        (close - close_10d) / NULLIF(close_10d, 0) AS ret_10d
    FROM base
),
norm AS (
    SELECT
        etf,
        etf_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        log_volume,
        range_1d,
        true_range,
        CASE WHEN ma_5_count = 5 THEN ma_5_raw END AS ma_5,
        CASE WHEN ma_10_count = 10 THEN ma_10_raw END AS ma_10,
        CASE WHEN vol_avg_5_count = 5 THEN vol_avg_5_raw END AS vol_avg_5,
        CASE WHEN vol_avg_20_count = 20 THEN vol_avg_20_raw END AS vol_avg_20,
        CASE WHEN range_avg_5_count = 5 THEN range_avg_5_raw END AS range_avg_5,
        CASE WHEN atr_14_count = 14 THEN atr_14_raw END AS atr_14,
        ret_1d,
        ret_3d,
        ret_5d,
        ret_10d,
        COUNT(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS close_5_prev_count,
        AVG(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS close_mean_5_prev,
        STDDEV_SAMP(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS close_std_5_prev,
        COUNT(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_20_prev_count,
        AVG(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_mean_20_prev,
        STDDEV_SAMP(close) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_std_20_prev,
        COUNT(ret_1d) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_20_prev_count,
        AVG(ret_1d) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_mean_20_prev,
        STDDEV_SAMP(ret_1d) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_std_20_prev,
        COUNT(log_volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_20_prev_count,
        AVG(log_volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_mean_20_prev,
        STDDEV_SAMP(log_volume) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_std_20_prev,
        COUNT(true_range) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS atr_14_prev_count,
        AVG(true_range) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS atr_14_prev,
        COUNT(true_range) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_20_prev_count,
        AVG(true_range) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_mean_20_prev,
        STDDEV_SAMP(true_range) OVER (
            PARTITION BY etf
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_std_20_prev
    FROM raw
)
SELECT
    etf,
    etf_name,
    date,
    open,
    high,
    low,
    close,
    volume,
    ma_5,
    ma_10,
    vol_avg_5,
    vol_avg_20,
    range_1d,
    range_avg_5,
    true_range,
    atr_14,
    ret_1d,
    ret_3d,
    ret_5d,
    ret_10d,
    CASE
        WHEN close_5_prev_count = 5 THEN
            (close - close_mean_5_prev) / NULLIF(close_std_5_prev, 0)
    END AS zscore_5d,
    CASE
        WHEN close_20_prev_count = 20 THEN
            (close - close_mean_20_prev) / NULLIF(close_std_20_prev, 0)
    END AS price_z_20,
    CASE
        WHEN ret_20_prev_count = 20 THEN
            (ret_1d - ret_mean_20_prev) / NULLIF(ret_std_20_prev, 0)
    END AS ret_z_20,
    CASE
        WHEN log_volume_20_prev_count = 20 THEN
            (log_volume - log_volume_mean_20_prev) / NULLIF(log_volume_std_20_prev, 0)
    END AS vol_z_20,
    CASE
        WHEN true_range_20_prev_count = 20 THEN
            (true_range - true_range_mean_20_prev) / NULLIF(true_range_std_20_prev, 0)
    END AS range_z_20,
    CASE
        WHEN atr_14_prev_count = 14 THEN
            (close - ma_5) / NULLIF(atr_14_prev, 0)
    END AS dist_ma_5_atr,
    CASE
        WHEN atr_14_prev_count = 14 THEN
            (close - ma_10) / NULLIF(atr_14_prev, 0)
    END AS dist_ma_10_atr,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_3d / NULLIF(ret_std_20_prev * SQRT(3.0), 0)
    END AS ret_3d_norm,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_5d / NULLIF(ret_std_20_prev * SQRT(5.0), 0)
    END AS ret_5d_norm,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_10d / NULLIF(ret_std_20_prev * SQRT(10.0), 0)
    END AS ret_10d_norm
FROM norm;

CREATE OR REPLACE VIEW v_industry_signals AS
WITH raw AS (
    SELECT
        as_of_date,
        industry,
        rank,
        perf_week,
        perf_month,
        perf_quart,
        perf_half,
        perf_year,
        perf_ytd,
        avg_volume,
        rel_volume,
        change,
        volume,
        CASE
            WHEN avg_volume IS NULL OR avg_volume = 0 THEN NULL
            ELSE volume / avg_volume
        END AS volume_ratio,
        (perf_week - AVG(perf_week) OVER (PARTITION BY as_of_date))
            / NULLIF(STDDEV_SAMP(perf_week) OVER (PARTITION BY as_of_date), 0) AS z_perf_week,
        (perf_month - AVG(perf_month) OVER (PARTITION BY as_of_date))
            / NULLIF(STDDEV_SAMP(perf_month) OVER (PARTITION BY as_of_date), 0) AS z_perf_month,
        (rel_volume - AVG(rel_volume) OVER (PARTITION BY as_of_date))
            / NULLIF(STDDEV_SAMP(rel_volume) OVER (PARTITION BY as_of_date), 0) AS z_rel_volume,
        (change - AVG(change) OVER (PARTITION BY as_of_date))
            / NULLIF(STDDEV_SAMP(change) OVER (PARTITION BY as_of_date), 0) AS z_change
    FROM industry_flows
)
SELECT
    as_of_date,
    industry,
    rank,
    perf_week,
    perf_month,
    perf_quart,
    perf_half,
    perf_year,
    perf_ytd,
    avg_volume,
    rel_volume,
    change,
    volume,
    volume_ratio,
    z_perf_week,
    z_perf_month,
    z_rel_volume,
    z_change,
    GREATEST(LEAST(z_perf_week, 3.0), -3.0) AS z_perf_week_clip,
    GREATEST(LEAST(z_perf_month, 3.0), -3.0) AS z_perf_month_clip,
    GREATEST(LEAST(z_rel_volume, 3.0), -3.0) AS z_rel_volume_clip,
    GREATEST(LEAST(z_change, 3.0), -3.0) AS z_change_clip
FROM raw;

CREATE OR REPLACE VIEW v_etf_signals AS
WITH clipped AS (
    SELECT
        etf,
        etf_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        ma_5,
        ma_10,
        vol_avg_5,
        vol_avg_20,
        range_1d,
        range_avg_5,
        true_range,
        atr_14,
        ret_1d,
        ret_3d,
        ret_5d,
        ret_10d,
        zscore_5d,
        price_z_20,
        ret_z_20,
        vol_z_20,
        range_z_20,
        dist_ma_5_atr,
        dist_ma_10_atr,
        ret_3d_norm,
        ret_5d_norm,
        ret_10d_norm,
        GREATEST(LEAST(price_z_20, 3.0), -3.0) AS price_z_20_clip,
        GREATEST(LEAST(ret_z_20, 3.0), -3.0) AS ret_z_20_clip,
        GREATEST(LEAST(vol_z_20, 3.0), -3.0) AS vol_z_20_clip,
        GREATEST(LEAST(range_z_20, 3.0), -3.0) AS range_z_20_clip,
        GREATEST(LEAST(dist_ma_5_atr, 3.0), -3.0) AS dist_ma_5_atr_clip,
        GREATEST(LEAST(dist_ma_10_atr, 3.0), -3.0) AS dist_ma_10_atr_clip,
        GREATEST(LEAST(ret_3d_norm, 3.0), -3.0) AS ret_3d_norm_clip,
        GREATEST(LEAST(ret_5d_norm, 3.0), -3.0) AS ret_5d_norm_clip,
        GREATEST(LEAST(ret_10d_norm, 3.0), -3.0) AS ret_10d_norm_clip
    FROM v_etf_enriched
)
SELECT
    etf,
    etf_name,
    date,
    open,
    high,
    low,
    close,
    volume,
    ma_5,
    ma_10,
    ret_1d,
    ret_3d,
    ret_5d,
    ret_10d,
    range_1d,
    range_avg_5,
    true_range,
    atr_14,
    vol_avg_5,
    vol_avg_20,
    zscore_5d,
    price_z_20,
    ret_z_20,
    vol_z_20,
    range_z_20,
    dist_ma_5_atr,
    dist_ma_10_atr,
    ret_3d_norm,
    ret_5d_norm,
    ret_10d_norm,
    CASE
        WHEN vol_avg_5 IS NULL OR vol_avg_5 = 0 THEN NULL
        ELSE volume / vol_avg_5
    END AS vol_ratio_5,
    CASE
        WHEN vol_avg_20 IS NULL OR vol_avg_20 = 0 THEN NULL
        ELSE volume / vol_avg_20
    END AS vol_ratio_20,
    CASE
        WHEN range_avg_5 IS NULL OR range_avg_5 = 0 THEN NULL
        ELSE range_1d / range_avg_5
    END AS range_ratio_5,
    CASE
        WHEN ma_5 IS NULL OR ma_5 = 0 THEN NULL
        ELSE (close - ma_5) / ma_5
    END AS dist_ma_5,
    CASE
        WHEN ma_10 IS NULL OR ma_10 = 0 THEN NULL
        ELSE (close - ma_10) / ma_10
    END AS dist_ma_10,
    0.20 * ret_z_20_clip
        + 0.35 * ret_3d_norm_clip
        + 0.45 * ret_5d_norm_clip AS momentum_factor,
    -0.70 * price_z_20_clip
        - 0.30 * dist_ma_5_atr_clip AS reversion_factor,
    0.60 * vol_z_20_clip
        + 0.40 * range_z_20_clip AS activity_factor,
    0.60 * ABS(price_z_20_clip)
        + 0.40 * ABS(dist_ma_5_atr_clip) AS stretch_score
FROM clipped;

CREATE OR REPLACE VIEW v_industry_signal_rank AS
SELECT
    as_of_date,
    industry,
    rank,
    perf_week,
    perf_month,
    perf_quart,
    perf_half,
    perf_year,
    perf_ytd,
    avg_volume,
    rel_volume,
    change,
    volume,
    volume_ratio,
    z_perf_week,
    z_perf_month,
    z_rel_volume,
    z_change,
    z_perf_week_clip,
    z_perf_month_clip,
    z_rel_volume_clip,
    z_change_clip,
    0.55 * z_perf_week_clip
        + 0.45 * z_perf_month_clip AS momentum_factor,
    -0.70 * z_change_clip
        - 0.30 * z_perf_week_clip AS reversion_factor,
    z_rel_volume_clip AS activity_factor,
    0.70 * ABS(z_change_clip)
        + 0.30 * ABS(z_perf_week_clip) AS stretch_score,
    0.80 * (0.55 * z_perf_week_clip + 0.45 * z_perf_month_clip)
        + 0.20 * z_rel_volume_clip AS momentum_score,
    -0.70 * z_change_clip
        - 0.30 * z_perf_week_clip AS mean_reversion_score
FROM v_industry_signals;

CREATE OR REPLACE VIEW v_etf_signal_rank AS
SELECT
    etf,
    etf_name,
    date,
    ret_1d,
    ret_3d,
    ret_5d,
    ret_10d,
    vol_ratio_5,
    vol_ratio_20,
    range_ratio_5,
    dist_ma_5,
    dist_ma_10,
    zscore_5d,
    price_z_20,
    ret_z_20,
    vol_z_20,
    range_z_20,
    dist_ma_5_atr,
    dist_ma_10_atr,
    ret_3d_norm,
    ret_5d_norm,
    ret_10d_norm,
    momentum_factor,
    reversion_factor,
    activity_factor,
    stretch_score,
    0.75 * momentum_factor + 0.25 * activity_factor AS momentum_score,
    reversion_factor AS mean_reversion_score
FROM v_etf_signals;

CREATE OR REPLACE VIEW v_etf_basing AS
SELECT
    etf,
    etf_name,
    date,
    ret_3d,
    ret_5d,
    range_ratio_5,
    dist_ma_5,
    dist_ma_10,
    vol_ratio_5,
    price_z_20,
    range_z_20,
    dist_ma_5_atr,
    momentum_factor,
    activity_factor
FROM v_etf_signals
WHERE
    price_z_20 IS NOT NULL
    AND ret_5d_norm IS NOT NULL
    AND dist_ma_5_atr IS NOT NULL
    AND ABS(price_z_20) <= 0.75
    AND ABS(ret_5d_norm) <= 0.75
    AND range_z_20 <= 0.25
    AND ABS(dist_ma_5_atr) <= 0.50
    AND ABS(momentum_factor) <= 0.75
    AND activity_factor <= 0.50;

CREATE OR REPLACE VIEW v_stock_signals AS
WITH base AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        LN(NULLIF(volume, 0)) AS log_volume,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date) AS close_3d,
        LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
        LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) AS close_10d
    FROM stock_analysis
),
raw AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        log_volume,
        prev_close,
        close_3d,
        close_5d,
        close_10d,
        high - low AS range_1d,
        GREATEST(
            high - low,
            ABS(high - COALESCE(prev_close, close)),
            ABS(low - COALESCE(prev_close, close))
        ) AS true_range,
        COUNT(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS ma_3_count,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS ma_3_raw,
        COUNT(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5_count,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5_raw,
        COUNT(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS ma_10_count,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS ma_10_raw,
        COUNT(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_sum_5_count,
        SUM(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_sum_5_raw,
        COUNT(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_avg_5_count,
        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_avg_5_raw,
        COUNT(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_avg_20_count,
        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_avg_20_raw,
        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14_count,
        AVG(
            GREATEST(
                high - low,
                ABS(high - COALESCE(prev_close, close)),
                ABS(low - COALESCE(prev_close, close))
            )
        ) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14_raw,
        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS range_avg_5_count,
        AVG(high - low) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS range_avg_5_raw,
        (close - prev_close) / NULLIF(prev_close, 0) AS ret_1d,
        (close - close_3d) / NULLIF(close_3d, 0) AS ret_3d,
        (close - close_5d) / NULLIF(close_5d, 0) AS ret_5d,
        (close - close_10d) / NULLIF(close_10d, 0) AS ret_10d
    FROM base
),
norm AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        log_volume,
        range_1d,
        true_range,
        CASE WHEN ma_3_count = 3 THEN ma_3_raw END AS ma_3,
        CASE WHEN ma_5_count = 5 THEN ma_5_raw END AS ma_5,
        CASE WHEN ma_10_count = 10 THEN ma_10_raw END AS ma_10,
        CASE WHEN vol_sum_5_count = 5 THEN vol_sum_5_raw END AS vol_sum_5,
        CASE WHEN vol_avg_5_count = 5 THEN vol_avg_5_raw END AS vol_avg_5,
        CASE WHEN vol_avg_20_count = 20 THEN vol_avg_20_raw END AS vol_avg_20,
        CASE WHEN atr_14_count = 14 THEN atr_14_raw END AS atr_14,
        CASE WHEN range_avg_5_count = 5 THEN range_avg_5_raw END AS range_avg_5,
        ret_1d,
        ret_3d,
        ret_5d,
        ret_10d,
        COUNT(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_20_prev_count,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_mean_20_prev,
        STDDEV_SAMP(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS close_std_20_prev,
        COUNT(ret_1d) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_20_prev_count,
        AVG(ret_1d) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_mean_20_prev,
        STDDEV_SAMP(ret_1d) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS ret_std_20_prev,
        COUNT(log_volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_20_prev_count,
        AVG(log_volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_mean_20_prev,
        STDDEV_SAMP(log_volume) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS log_volume_std_20_prev,
        COUNT(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS atr_14_prev_count,
        AVG(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
        ) AS atr_14_prev,
        COUNT(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_20_prev_count,
        AVG(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_mean_20_prev,
        STDDEV_SAMP(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS true_range_std_20_prev
    FROM raw
)
SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    ma_3,
    ma_5,
    ma_10,
    vol_sum_5,
    vol_avg_5,
    vol_avg_20,
    range_1d,
    range_avg_5,
    true_range,
    atr_14,
    ret_1d,
    ret_3d,
    ret_5d,
    ret_10d,
    CASE
        WHEN close_20_prev_count = 20 THEN
            (close - close_mean_20_prev) / NULLIF(close_std_20_prev, 0)
    END AS price_z_20,
    CASE
        WHEN ret_20_prev_count = 20 THEN
            (ret_1d - ret_mean_20_prev) / NULLIF(ret_std_20_prev, 0)
    END AS ret_z_20,
    CASE
        WHEN log_volume_20_prev_count = 20 THEN
            (log_volume - log_volume_mean_20_prev) / NULLIF(log_volume_std_20_prev, 0)
    END AS vol_z_20,
    CASE
        WHEN true_range_20_prev_count = 20 THEN
            (true_range - true_range_mean_20_prev) / NULLIF(true_range_std_20_prev, 0)
    END AS range_z_20,
    CASE
        WHEN atr_14_prev_count = 14 THEN
            (close - ma_5) / NULLIF(atr_14_prev, 0)
    END AS dist_ma_5_atr,
    CASE
        WHEN atr_14_prev_count = 14 THEN
            (close - ma_10) / NULLIF(atr_14_prev, 0)
    END AS dist_ma_10_atr,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_3d / NULLIF(ret_std_20_prev * SQRT(3.0), 0)
    END AS ret_3d_norm,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_5d / NULLIF(ret_std_20_prev * SQRT(5.0), 0)
    END AS ret_5d_norm,
    CASE
        WHEN ret_20_prev_count = 20 THEN ret_10d / NULLIF(ret_std_20_prev * SQRT(10.0), 0)
    END AS ret_10d_norm,
    CASE
        WHEN vol_avg_5 IS NULL OR vol_avg_5 = 0 THEN NULL
        ELSE volume / vol_avg_5
    END AS vol_ratio_5,
    CASE
        WHEN vol_avg_20 IS NULL OR vol_avg_20 = 0 THEN NULL
        ELSE volume / vol_avg_20
    END AS vol_ratio_20,
    CASE
        WHEN range_avg_5 IS NULL OR range_avg_5 = 0 THEN NULL
        ELSE range_1d / range_avg_5
    END AS range_ratio_5,
    CASE
        WHEN ma_5 IS NULL OR ma_5 = 0 THEN NULL
        ELSE (close - ma_5) / ma_5
    END AS dist_ma_5,
    CASE
        WHEN ma_10 IS NULL OR ma_10 = 0 THEN NULL
        ELSE (close - ma_10) / ma_10
    END AS dist_ma_10
FROM norm;

CREATE OR REPLACE VIEW v_stock_signal_rank AS
WITH clipped AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        ma_3,
        ma_5,
        ma_10,
        vol_sum_5,
        vol_avg_5,
        vol_avg_20,
        range_1d,
        range_avg_5,
        true_range,
        atr_14,
        ret_1d,
        ret_3d,
        ret_5d,
        ret_10d,
        price_z_20,
        ret_z_20,
        vol_z_20,
        range_z_20,
        dist_ma_5_atr,
        dist_ma_10_atr,
        ret_3d_norm,
        ret_5d_norm,
        ret_10d_norm,
        vol_ratio_5,
        vol_ratio_20,
        range_ratio_5,
        dist_ma_5,
        dist_ma_10,
        GREATEST(LEAST(price_z_20, 3.0), -3.0) AS price_z_20_clip,
        GREATEST(LEAST(ret_z_20, 3.0), -3.0) AS ret_z_20_clip,
        GREATEST(LEAST(vol_z_20, 3.0), -3.0) AS vol_z_20_clip,
        GREATEST(LEAST(range_z_20, 3.0), -3.0) AS range_z_20_clip,
        GREATEST(LEAST(dist_ma_5_atr, 3.0), -3.0) AS dist_ma_5_atr_clip,
        GREATEST(LEAST(ret_3d_norm, 3.0), -3.0) AS ret_3d_norm_clip,
        GREATEST(LEAST(ret_5d_norm, 3.0), -3.0) AS ret_5d_norm_clip,
        GREATEST(LEAST(ret_10d_norm, 3.0), -3.0) AS ret_10d_norm_clip
    FROM v_stock_signals
)
SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    ma_3,
    ma_5,
    ma_10,
    vol_sum_5,
    vol_avg_5,
    vol_avg_20,
    range_1d,
    range_avg_5,
    true_range,
    atr_14,
    ret_1d,
    ret_3d,
    ret_5d,
    ret_10d,
    price_z_20,
    ret_z_20,
    vol_z_20,
    range_z_20,
    dist_ma_5_atr,
    dist_ma_10_atr,
    ret_3d_norm,
    ret_5d_norm,
    ret_10d_norm,
    vol_ratio_5,
    vol_ratio_20,
    range_ratio_5,
    dist_ma_5,
    dist_ma_10,
    0.20 * ret_z_20_clip
        + 0.35 * ret_3d_norm_clip
        + 0.45 * ret_5d_norm_clip AS momentum_factor,
    -0.70 * price_z_20_clip
        - 0.30 * dist_ma_5_atr_clip AS reversion_factor,
    0.60 * vol_z_20_clip
        + 0.40 * range_z_20_clip AS activity_factor,
    0.60 * ABS(price_z_20_clip)
        + 0.40 * ABS(dist_ma_5_atr_clip) AS stretch_score,
    0.75 * (
        0.20 * ret_z_20_clip
        + 0.35 * ret_3d_norm_clip
        + 0.45 * ret_5d_norm_clip
    ) + 0.25 * (
        0.60 * vol_z_20_clip
        + 0.40 * range_z_20_clip
    ) AS momentum_score,
    -0.70 * price_z_20_clip
        - 0.30 * dist_ma_5_atr_clip AS mean_reversion_score
FROM clipped;
