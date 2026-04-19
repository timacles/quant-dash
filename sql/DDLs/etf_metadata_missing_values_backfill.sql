BEGIN;

-- Conservative metadata backfill for high-confidence gaps found in PROD.
-- Intentional blanks remain for dimensions that are not a good fit or would
-- require inventing new taxonomy labels.
--
-- Left blank intentionally:
-- - sector/industry for broad bond funds and broad regional/country index ETFs
-- - industry for sector ETFs where sector is the right level of detail
-- - country for global, multi-country, and commodity exposures
-- - duration_bucket and credit_bucket for non-bond ETFs
-- - commodity_group for plain equity ETFs without a direct commodity linkage
-- - benchmark_group where the current taxonomy does not cleanly cover the index family
-- - theme_type, benchmark_symbol, and other residual fields that would require weak inference

UPDATE public.etf_metadata
SET
    display_name = CASE symbol
        WHEN 'BSV' THEN 'Vanguard Short-Term Bond ETF'
        WHEN 'QQQ' THEN 'Invesco QQQ Trust'
        WHEN 'SHY' THEN 'iShares 1-3 Year Treasury Bond ETF'
        WHEN 'SMH' THEN 'VanEck Semiconductor ETF'
        ELSE display_name
    END,
    asset_class = CASE symbol
        WHEN 'BSV' THEN 'bonds'
        WHEN 'QQQ' THEN 'equity'
        WHEN 'SHY' THEN 'bonds'
        WHEN 'SMH' THEN 'equity'
        ELSE asset_class
    END,
    theme_type = CASE symbol
        WHEN 'SMH' THEN 'thematic'
        ELSE theme_type
    END,
    sector = CASE symbol
        WHEN 'SMH' THEN 'Technology'
        ELSE sector
    END,
    region = CASE symbol
        WHEN 'BSV' THEN 'US'
        WHEN 'QQQ' THEN 'US'
        WHEN 'SHY' THEN 'US'
        ELSE region
    END,
    country = CASE symbol
        WHEN 'BSV' THEN 'US'
        WHEN 'QQQ' THEN 'US'
        WHEN 'SHY' THEN 'US'
        ELSE country
    END,
    style = CASE symbol
        WHEN 'BSV' THEN 'investment_grade'
        WHEN 'QQQ' THEN 'growth'
        WHEN 'SHY' THEN 'sovereign'
        WHEN 'SMH' THEN 'high_beta_growth'
        ELSE style
    END,
    commodity_group = CASE symbol
        WHEN 'GDX' THEN 'Precious Metals'
        ELSE commodity_group
    END,
    credit_bucket = CASE symbol
        WHEN 'SHY' THEN 'government'
        ELSE credit_bucket
    END,
    risk_bucket = CASE symbol
        WHEN 'BSV' THEN 'low'
        WHEN 'QQQ' THEN 'high'
        WHEN 'SHY' THEN 'low'
        WHEN 'SMH' THEN 'high_risk'
        ELSE risk_bucket
    END,
    benchmark_group = CASE symbol
        WHEN 'EWG' THEN 'MSCI'
        WHEN 'EWJ' THEN 'MSCI'
        WHEN 'EWU' THEN 'MSCI'
        WHEN 'EWZ' THEN 'MSCI'
        WHEN 'IEFA' THEN 'MSCI'
        WHEN 'INDA' THEN 'MSCI'
        WHEN 'JNK' THEN 'Bloomberg'
        ELSE benchmark_group
    END
WHERE symbol IN (
    'BSV', 'EWG', 'EWJ', 'EWU', 'EWZ', 'GDX', 'IEFA', 'INDA', 'JNK', 'QQQ',
    'SHY', 'SMH'
);

COMMIT;
