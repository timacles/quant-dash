\set ON_ERROR_STOP on

BEGIN;

CREATE TEMP TABLE etf_metadata_stage (
    LIKE public.etf_metadata INCLUDING DEFAULTS
) ON COMMIT DROP;

CREATE TEMP TABLE etf_universe_stage (
    LIKE public.etf_universe INCLUDING DEFAULTS
) ON COMMIT DROP;

\copy etf_metadata_stage (
    symbol,
    display_name,
    asset_class,
    theme_type,
    sector,
    industry,
    region,
    country,
    style,
    commodity_group,
    duration_bucket,
    credit_bucket,
    risk_bucket,
    benchmark_group,
    benchmark_symbol,
    is_macro_reference
) FROM :'metadata_csv' WITH (FORMAT csv, HEADER true)

\copy etf_universe_stage (
    etf,
    active
) FROM :'universe_csv' WITH (FORMAT csv, HEADER true)

INSERT INTO public.etf_metadata AS target (
    symbol,
    display_name,
    asset_class,
    theme_type,
    sector,
    industry,
    region,
    country,
    style,
    commodity_group,
    duration_bucket,
    credit_bucket,
    risk_bucket,
    benchmark_group,
    benchmark_symbol,
    is_macro_reference
)
SELECT
    symbol,
    display_name,
    asset_class,
    theme_type,
    sector,
    industry,
    region,
    country,
    style,
    commodity_group,
    duration_bucket,
    credit_bucket,
    risk_bucket,
    benchmark_group,
    benchmark_symbol,
    is_macro_reference
FROM etf_metadata_stage
ON CONFLICT (symbol) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    asset_class = EXCLUDED.asset_class,
    theme_type = EXCLUDED.theme_type,
    sector = EXCLUDED.sector,
    industry = EXCLUDED.industry,
    region = EXCLUDED.region,
    country = EXCLUDED.country,
    style = EXCLUDED.style,
    commodity_group = EXCLUDED.commodity_group,
    duration_bucket = EXCLUDED.duration_bucket,
    credit_bucket = EXCLUDED.credit_bucket,
    risk_bucket = EXCLUDED.risk_bucket,
    benchmark_group = EXCLUDED.benchmark_group,
    benchmark_symbol = EXCLUDED.benchmark_symbol,
    is_macro_reference = EXCLUDED.is_macro_reference;

INSERT INTO public.etf_universe AS target (
    etf,
    active
)
SELECT
    etf,
    active
FROM etf_universe_stage
ON CONFLICT (etf) DO UPDATE
SET
    active = EXCLUDED.active;

COMMIT;
