-- Fix missing etf_universe and etf_metadata entries
-- Identified by check_data_consistency.py gaps on 2026-05-10
--
-- 14 symbols exist in etf_flows but are absent from both etf_universe and etf_metadata.
-- This happens when data ingestion ran but the universe/registration step was skipped.
--
-- Run: psql -h <host> -d <dbname> -f sql/fix_missing_universe_metadata.sql
-- Target: DEV (192.168.50.237:financials_dev) then PROD (192.168.50.5:financials)

BEGIN;

-- ──────────────────────────────────────────────
-- 1. Register in etf_universe
-- ──────────────────────────────────────────────
INSERT INTO etf_universe (etf, active) VALUES
    ('DIA',  true),
    ('IWD',  true),
    ('IYT',  true),
    ('KRE',  true),
    ('RSP',  true),
    ('SGDJ', true),
    ('SPHB', true),
    ('SPLV', true),
    ('UTWO', true),
    ('VIXM', true),
    ('VIXY', true),
    ('VUG',  true),
    ('WOOD', true),
    ('XHB',  true)
ON CONFLICT (etf) DO NOTHING;

-- ──────────────────────────────────────────────
-- 2. Add metadata entries
-- ──────────────────────────────────────────────
INSERT INTO etf_metadata (
    symbol, display_name, asset_class, theme_type, sector, industry,
    region, country, style, commodity_group, duration_bucket,
    credit_bucket, risk_bucket, benchmark_group, benchmark_symbol,
    is_macro_reference
) VALUES
    -- DIA: Dow Jones Industrial Average — 30 blue-chip US stocks
    ('DIA',  'SPDR Dow Jones Industrial Average ETF',           'equity', 'broad_market',       'multi-sector', 'large_cap',
     'US',   'US',   'large_cap_blend',   NULL, NULL, NULL, 'medium',       'DJIA',  'DIA',  false),

    -- IWD: Russell 1000 Value — large-cap US value stocks
    ('IWD',  'iShares Russell 1000 Value ETF',                  'equity', 'broad_market',       'multi-sector', 'large_cap',
     'US',   'US',   'large_cap_value',   NULL, NULL, NULL, 'medium',       'Russell 1000',  'IWD',  false),

    -- IYT: Transportation — airlines, rails, trucking
    ('IYT',  'iShares US Transportation ETF',                   'equity', 'sector',             'transportation', 'broad_transportation',
     'US',   'US',   'cyclical',          NULL, NULL, NULL, 'high_risk',    NULL,            NULL,   false),

    -- KRE: Regional banks
    ('KRE',  'SPDR S&P Regional Banking ETF',                   'equity', 'sector',             'financials', 'regional_banks',
     'US',   'US',   'value_cyclical',    NULL, NULL, NULL, 'high_risk',    NULL,            NULL,   false),

    -- RSP: S&P 500 Equal Weight — broad market equal-weight
    ('RSP',  'Invesco S&P 500 Equal Weight ETF',               'equity', 'broad_market',       'multi-sector', 'large_cap',
     'US',   'US',   'large_cap_blend',   NULL, NULL, NULL, 'medium',       'S&P 500 EW',    'SPY',  false),

    -- SGDJ: Sprott Junior Gold Miners
    ('SGDJ', 'Sprott Junior Gold Miners ETF',                   'equity', 'gold_miners',        'materials', 'gold_mining',
     'Global', NULL, 'small_cap_cyclical', NULL, NULL, NULL, 'high_risk',    NULL,            'GDX',  false),

    -- SPHB: S&P 500 High Beta — high-volatility large caps
    ('SPHB', 'Invesco S&P 500 High Beta ETF',                  'equity', 'factor',             'multi-sector', 'large_cap',
     'US',   'US',   'high_beta',         NULL, NULL, NULL, 'high_risk',    'S&P 500 Factor', 'SPY', false),

    -- SPLV: S&P 500 Low Volatility — low-volatility large caps
    ('SPLV', 'Invesco S&P 500 Low Volatility ETF',             'equity', 'factor',             'multi-sector', 'large_cap',
     'US',   'US',   'low_volatility',    NULL, NULL, NULL, 'low',          'S&P 500 Factor', 'SPY', false),

    -- UTWO: US Treasury 2 Year Note — short duration treasuries
    ('UTWO', 'US Treasury 2 Year Note ETF',                    'bonds',  'treasury_short',     NULL, NULL,
     'US',   'US',   'sovereign',          NULL, 'short',   'government', 'low',           'US Treasury', 'UTWO', false),

    -- VIXM: VIX Mid-Term Futures — vol carry, macro reference
    ('VIXM', 'ProShares VIX Mid-Term Futures ETF',              'equity', 'volatility',         'financials', 'volatility_futures',
     'US',   'US',   'tactical',           NULL, NULL, NULL, 'high_risk',    NULL,           NULL,    true),

    -- VIXY: VIX Short-Term Futures — vol crisis gauge, macro reference
    ('VIXY', 'ProShares VIX Short-Term Futures ETF',            'equity', 'volatility',         'financials', 'volatility_futures',
     'US',   'US',   'tactical',           NULL, NULL, NULL, 'high_risk',    NULL,           NULL,    true),

    -- VUG: Vanguard Growth — large-cap US growth stocks
    ('VUG',  'Vanguard Growth ETF',                            'equity', 'style',              'multi-sector', 'large_cap',
     'US',   'US',   'large_cap_growth',  NULL, NULL, NULL, 'medium',       'Russell 1000 Growth', 'VUG', false),

    -- WOOD: iShares Global Timber & Forestry
    ('WOOD', 'iShares Global Timber & Forestry ETF',            'equity', 'thematic',           'materials', 'timber_forestry',
     'Global', NULL, 'natural_resources', 'Timber', NULL, NULL, 'high_risk',  NULL,           NULL,    false),

    -- XHB: SPDR S&P Homebuilders — US homebuilding sector
    ('XHB',  'SPDR S&P Homebuilders ETF',                      'equity', 'sector',             'homebuilding', 'residential_construction',
     'US',   'US',   'cyclical',          NULL, NULL, NULL, 'high_risk',    NULL,           NULL,    false)
ON CONFLICT (symbol) DO NOTHING;

COMMIT;

-- ──────────────────────────────────────────────
-- Verification queries (run after commit)
-- ──────────────────────────────────────────────
-- \echo '--- Newly added symbols in etf_universe ---'
-- SELECT u.etf, u.active, m.display_name
-- FROM etf_universe u
-- LEFT JOIN etf_metadata m ON m.symbol = u.etf
-- WHERE u.etf IN ('DIA','IWD','IYT','KRE','RSP','SGDJ','SPHB','SPLV','UTWO','VIXM','VIXY','VUG','WOOD','XHB')
-- ORDER BY u.etf;
--
-- \echo '--- Running check_data_consistency.py again ---'
-- \! ./helpers/check_data_consistency.py gaps
