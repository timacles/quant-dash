BEGIN;

UPDATE public.etf_metadata
SET benchmark_symbol = CASE symbol
    WHEN 'AGG' THEN 'AGG'
    WHEN 'BND' THEN 'AGG'
    WHEN 'CORN' THEN 'DBA'
    WHEN 'CPER' THEN 'DBB'
    WHEN 'DBA' THEN 'DBA'
    WHEN 'DBB' THEN 'DBB'
    WHEN 'DBC' THEN 'DBC'
    WHEN 'EEM' THEN 'EEM'
    WHEN 'EFA' THEN 'EFA'
    WHEN 'EMB' THEN 'EMB'
    WHEN 'EWG' THEN 'EFA'
    WHEN 'EWJ' THEN 'EFA'
    WHEN 'EWU' THEN 'EFA'
    WHEN 'EWZ' THEN 'EEM'
    WHEN 'FXI' THEN 'EEM'
    WHEN 'GDX' THEN 'GLD'
    WHEN 'GLD' THEN 'GLD'
    WHEN 'HYG' THEN 'HYG'
    WHEN 'IBIT' THEN 'IBIT'
    WHEN 'IEF' THEN 'IEF'
    WHEN 'IEFA' THEN 'EFA'
    WHEN 'INDA' THEN 'EEM'
    WHEN 'JNK' THEN 'HYG'
    WHEN 'LQD' THEN 'LQD'
    WHEN 'PALL' THEN 'GLD'
    WHEN 'PDBC' THEN 'DBC'
    WHEN 'PPLT' THEN 'GLD'
    WHEN 'SLV' THEN 'GLD'
    WHEN 'SOYB' THEN 'DBA'
    WHEN 'SPY' THEN 'SPY'
    WHEN 'TIP' THEN 'TIP'
    WHEN 'TLT' THEN 'TLT'
    WHEN 'UNG' THEN 'DBC'
    WHEN 'URA' THEN 'XLE'
    WHEN 'USO' THEN 'USO'
    WHEN 'UUP' THEN 'UUP'
    WHEN 'VEA' THEN 'EFA'
    WHEN 'VWO' THEN 'EEM'
    WHEN 'WEAT' THEN 'DBA'
    WHEN 'XLB' THEN 'SPY'
    WHEN 'XLC' THEN 'SPY'
    WHEN 'XLE' THEN 'SPY'
    WHEN 'XLF' THEN 'SPY'
    WHEN 'XLI' THEN 'SPY'
    WHEN 'XLK' THEN 'SPY'
    WHEN 'XLP' THEN 'SPY'
    WHEN 'XLRE' THEN 'SPY'
    WHEN 'XLU' THEN 'SPY'
    WHEN 'XLV' THEN 'SPY'
    WHEN 'XLY' THEN 'SPY'
    ELSE benchmark_symbol
END
WHERE symbol IN (
    'AGG', 'BND', 'CORN', 'CPER', 'DBA', 'DBB', 'DBC', 'EEM', 'EFA', 'EMB',
    'EWG', 'EWJ', 'EWU', 'EWZ', 'FXI', 'GDX', 'GLD', 'HYG', 'IBIT', 'IEF',
    'IEFA', 'INDA', 'JNK', 'LQD', 'PALL', 'PDBC', 'PPLT', 'SLV', 'SOYB', 'SPY',
    'TIP', 'TLT', 'UNG', 'URA', 'USO', 'UUP', 'VEA', 'VWO', 'WEAT', 'XLB',
    'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY'
);

COMMIT;
