CREATE TABLE IF NOT EXISTS stock_analysis (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    CONSTRAINT stock_analysis_date_symbol_uniq UNIQUE (date, symbol)
);
