-- Insert missing iShares MSCI country ETFs (EW* series)
-- Run against: financials_dev or financials

BEGIN;

INSERT INTO etf_metadata (symbol, display_name, country, region, asset_class) VALUES
  ('EWA', 'Australia',        'Australia',       'Asia Pacific',  'Equity'),
  ('EWC', 'Canada',           'Canada',          'North America', 'Equity'),
  ('EWH', 'Hong Kong',        'Hong Kong',       'Asia',          'Equity'),
  ('EWI', 'Italy',            'Italy',           'Europe',        'Equity'),
  ('EWL', 'Switzerland',      'Switzerland',     'Europe',        'Equity'),
  ('EWM', 'Malaysia',         'Malaysia',        'Asia',          'Equity'),
  ('EWP', 'Spain',            'Spain',           'Europe',        'Equity'),
  ('EWS', 'Singapore',        'Singapore',       'Asia',          'Equity'),
  ('EWT', 'Taiwan',           'Taiwan',          'Asia',          'Equity'),
  ('EWY', 'South Korea',      'South Korea',     'Asia',          'Equity')
ON CONFLICT (symbol) DO NOTHING;

INSERT INTO etf_universe (etf, active) VALUES
  ('EWA', true),
  ('EWC', true),
  ('EWH', true),
  ('EWI', true),
  ('EWL', true),
  ('EWM', true),
  ('EWP', true),
  ('EWS', true),
  ('EWT', true),
  ('EWY', true)
ON CONFLICT (etf) DO NOTHING;

COMMIT;
