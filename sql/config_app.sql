BEGIN;

CREATE SCHEMA IF NOT EXISTS config;

CREATE TABLE IF NOT EXISTS config.etf_dashboard_section_config (
    section_key text PRIMARY KEY,
    columns text[] NOT NULL,
    column_labels jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT etf_dashboard_section_config_columns_nonempty CHECK (cardinality(columns) > 0),
    CONSTRAINT etf_dashboard_section_config_column_labels_object CHECK (jsonb_typeof(column_labels) = 'object')
);

INSERT INTO config.etf_dashboard_section_config (section_key, columns, column_labels)
VALUES
    (
        'momentum_longs',
        ARRAY['rank', 'symbol', 'display_name', 'ret_1d', 'ret_3d', 'ret_5d', 'ret_10d', 'rvol_20', 'volume_ratio_5_20', 'close_location_20', 'zscore_close_20', 'composite_score'],
        jsonb_build_object(
            'rank', 'Rank',
            'symbol', 'Ticker',
            'display_name', 'Name',
            'ret_1d', '1D',
            'ret_3d', '3D',
            'ret_5d', '5D',
            'ret_10d', '10D',
            'rvol_20', '20D RVOL',
            'volume_ratio_5_20', '5D / 20D Vol',
            'close_location_20', 'Close Loc 20',
            'zscore_close_20', 'Z-Score 20',
            'composite_score', 'Score'
        )
    ),
    (
        'momentum_shorts',
        ARRAY['rank', 'symbol', 'display_name', 'ret_1d', 'ret_3d', 'ret_5d', 'ret_10d', 'rvol_20', 'volume_ratio_5_20', 'close_location_20', 'composite_score'],
        jsonb_build_object(
            'rank', 'Rank',
            'symbol', 'Ticker',
            'display_name', 'Name',
            'ret_1d', '1D',
            'ret_3d', '3D',
            'ret_5d', '5D',
            'ret_10d', '10D',
            'rvol_20', '20D RVOL',
            'volume_ratio_5_20', '5D / 20D Vol',
            'close_location_20', 'Close Loc 20',
            'composite_score', 'Score'
        )
    ),
    (
        'oversold_mean_reversion',
        ARRAY['rank', 'symbol', 'display_name', 'ret_1d', 'ret_3d', 'ret_5d', 'zscore_close_20', 'atr_stretch_20', 'close_location_20', 'volume_ratio_5_20', 'rvol_20', 'composite_score'],
        jsonb_build_object(
            'rank', 'Rank',
            'symbol', 'Ticker',
            'display_name', 'Name',
            'ret_1d', '1D',
            'ret_3d', '3D',
            'ret_5d', '5D',
            'zscore_close_20', 'Z-Score 20',
            'atr_stretch_20', 'ATR Stretch 20',
            'close_location_20', 'Close Loc 20',
            'volume_ratio_5_20', '5D / 20D Vol',
            'rvol_20', '20D RVOL',
            'composite_score', 'Score'
        )
    ),
    (
        'overbought_mean_reversion',
        ARRAY['rank', 'symbol', 'display_name', 'ret_1d', 'ret_3d', 'ret_5d', 'zscore_close_20', 'atr_stretch_20', 'close_location_20', 'volume_ratio_5_20', 'rvol_20', 'composite_score'],
        jsonb_build_object(
            'rank', 'Rank',
            'symbol', 'Ticker',
            'display_name', 'Name',
            'ret_1d', '1D',
            'ret_3d', '3D',
            'ret_5d', '5D',
            'zscore_close_20', 'Z-Score 20',
            'atr_stretch_20', 'ATR Stretch 20',
            'close_location_20', 'Close Loc 20',
            'volume_ratio_5_20', '5D / 20D Vol',
            'rvol_20', '20D RVOL',
            'composite_score', 'Score'
        )
    ),
    (
        'range_compression',
        ARRAY['rank', 'symbol', 'display_name', 'ret_1d', 'ret_3d', 'ret_5d', 'range_compression_5_20', 'range_compression_5_60', 'atr_compression_5_20', 'close_location_20', 'volume_ratio_5_20', 'composite_score'],
        jsonb_build_object(
            'rank', 'Rank',
            'symbol', 'Ticker',
            'display_name', 'Name',
            'ret_1d', '1D',
            'ret_3d', '3D',
            'ret_5d', '5D',
            'range_compression_5_20', 'Range 5/20',
            'range_compression_5_60', 'Range 5/60',
            'atr_compression_5_20', 'ATR 5/20',
            'close_location_20', 'Close Loc 20',
            'volume_ratio_5_20', '5D / 20D Vol',
            'composite_score', 'Score'
        )
    )
ON CONFLICT (section_key) DO UPDATE
SET
    columns = EXCLUDED.columns,
    column_labels = EXCLUDED.column_labels,
    updated_at = now();

COMMIT;
