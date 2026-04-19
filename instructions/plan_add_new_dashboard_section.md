# Adding a New Dashboard Section

## Steps

- Add a `SectionConfig` entry to `SECTIONS` in `dashboard/sections.py` with `key`, `title`, `description`, `type="table"`, and `source` matching the view name
- Insert a row into `config.etf_dashboard_section_config` with `section_key`, `columns` (ordered array), and `column_labels` (JSONB, keys must match columns exactly)
- If the view introduces new numeric columns, add them to the appropriate formatting sets in `sections.py` (`PERCENT_COLUMNS`, `SIGNED_COLUMNS`, `DECIMAL_2_COLUMNS`, `DECIMAL_3_COLUMNS`)
- If the view returns `Decimal` types (from `round()` or `::numeric`), ensure they are converted to `float` before JSON serialization
- Materialize the view if needed — add to `sql/materialized_views.sql` and the refresh function

## Notes

- The `columns` and `column_labels` in `config.etf_dashboard_section_config` are validated at startup against the actual view columns — mismatches raise errors
- Standard table sections must have a `rank` column and are ordered by `rank`
- Non-standard layouts (grouped tables, custom rendering) require changes to `dashboard.js` and possibly `dashboard/render.py`
