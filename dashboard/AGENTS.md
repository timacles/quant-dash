
# Python backend files for the dashboard


  - `dashboard/` package — all dashboard logic lives here:
    - `config.py` — `load_config`, `resolve_database_config`, `build_connection_kwargs`
    - `sections.py` — `SectionConfig` dataclasses, `SECTIONS` registry, column classification sets
    - `db.py` — all `fetch_*` functions, `update_section_config`, `serialize_date`, `parse_limit`, `get_section`
    - `render.py` — `render_*`, `format_*`, `value_*` helpers, `build_page`
    - `routes.py` — one handler per URL path; static file serving
    - `static/` — `dashboard.css`, `dashboard.js` (served at `/static/`)
    - `templates/` — `pull_stats.html`, `config.html`


### Add a new route
1. Add a handler function in `dashboard/routes.py`
2. Register it with one `if path == "..."` line in `app()` in `serve_dashboard.py`
3. For routes that support both GET and POST, dispatch on `environ.get("REQUEST_METHOD")` inside the `app()` block


### Change styles or client-side behaviour
- Edit `dashboard/static/dashboard.css` or `dashboard/static/dashboard.js` directly
- No Python changes needed

## Extension Recipes

### Add a new dashboard section
1. Add a `SectionConfig` entry to `SECTIONS` in `dashboard/sections.py`
2. Create the backing SQL view in `sql/` (follow the naming pattern `vw_etf_report_<name>.sql`) and apply it to DEV
3. Insert a row into `config.etf_dashboard_section_config`:
   ```sql
   INSERT INTO config.etf_dashboard_section_config (section_key, columns, column_labels)
   VALUES (
     'my_section',
     ARRAY['rank', 'symbol', 'display_name', ...],
     '{"rank":"Rank","symbol":"Ticker","display_name":"Name",...}'::jsonb
   );
   ```
   `columns` and `column_labels` must have the same keys. Source: `sql/config_app.sql` for examples.
