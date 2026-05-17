
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
