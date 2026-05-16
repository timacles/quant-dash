# Skill: Unused Object Housekeeping

A repeatable workflow for auditing the SQL dependency chain, generating a visual dependency diagram, identifying objects not used by the dashboard, and producing dated DROP scripts.

---

## Trigger

Use this skill when asked any of:
- "generate a dependency diagram"
- "find unused objects"
- "highlight what's not used by the dashboard"
- "generate drop statements for unused objects"
- "housekeeping / cleanup the SQL schema"
- Any audit of what the dashboard actually queries

---

## Workflow

### Step 1 — Trace the full dependency chain

Start at the Python layer and work down to base tables.

```
serve_dashboard.py
  → dashboard/routes.py (route handlers)
    → dashboard/db.py (fetch_*, update_*, resolve_*)
      → database objects queried directly
        → MV's
          → upstream views
            → vw_etf_daily_features (central hub)
              → base tables (etf_flows, etf_metadata, etf_ranking_config)
```

Files to read:

| File | Purpose |
|------|---------|
| `serve_dashboard.py` | WSGI router — lists all URL paths |
| `dashboard/sections.py` | `SECTIONS` tuple — defines which MV's are displayed (`source` field) |
| `dashboard/db.py` | All `fetch_*` functions — shows every SQL query the dashboard runs |
| `dashboard/routes.py` | Route handlers that call db.py functions |
| `sql/config_app.sql` | `config.etf_dashboard_section_config` DDL + seed data |
| `sql/materialized_views.sql` | All MV definitions + `refresh_etf_matviews()` |
| `sql/etf_ranking_views.sql` | View definitions for scoring, regime, ranked lists |
| `sql/macro_signal_views.sql` | Macro view chain (cluster, bonds, ratios, signal dashboard) |
| `sql/macro_signal_table.sql` | `vw_macro_signal_table` |
| `sql/json_views.sql` | JSON/LLM output views |
| `sql/vw_llm_market_summary.sql` | LLM summary view |
| `sql/DDLs/desk_views.sql` | Legacy `v_*` view chain |
| `sql/DDLs/etf_universe_seed.sql` | `etf_universe` table |
| `sql/instructions.md` | Deployment notes and object relationships |

Classify every database object into one of:

| Status | Meaning |
|--------|---------|
| **DIRECT** | Queried directly by Python db.py |
| **DEP** | Transitive SQL dependency of a DIRECT object |
| **UNUSED** | Not queried by the dashboard (directly or transitively) |

Objects queried by `db.py`:
- `config.etf_dashboard_section_config` — column config
- `mv_etf_report_<name>` for each section in `SECTIONS` (5 MVs)
- `mv_macro_signal_table` — via `fetch_macro_summary()`
- `etf_analysis` — via `fetch_analysis_row()`
- `pg_attribute` + `pg_class` + `pg_namespace` — column discovery

All 5 SECTIONS are defined in `dashboard/sections.py`. Any MV defined in `materialized_views.sql` but *not* listed as a section `source` is unused (e.g. `mv_etf_report_bond_credit_performance`).

Any MV that is only consumed by another unused MV is itself unused (e.g. `mv_macro_signal_dashboard`, `mv_macro_bond_treasury_summary`).

Any view that is only consumed by an unused MV/view is also unused (e.g. `vw_macro_signal_dashboard`, `vw_macro_cluster_momentum`, `vw_macro_bond_treasury_buckets`, `vw_macro_bond_treasury_summary`).

Any view not referenced by anything in the DIRECT/DEP closure is unused (e.g. JSON views, LLM views, legacy DDL `v_*` views, ranking views superseded by MVs).

### Step 2 — Generate the HTML dependency diagram

Create or update `docs/dependency_diagram.html`:

1. The diagram reads top-to-bottom: Python → Frontend → Config → Materialized Views → SQL Views → Central Hub (`vw_etf_daily_features`) → Base Tables → Ingestion.

2. Each layer is a horizontal band. Objects are nodes with colour-coded left borders:
   - Yellow (`#f7c948`): Python
   - Orange (`#ed8936`): HTML/Frontend
   - Purple (`#9f7aea`): Materialized View
   - Blue (`#63b3ed`): SQL View
   - Red (`#f56565`): Base Table

3. Unused objects get the CSS class `node-unused`:
   - Muted border (`#2d3748`) instead of colour
   - Opacity 0.7
   - `.name` text is strikethrough in grey (`#6b7280`)
   - Red `NOT USED` badge vs green `USED` badge on active objects

4. CSS classes needed:
   ```css
   .node-unused {
     border-color: #2d3748 !important;
     background: #0f1422;
     opacity: 0.7;
   }
   .node-unused .name {
     color: #6b7280 !important;
     text-decoration: line-through;
     text-decoration-color: #4a5568;
   }
   .node-unused .detail { color: #4a5568 !important; }
   .node-unused .file { color: #374151 !important; }
   .node-unused .status-tag {
     background: #3a1a1a;
     color: #fc8181;
     border: 1px solid #742a2a;
   }
   ```

5. Status tags on every node:
   ```html
   <span class="status-tag" style="background:#1a3a2a;color:#68d391;">USED</span>
   <span class="status-tag">NOT USED</span>
   ```

6. Use `group-box` elements (dashed borders) to group related objects (e.g. "5 ETF Report Section Sources — USED", "Legacy DDL Views — ALL UNUSED").

7. Connectors between layers use CSS pseudo-elements (`::before` for the stem line, `::after` for the arrowhead). For multi-fan-out use `connector-group` with individual `c-line` divs.

8. Add a legend at the top and a note at the bottom listing all unused objects with counts.

### Step 3 — Catalog unused objects

Update or create `instructions/unused_sql_objects.md` with the canonical catalog table.

Table format per SQL file:

```
## `sql/<file>.sql`

| Object | Type | Status | Notes |
|--------|------|--------|-------|
| `name` | TABLE/VIEW/MATVIEW/FUNCTION | **DIRECT** / **DEP** / **UNUSED** / **REFRESHED** / **UTILITY** | Why |
```

Summary section at the bottom with counts per category.

### Step 4 — Generate dated DROP SQL

Create `sql/drop_unused_objects_<YYYYMMDD>.sql`.

Drop order (reverse of creation order, respecting dependencies):

```
PHASE 1 — Materialized Views (depend on views)
  mv_* that are UNUSED

PHASE 2 — Regular Views (depend on other views/tables)
  vw_* / v_* that are UNUSED, in reverse dependency order

PHASE 3 — Functions
  Functions that only reference dropped objects

PHASE 4 — Tables
  Tables that are UNUSED (be extra careful — ask if uncertain)
```

Each DROP uses:
```sql
DROP <TYPE> IF EXISTS <schema>.<name> CASCADE;
```

Include a `COMMIT;` at the end.

Add a post-drop verification comment listing all objects that should *still* exist.

**Important**: If `refresh_etf_matviews()` function references dropped MVs, add a note about which `REFRESH MATERIALIZED VIEW` lines to remove from `sql/materialized_views.sql`.

### Step 5 — Naming conventions

| Artifact | Path template |
|----------|---------------|
| Dependency diagram | `docs/dependency_diagram_<YYYYMMDD>.html` |
| Unused object catalog | `instructions/unused_sql_objects.md` |
| Drop SQL | `sql/drop_unused_objects_<YYYYMMDD>.sql` |

### Step 6 — Safety checklist

Before generating DROP SQL, verify:

- [ ] Each marked-unused object has no transitive dependency path to the dashboard
- [ ] No MV that backs a `SECTIONS` entry is being dropped
- [ ] `vw_etf_daily_features` is never dropped (central hub, not in repo SQL)
- [ ] `vw_etf_prices`, `etf_metadata`, `etf_ranking_config` are never dropped (core deps)
- [ ] `vw_macro_ratio_signals` is kept (used by `vw_macro_signal_table` → `mv_macro_signal_table`)
- [ ] `vw_macro_signal_table` is kept (backing view for `mv_macro_signal_table`)
- [ ] `etf_flows` is never dropped (primordial OHLCV table)
- [ ] `etf_analysis` is kept (queried by `fetch_analysis_row`)
- [ ] Drop order respects intra-file dependency chains (e.g. `v_etf_signal_rank` depends on `v_etf_signals` depends on `v_etf_enriched`)

---

## Quick Reference: Key Python → DB mappings

| Python function | Queries |
|----------------|---------|
| `fetch_section_display_configs()` | `config.etf_dashboard_section_config` |
| `fetch_section_rows()` | `public.mv_etf_report_<section_key>` (from `section.source`) |
| `fetch_macro_summary()` | `public.mv_macro_signal_table` |
| `fetch_latest_report_date()` | `max(date)` across all 5 report MV's |
| `fetch_analysis_row()` | `public.etf_analysis` |
| `fetch_section_all_columns()` | `pg_attribute` + `pg_class` on `section.source` |

The 5 section sources (from `dashboard/sections.py` `SECTIONS`):
```
mv_etf_report_momentum_longs
mv_etf_report_momentum_shorts
mv_etf_report_oversold_mean_reversion
mv_etf_report_overbought_mean_reversion
mv_etf_report_range_compression
```
