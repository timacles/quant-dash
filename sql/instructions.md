# SQL Analytics Instructions

## Directory Purpose
This directory defines the deployed ETF analytics stack for the `financials` database behind the ranking dashboard and related analytical views.

Read the stack in layers:

`public.vw_etf_daily_features` -> scoring/regime/macro views -> report views -> dashboard and downstream JSON/LLM consumers

The active system is centered on the newer `public.vw_etf_*` and `public.vw_macro_*` views. The `DDLs/` subdirectory is legacy unless the task explicitly asks for it.

## What Is Actually Deployed
The following reviewed objects are present in `service=fin` as of this audit:

- Core ranking stack:
  `public.vw_etf_prices`, `public.vw_etf_daily_scores`, `public.vw_etf_theme_group_metrics`, `public.vw_market_regime`, `public.vw_etf_ranked_lists`
- Payload/snapshot outputs:
  `public.vw_etf_daily_report_payload`, `public.vw_etf_daily_report_payload_latest`, `public.vw_etf_report_json_snapshot_latest`, `public.refresh_etf_report_json_snapshot`
- Latest-date analytical rankings:
  `public.vw_etf_risk_adjusted_momentum_rankings`, `public.vw_etf_oversold_mean_reversion_rankings`, `public.vw_etf_overbought_mean_reversion_rankings`, `public.vw_etf_range_compression_rankings`
- Macro layer:
  `public.vw_macro_cluster_momentum`, `public.vw_macro_bond_treasury_buckets`, `public.vw_macro_bond_treasury_summary`, `public.vw_macro_ratio_signals`, `public.vw_macro_signal_dashboard`
- Report views:
  `public.vw_etf_report_momentum_longs`, `public.vw_etf_report_momentum_shorts`, `public.vw_etf_report_oversold_mean_reversion`, `public.vw_etf_report_overbought_mean_reversion`, `public.vw_etf_report_range_compression`, `public.vw_etf_report_bond_credit_performance`
- Supporting tables:
  `public.etf_metadata`, `public.etf_ranking_config`, `public.etf_report_json_snapshot`, `config.etf_dashboard_section_config`, `public.etf_analysis`
- LLM summary output:
  `public.vw_llm_market_summary`

## Core Files
`etf_ranking_views.sql`

- Defines the core scoring stack and most public ETF outputs.
- Creates support tables `public.etf_metadata`, `public.etf_ranking_config`, and `public.etf_report_json_snapshot`.
- Defines `vw_etf_daily_scores`, `vw_market_regime`, `vw_etf_ranked_lists`, JSON payload views, and latest-date ranking views.
- Computes `eligible_momentum_positive` and `score_momentum_positive`, but does not publish an active `vw_etf_report_momentum_positive` report view.

`macro_signal_views.sql`

- Defines the macro interpretation layer built on `public.vw_etf_daily_features`.
- Produces cluster aggregates, bond/treasury summaries, ratio signals, and `public.vw_macro_signal_dashboard`.

`vw_etf_report_momentum_longs.sql`
`vw_etf_report_momentum_shorts.sql`
`vw_etf_report_oversold_mean_reversion.sql`
`vw_etf_report_overbought_mean_reversion.sql`
`vw_etf_report_range_compression.sql`

- Thin report-ready top-15 views over shared scoring logic from `public.vw_etf_daily_scores` plus `public.vw_market_regime`.

`vw_etf_report_bond_credit_performance.sql`

- Not a thin presentation wrapper.
- Defines its own bond/credit ETF universe, classification rules, normalization, and composite score before publishing a ranked report view.

`config_app.sql`

- Defines `config.etf_dashboard_section_config`, which the Python dashboard uses to validate visible columns and labels.

`vw_llm_market_summary.sql`

- Produces a compact JSON summary for LLM-oriented consumers from `public.vw_market_regime`, `public.vw_macro_signal_dashboard`, and `public.vw_etf_theme_group_metrics`.

## Report View Semantics
Use these meanings when analyzing or extending the active report views:

- `momentum_longs`: long-side momentum candidates from the shared momentum score.
- `momentum_shorts`: short-side momentum candidates from the shared momentum score.
- `oversold_mean_reversion`: long reversion candidates from the shared mean-reversion score.
- `overbought_mean_reversion`: short reversion candidates from the shared mean-reversion score.
- `range_compression`: tightening-base / volatility-contraction candidates from the shared tightening-base score.
- `bond_credit_performance`: custom-scored bond/credit relative-strength report with its own universe and bucket logic.

`momentum_positive` is not an active published report in the current stack. The old report view is explicitly dropped in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:3), while the underlying score/eligibility logic still exists in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:128) and [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:168).

## Dashboard-Used Objects
`serve_dashboard.py` directly queries these deployed database objects:

- `config.etf_dashboard_section_config`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:993)
- `public.vw_etf_report_momentum_longs`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:909)
- `public.vw_etf_report_momentum_shorts`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:917)
- `public.vw_etf_report_oversold_mean_reversion`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:925)
- `public.vw_etf_report_overbought_mean_reversion`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:932)
- `public.vw_etf_report_range_compression`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:939)
- `public.vw_macro_signal_dashboard`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:1085)
- `public.vw_etf_report_bond_credit_performance`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:1113)
- `public.etf_analysis`
  Source: [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:1201)

## Deployed But Not Directly Used By `serve_dashboard.py`
These reviewed DDL-defined objects are deployed in `service=fin` but are not directly queried by `serve_dashboard.py`:

- `public.etf_metadata`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:5)
- `public.etf_ranking_config`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:24)
- `public.etf_report_json_snapshot`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:38)
- `public.vw_etf_prices`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:53)
- `public.vw_etf_daily_scores`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:65)
- `public.vw_etf_theme_group_metrics`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:205)
- `public.vw_market_regime`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:267)
- `public.vw_etf_ranked_lists`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:384)
- `public.refresh_etf_report_json_snapshot(date)`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:660)
- `public.vw_etf_report_json_snapshot_latest`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:781)
- `public.vw_etf_daily_report_payload`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:791)
- `public.vw_etf_daily_report_payload_latest`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:947)
- `public.vw_etf_risk_adjusted_momentum_rankings`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:954)
- `public.vw_etf_oversold_mean_reversion_rankings`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:1069)
- `public.vw_etf_overbought_mean_reversion_rankings`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:1099)
- `public.vw_etf_range_compression_rankings`
  Defined in [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:1129)
- `public.vw_macro_cluster_momentum`
  Defined in [macro_signal_views.sql](/Users/timur.alekper/trading/sql/macro_signal_views.sql:1)
- `public.vw_macro_bond_treasury_buckets`
  Defined in [macro_signal_views.sql](/Users/timur.alekper/trading/sql/macro_signal_views.sql:99)
- `public.vw_macro_bond_treasury_summary`
  Defined in [macro_signal_views.sql](/Users/timur.alekper/trading/sql/macro_signal_views.sql:204)
- `public.vw_macro_ratio_signals`
  Defined in [macro_signal_views.sql](/Users/timur.alekper/trading/sql/macro_signal_views.sql:257)
- `public.vw_llm_market_summary`
  Defined in [vw_llm_market_summary.sql](/Users/timur.alekper/trading/sql/vw_llm_market_summary.sql:1)

Note: some of the objects above are still important as upstream dependencies of dashboard-used views; this section only means the Python file does not query them directly.

## Important Views For Analysis
When reasoning about behavior, prefer these conceptual outputs:

- `public.vw_etf_daily_scores`
- `public.vw_market_regime`
- `public.vw_etf_ranked_lists`
- `public.vw_macro_signal_dashboard`
- `public.vw_etf_daily_report_payload`
- `public.vw_llm_market_summary`

Do not infer semantics from filenames alone. Check eligibility filters and ranking logic in the SQL.

## Things To Fix
The following items describe correctness or maintenance gaps discovered during review. Each item includes the relevant references.

1. `vw_market_regime` can classify `risk_on` when VIX input is missing.
   References:
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:333),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:345)
   Needed fix:
   Require non-null VIX input for the `risk_on` branch or treat missing VIX as `mixed`/unknown instead of neutral.

2. Momentum long/short reports are looser than the documented continuation semantics.
   References:
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:118),
   [vw_etf_report_momentum_longs.sql](/Users/timur.alekper/trading/sql/vw_etf_report_momentum_longs.sql:40),
   [vw_etf_report_momentum_shorts.sql](/Users/timur.alekper/trading/sql/vw_etf_report_momentum_shorts.sql:39)
   Needed fix:
   Tighten the directional filter so published momentum lists require aligned return/relative-strength continuation conditions, not just the fallback `direction_flag`.

3. `momentum_positive` logic exists but is not published as an active report.
   References:
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:3),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:128),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:168),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:384),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:848)
   Needed fix:
   Either restore a real published `momentum_positive` report path or remove the unused score/eligibility path from the active stack.

4. `vw_etf_report_bond_credit_performance` is custom logic, not a thin shared-score publishing view.
   References:
   [vw_etf_report_bond_credit_performance.sql](/Users/timur.alekper/trading/sql/vw_etf_report_bond_credit_performance.sql:2),
   [vw_etf_report_bond_credit_performance.sql](/Users/timur.alekper/trading/sql/vw_etf_report_bond_credit_performance.sql:33),
   [vw_etf_report_bond_credit_performance.sql](/Users/timur.alekper/trading/sql/vw_etf_report_bond_credit_performance.sql:127)
   Needed fix:
   Decide whether to keep this as an explicitly separate report family or refactor it into the shared ranking framework, then document that decision consistently.

5. `sync_etf_reference_tables.sql` has no duplicate-row guard or stale-row policy.
   References:
   [sync_etf_reference_tables.sql](/Users/timur.alekper/trading/sql/sync_etf_reference_tables.sql:5),
   [sync_etf_reference_tables.sql](/Users/timur.alekper/trading/sql/sync_etf_reference_tables.sql:72),
   [sync_etf_reference_tables.sql](/Users/timur.alekper/trading/sql/sync_etf_reference_tables.sql:98)
   Needed fix:
   Dedupe staged rows before upsert, validate key columns, and define how rows missing from the latest CSV should be handled.

6. The JSON payload and LLM summary mix `theme_type`, `sector`, and `region` into one combined leaderboard.
   References:
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:811),
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:839),
   [vw_llm_market_summary.sql](/Users/timur.alekper/trading/sql/vw_llm_market_summary.sql:58)
   Needed fix:
   Either rank per `group_kind` or document clearly that the top/bottom lists are intentionally cross-kind.

7. `vw_etf_daily_report_payload` exposes an empty `warnings` array instead of real quality signals.
   References:
   [etf_ranking_views.sql](/Users/timur.alekper/trading/sql/etf_ranking_views.sql:932)
   Needed fix:
   Populate warnings for missing metadata, missing benchmark/VIX inputs, insufficient cross-section, or other material data-quality issues.

## Guidance For Future Reviews
- If the question is about the dashboard, start with [serve_dashboard.py](/Users/timur.alekper/trading/serve_dashboard.py:909) and `config.etf_dashboard_section_config` before tracing upstream SQL dependencies.
- If the question is about ranking behavior, start with `public.vw_etf_daily_scores`, then `public.vw_etf_ranked_lists`, then the thin report views.
- If the question is about macro behavior, start with `public.vw_macro_signal_dashboard` and work backward through `macro_signal_views.sql`.
- If a prompt references `DDLs/`, treat it as the legacy framework unless the user explicitly wants that older system.
