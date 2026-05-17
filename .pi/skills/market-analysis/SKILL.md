---
name: market-analysis
description: >
  Generates a structured market analysis from live prod database data.
  Uses the JSON market analysis reasoning framework (regime, breadth,
  leadership, timeframe conflict, stress transmission, tension).
  Trigger: "analyze the market", "market analysis", "what's the market doing".
---

# Market Analysis

Fetches live ETF reports and macro signals from the prod database, then
produces a high-signal market interpretation following the reasoning
framework in `instructions/skill_json_market_analysis.md`.

## Execution

Run the prompt generator to get the instructions + live data:

```bash
cd /home/tim/trading
.venv/bin/python3 helpers/llm_prompt.py instructions/skill_json_market_analysis.md
```

Read the output of that command. It contains:

1. **`# Instructions`** — the full skill reasoning framework (regime → breadth → leadership → timeframe conflict → stress transmission → actionable tension)
2. **`# Current Data`** — live JSON from `vw_json_etf_reports` and `vw_json_macro_signal_table`

Then follow the instructions to produce the analysis.
