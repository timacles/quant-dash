# Skill: JSON Market Analysis

A reasoning-first framework for analyzing the dashboard JSON views as a market narrative rather than a data dump.

---

## Trigger

Use this skill when asked any of:
- "analyze the JSON output"
- "what themes do you see in this market view"
- "summarize `vw_json_macro_signal_table`"
- "summarize `vw_json_etf_reports`"
- "compare the most recent day to the 3-day and 5-day moves"
- "turn these dashboard signals into a regime read"

Primary inputs:
- `vw_json_macro_signal_table`
- `vw_json_etf_reports`
- `vw_llm_market_summary`

---

## Objective

Produce a high-signal market interpretation.

Do not spend time narrating the JSON structure or the extraction mechanics unless the user asks. The task is to answer:

1. What regime is the market expressing?
2. What changed on the latest day?
3. Does the latest day confirm or challenge the 3-day and 5-day trend?
4. Which themes are broad, and which are narrow or fragile?
5. What tensions matter most if the tape is about to resolve one way or the other?

---

## Core Principle

Treat the JSON as **evidence**, not as the conclusion.

The conclusion should be a short list of market truths such as:
- "tightening conditions but no clear credit break"
- "risk-on leadership remains intact but breadth is deteriorating"
- "commodity strength is real, but some leaders are stalling on the latest day"
- "oversold groups are still not bouncing, so weakness is not yet washed out"

Every final theme should be supported by **multiple independent signals**, not by repeating one noisy move in different words.

---

## Reasoning Order

Always reason in this order:

1. **Regime**
   Determine whether the market is acting like `risk-on`, `risk-off`, `inflationary`, `defensive`, `tightening`, or `transition`.

2. **Breadth**
   Ask whether the move is broad or concentrated.

3. **Leadership**
   Identify which groups are pulling the market and whether that leadership is cyclical, defensive, inflationary, duration-sensitive, or pure mega-cap growth.

4. **Timeframe conflict**
   Compare `1d`, `3d`, and `5d` to see whether the latest session confirms the recent tape or pushes against it.

5. **Stress transmission**
   Check whether weakness is flowing through rates, dollar, credit, housing, regional banks, small caps, or international assets.

6. **Actionable tension**
   End on the unresolved issue the market still needs to answer.

---

## Interpreting 1D vs 3D vs 5D

This is the most important reasoning step for `vw_json_etf_reports`.

| Pattern | Meaning |
|--------|---------|
| `1d +`, `3d +`, `5d +` | Strength is persistent and currently being confirmed |
| `1d -`, `3d +`, `5d +` | Pullback inside strength; leadership still intact but stalling |
| `1d -`, `3d -`, `5d +` | Short-term rollover after a prior run; watch for loss of trend quality |
| `1d +`, `3d -`, `5d -` | Bounce inside weakness; likely reflexive unless breadth improves |
| `1d -`, `3d -`, `5d -` | Persistent weakness; no evidence of stabilization |
| `1d +`, `3d +`, `5d -` | Early reversal attempt; interesting but not yet durable |

Interpret the **change in participation**, not just the average return.

Examples:
- If `momentum_longs` are positive on 5-day returns but only half the names are positive on the latest day, the trend is aging.
- If `oversold_mean_reversion` is negative across `1d`, `3d`, and `5d`, oversold does not mean buyable. It means falling knives.
- If `momentum_shorts` remain negative across all horizons, the weak tape is still being sold and short-side pressure remains live.

---

## Macro Table Reasoning

For `vw_json_macro_signal_table`, reason by **clusters**, not by isolated rows.

### 1. Financial conditions

Start with:
- Dollar
- Rates
- Real rates
- Curve proxy
- Credit proxy

Questions:
- Are conditions tightening or easing?
- Is tightening showing up in both rates and dollar, or only one?
- Is credit absorbing the move or breaking under it?

Key interpretation:
- Higher rates plus stronger dollar plus stable credit usually means tightening without immediate panic.
- Higher rates plus stronger dollar plus weaker credit is a more serious macro deterioration.

### 2. Risk appetite

Use:
- `SPY/TLT`
- `IWM/SPY`
- `RSP/SPY`
- `SPHB/SPLV`
- `QQQ/IWD`
- `SMH/SPY`

Questions:
- Is the market rewarding broad risk, or only specific leadership?
- Is growth leadership healthy, or is it masking weak breadth?

Key interpretation:
- Strong `SPY/TLT` with weak `RSP/SPY` and weak `IWM/SPY` means risk appetite exists, but it is narrow.
- Strong `QQQ/IWD` and `SMH/SPY` with poor breadth often means a rally that can still work, but is less robust than the index implies.

### 3. Reflation vs slowdown

Use:
- `CPER/GLD`
- `DBC`
- `USO`
- `XLY/XLP`
- `XLI/XLU`
- `XHB`
- `WOOD`
- `KRE`

Questions:
- Is the market pricing demand strength, inflation pressure, or both?
- Are housing and consumer internals confirming the optimistic read?

Key interpretation:
- Strong commodities and weak housing can mean reflation with tightening stress, not a clean growth boom.
- Strong industrials with weak consumer and weak homebuilders suggests a split economy, not broad expansion.

### 4. Volatility

Use:
- `VIXY`
- `VIXY/VIXM`
- realized vs implied vol

Questions:
- Is volatility confirming fear, or just showing mild hedging demand?
- Is term structure calm enough to keep the bullish interpretation alive?

Key interpretation:
- A normal or contango vol structure alongside weak breadth often means the market is complacent about internal damage.

---

## ETF Report Reasoning

For `vw_json_etf_reports`, remember that each section represents a **candidate set**, not a forecast.

### Momentum longs

Read as:
- Which winners are still trending?
- Are leaders still being bought on the latest day?
- Is strength broadening or narrowing?

Important distinction:
- Negative `1d` but positive `5d` often means orderly digestion.
- Negative `1d`, negative `3d`, and barely positive `5d` means the trend may be fading rather than merely pausing.

### Momentum shorts

Read as:
- Which weak groups are still under distribution?
- Are they accelerating lower or attempting to base?

Important distinction:
- Uniformly negative across `1d`, `3d`, and `5d` means the short book still has directional validity.

### Oversold mean reversion

Do not assume oversold equals bullish.

Ask:
- Are oversold names bouncing yet?
- Or are they oversold because the selling is still active?

Important distinction:
- If this basket is negative across all short horizons, the market is still denying mean reversion.

### Overbought mean reversion

Ask:
- Are crowded winners finally reverting?
- Or are they staying overbought because the underlying theme is still strong?

Important distinction:
- Overbought names that keep rising usually indicate genuine leadership, not exhaustion.

### Range compression

Treat this as a **setup list**, not a directional verdict.

Ask:
- Are compressed names resolving upward, downward, or not at all?
- Do the breakout candidates agree with the macro tape?

Important distinction:
- Positive `5d` with weak `1d` and `3d` often means a failed or delayed resolution.

---

## Weighting Signals Properly

Do not overcount repeated evidence.

Common duplicates:
- `XLY/XLP` may appear in both `Ratios` and `Leading Econ`
- `CPER/GLD` may appear in both `Ratios` and `Leading Econ`
- `UUP` can show up as trend, momentum, and percentile
- a single ETF can appear in both `momentum_shorts` and `oversold_mean_reversion`

Reasoning rule:
- Multiple appearances of the same underlying source increase confidence modestly.
- They do **not** count as multiple independent themes.

Collapse duplicates into one statement such as:
- "consumer/cyclical relative weakness is appearing in multiple places"
- "commodity reflation is confirmed by both absolute and relative measures"

---

## What Makes a Theme Interesting

A theme is worth calling out when it has at least one of these properties:

- **Cross-asset confirmation**: the same message appears in equities, bonds, dollar, and commodities
- **Timeframe conflict**: the latest day materially challenges the 3-day or 5-day trend
- **Breadth divergence**: the index-level tone is stronger than internal participation
- **Macro tension**: risk assets are rallying while financial conditions tighten
- **Failed mean reversion**: oversold groups do not bounce, or overbought groups do not fade
- **Leadership concentration**: one cluster such as energy, semis, or dollar dominates multiple sections

If none of these are present, the analysis is probably too descriptive and not interpretive enough.

---

## Common Failure Modes

Avoid these mistakes:

- Listing signals one by one without synthesizing them
- Treating every row as independent
- Confusing a one-day bounce with a regime change
- Calling an oversold basket "bullish" before it actually rebounds
- Ignoring breadth deterioration because the top leaders still look strong
- Ignoring the difference between "tightening but stable" and "tightening and breaking"
- Using the report count as evidence of breadth

---

## Output Shape

Good output usually has this structure:

1. **Regime sentence**
   Example: "The market still looks like a narrow risk-on tape under tighter financial conditions."

2. **Three to five themes**
   Each theme should combine multiple signals and explain why it matters.

3. **Latest-day versus recent-trend read**
   Explicitly state whether the most recent session confirms or challenges the 3-day and 5-day picture.

4. **What to watch next**
   Name the small set of signals that would confirm continuation or reversal.

---

## Preferred Language

Prefer language like:
- "pullback within strength"
- "bounce within weakness"
- "narrow leadership"
- "breadth deterioration"
- "tightening without credit break"
- "split economy"
- "reflation impulse"
- "failed mean reversion"
- "leadership still intact but aging"

Avoid vague summaries like:
- "mixed"
- "some good and some bad"
- "volatile environment"

Those are acceptable only after the underlying tension has been stated precisely.
