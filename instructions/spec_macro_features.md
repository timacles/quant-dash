# Macro Dashboard Signal Spec

## Objective
Design a high-level macro signal dashboard for a trading desk. The dashboard provides a tight, at-a-glance view of macro regime, risk appetite, and cross-asset trends — refreshed daily with a weekly strategic overlay. All signals are derived from a key ETF/index universe using OHLCV data.

---

## 1. Macro Signal Table

All signals are displayed in a single unified table. Each row is one signal. Columns provide a consistent view of momentum, trend position, and relative volume across every signal.

### Column Definitions

| Column | Description |
|--------|-------------|
| **Category** | Signal group (Volatility, Dollar, Rates, Ratios, Commodities, Breadth, Leading Econ) |
| **Signal** | Descriptive label of what the signal measures |
| **Source** | Ticker(s) or ratio used |
| **1D Chg** | 1-day % change |
| **5D Chg** | 5-day % change |
| **10D Chg** | 10-day % change |
| **20D Chg** | 20-day % change |
| **vs 20-DMA** | Price (or ratio) relative to 20-day moving average (% above/below) |
| **vs 50-DMA** | Price (or ratio) relative to 50-day moving average (% above/below) |
| **vs 200-DMA** | Price (or ratio) relative to 200-day moving average (% above/below) |
| **Wk RVOL** | Weekly relative volume (current week's avg daily volume / 20-week avg daily volume) |
| **Interpretation** | What the current reading means |

### Signal Rows

| Category | Signal | Source | 1D | 5D | 10D | 20D | vs 20-DMA | vs 50-DMA | vs 200-DMA | Wk RVOL | Interpretation |
|----------|--------|--------|----|----|-----|-----|-----------|-----------|------------|---------|----------------|
| Volatility | VIX Level | VIXY | · | · | · | · | · | · | · | · | <15 complacent, 15–20 normal, 20–30 elevated, >30 crisis |
| Volatility | VIX Term Structure | VIXY/VIXM | · | · | · | · | · | · | · | · | <1.0 contango (calm), >1.0 backwardation (stress) |
| Volatility | VIX 10d Percentile | VIXY | · | · | · | · | · | · | · | · | Percentile rank over 1yr; flags rapid vol expansion |
| Volatility | Realized vs Implied Vol | SPY rv vs VIXY | · | · | · | · | · | · | · | · | Large spread = fear premium; negative = complacency |
| Dollar | USD Trend | UUP | · | · | · | · | · | · | · | · | Above both MAs = strong dollar regime |
| Dollar | USD Momentum | UUP | · | · | · | · | · | · | · | · | Rising = headwind for risk assets & commodities |
| Dollar | USD 1yr Percentile | UUP | · | · | · | · | · | · | · | · | Extremes flag potential reversals |
| Rates | Long-End Trend (20yr+) | TLT | · | · | · | · | · | · | · | · | Falling TLT = rising long rates |
| Rates | Yield Curve Proxy | TLT/SHY | · | · | · | · | · | · | · | · | Falling = flattening/inverting curve = recession risk |
| Rates | Credit Spread Proxy | HYG/LQD | · | · | · | · | · | · | · | · | Falling = credit stress widening |
| Rates | Real Rate Proxy (TIPS) | TIP | · | · | · | · | · | · | · | · | Rising TIP = falling real rates (easier conditions) |
| Rates | Duration Momentum (7-10yr) | IEF | · | · | · | · | · | · | · | · | Captures intermediate rate moves |
| Rates | Short-End Trend (2yr) | SHY / UTWO | · | · | · | · | · | · | · | · | Short-end rate direction (Fed expectations) |
| Ratios | Small vs Large Cap | IWM/SPY | · | · | · | · | · | · | · | · | Rising = risk-on broadening; falling = narrow/defensive |
| Ratios | Growth vs Value | QQQ/IWD | · | · | · | · | · | · | · | · | Rising = growth regime; falling = value rotation |
| Ratios | Cyclicals vs Defensives | XLY/XLP | · | · | · | · | · | · | · | · | Rising = economic optimism |
| Ratios | Equities vs Bonds | SPY/TLT | · | · | · | · | · | · | · | · | Rising = risk-on; falling = flight to safety |
| Ratios | Copper vs Gold | CPER/GLD | · | · | · | · | · | · | · | · | Rising = reflation/growth; falling = deflation fear |
| Ratios | EM vs DM | EEM/EFA | · | · | · | · | · | · | · | · | Rising = EM outperformance (weaker dollar, risk-on) |
| Ratios | Semis vs Market | SMH/SPY | · | · | · | · | · | · | · | · | Leading indicator of tech/growth cycle |
| Commodities | Gold Trend | GLD | · | · | · | · | · | · | · | · | Above both MAs = fear/inflation bid |
| Commodities | Oil Trend | USO | · | · | · | · | · | · | · | · | Rising = demand/inflation; falling = growth concern |
| Commodities | Broad Commodities | DBC | · | · | · | · | · | · | · | · | Overall commodity cycle direction |
| Breadth | Equal-Wt vs Cap-Wt | RSP/SPY | · | · | · | · | · | · | · | · | Rising = broad participation; falling = narrow leadership |
| Breadth | Small Cap Breadth | IWM | · | · | · | · | · | · | · | · | Above 200-DMA = healthy small-cap breadth |
| Breadth | Sector Breadth Score | 11 SPDR sectors | · | · | · | · | · | · | · | · | Count above 50-DMA; ≥8 strong, ≤3 weak |
| Breadth | High Beta vs Low Vol | SPHB/SPLV | · | · | · | · | · | · | · | · | Rising = risk appetite broad; falling = defensive rotation |
| Leading Econ | Industrials vs Utilities (ISM proxy) | XLI/XLU | · | · | · | · | · | · | · | · | Rising = expansion; falling = contraction |
| Leading Econ | Transports (Dow Theory) | IYT | · | · | · | · | · | · | · | · | Confirms or diverges from equity rally |
| Leading Econ | Consumer Strength | XLY/XLP | · | · | · | · | · | · | · | · | Rising = consumer confidence / spending |
| Leading Econ | Regional Banks (Credit) | KRE | · | · | · | · | · | · | · | · | Below 200-DMA = tightening credit conditions |
| Leading Econ | Homebuilders (Housing) | XHB | · | · | · | · | · | · | · | · | Most rate-sensitive leading sector (leads 6-9mo) |
| Leading Econ | Copper/Gold (Growth) | CPER/GLD | · | · | · | · | · | · | · | · | Rising = reflation; falling = deflation fear |
| Leading Econ | Lumber (Construction) | WOOD | · | · | · | · | · | · | · | · | Rising = building activity expanding (leads GDP) |
| Leading Econ | Semiconductors (Capex) | SMH | · | · | · | · | · | · | · | · | Semis lead the market by 1-2 quarters |

---

## 2. Dashboard Layout (Conceptual)

### Daily View — Single Table
```
Category      │ Signal                        │ Source     │  1D  │  5D  │ 10D  │ 20D  │ vs20 │ vs50 │vs200 │ RVOL │ Reading
──────────────┼───────────────────────────────┼────────────┼──────┼──────┼──────┼──────┼──────┼──────┼──────┼──────┼──────────────
Volatility    │ VIX Level                     │ VIXY       │ -2.1 │ -5.3 │ -8.1 │-12.0 │ -3.2 │ -8.1 │-15.2 │ 0.82 │ Complacent
Volatility    │ VIX Term Structure            │ VIXY/VIXM  │ -0.3 │ -1.1 │ -1.5 │ -2.0 │ -1.0 │ -2.3 │  —   │  —   │ Contango ✓
Dollar        │ USD Trend                     │ UUP        │ -0.2 │ -1.4 │ -2.1 │ -3.0 │ -1.1 │ -2.5 │ -4.1 │ 1.15 │ Weak ✓
Rates         │ Long-End Trend                │ TLT        │ +0.3 │ -0.8 │ -1.2 │ -2.5 │ -0.5 │ -1.8 │ -5.2 │ 0.95 │ Rates rising
Rates         │ Yield Curve Proxy             │ TLT/SHY    │ +0.1 │ -0.5 │ -0.8 │ -1.2 │ -0.3 │ -1.0 │  —   │  —   │ Flattening ⚠
Rates         │ Credit Spread Proxy           │ HYG/LQD    │ +0.1 │ +0.2 │ +0.3 │ +0.5 │ +0.2 │ +0.4 │  —   │  —   │ Stable ✓
Ratios        │ Small vs Large Cap            │ IWM/SPY    │ +0.3 │ +1.2 │ +1.8 │ +2.5 │ +1.0 │ +2.1 │  —   │  —   │ Broadening ▲
Ratios        │ Equities vs Bonds             │ SPY/TLT    │ +0.2 │ +1.5 │ +2.1 │ +3.8 │ +1.3 │ +3.0 │  —   │  —   │ Risk-on ▲
Breadth       │ Sector Breadth Score          │ 11 sectors │  —   │  —   │  —   │  —   │  —   │  —   │  —   │  —   │ 9/11 strong
Leading Econ  │ Regional Banks (Credit)       │ KRE        │ +0.5 │ -1.2 │ -3.0 │ -5.1 │ -2.0 │ -4.5 │ -8.2 │ 1.35 │ Tightening ⚠
Leading Econ  │ Semiconductors (Capex)        │ SMH        │ +1.1 │ +3.2 │ +4.5 │ +6.0 │ +2.5 │ +5.0 │+12.3 │ 1.10 │ Expanding ▲
  ...         │  (35 rows total)              │            │      │      │      │      │      │      │      │      │
```

### Weekly Strategic Overlay
- 13-week and 26-week trend direction for each ratio
- Regime shift alerts: flag when breadth or leading indicators cross key thresholds
- Trend persistence score: how many weeks the current regime has held

---

## 3. Required ETF Universe (Minimum)

| Category | Tickers |
|----------|---------|
| Equity Indices | SPY, QQQ, IWM, DIA |
| Sectors | XLY, XLP, XLF, XLE, XLK, XLV, XLI, XLU, XLB, XLRE, XLC |
| Bonds/Treasuries | TLT, IEF, SHY, TIP, UTWO |
| Credit | HYG, LQD |
| Dollar | UUP |
| Volatility | VIXY, VIXM |
| Commodities | GLD, SLV, USO, DBC (or PDBC), CPER |
| International | EEM, EFA, FXI |
| Style | IWD (or VTV), VUG |
| Semis | SMH |
| Breadth | RSP, SPHB, SPLV |
| Leading Econ | IYT, KRE, XHB, WOOD (or CUT) |

**Total: ~42–47 ETFs**

---

## 4. Trend Calculation Spec

All trend signals use a consistent framework:

- **Moving Averages:** 20-DMA (short), 50-DMA (medium), 200-DMA (long)
- **Momentum:** 1-week, 1-month, 3-month, 6-month % returns
- **Percentile Rank:** Current value's percentile over trailing 252 trading days
- **Ratio Trend:** Ratio value vs its own 50-DMA (above = bullish, below = bearish)
- **Direction Arrow:** Based on 5-day slope of the signal (▲ rising, ▼ falling, ▶ flat)


