# Main Page Navigation Design — 3 Suggestions

## Current State

The main page (`/`) currently has two navigation zones:

- **Operations bar** (top utility bar): links to *Pull Stats*, *Macro Signals*, *Config*
- **TOC nav** (below hero): link to *Macro Signals* + anchor links to each section card

This duplicates the macro-signals link and doesn't highlight the relationship between the 5 data sections and their configurable display columns.

---

## Suggestion A: Sticky Sidebar with Section Tabs

Replace the single-column vertical layout with a **two-column layout**: a narrow sticky sidebar on the left, and the main content area on the right.

### Layout

```
┌──────────────────────────────────────────────┐
│  Operations:  [Pull Stats] [Macro Signals]    │
├──────┬───────────────────────────────────────┤
│ NAV  │  Hero / date picker                   │
│      ├───────────────────────────────────────┤
│ 📊   │  ┌─ Momentum Longs ─────────────────┐ │
│ Mom. │  │  table data…                     │ │
│ Long │  └──────────────────────────────────┘ │
│ 📉   │  ┌─ Momentum Shorts ────────────────┐ │
│ Mom. │  │  table data…                     │ │
│ Short│  └──────────────────────────────────┘ │
│ 🔄   │  ┌─ Range Compression ──────────────┐ │
│ Range│  │  table data…                     │ │
│      │  └──────────────────────────────────┘ │
│ ⚙️   │  ⋮                                    │
│ Config│                                       │
├──────┴───────────────────────────────────────┤
│  Environment / DB badge                      │
└──────────────────────────────────────────────┘
```

### Details

- **Sidebar**: 200px wide, `position: sticky; top: 0` so it follows scroll
- **Section entries** use micro-icons (emoji or SVG) + short label
- **Active section** gets a highlight/accent border on the left
- **Clicking a sidebar item** scrolls to that section card (existing `#section-{key}` anchors)
- **Config** appears as a section entry at the bottom with a gear icon, linking to `/config`
- **Macro Signals** and **Pull Stats** stay in the operations bar
- On mobile (<768px), the sidebar collapses into a horizontal scrollable tab bar below the hero

### Pros
- Always-visible orientation — user can see all sections at a glance
- No content shift from the existing card grid
- Natural place for config link alongside data sections
- Familiar pattern from dashboards (Grafana, Retool, etc.)

### Cons
- Reduces horizontal space for tables (can be mitigated with `min-width` + horizontal scroll)
- Requires layout refactor in both CSS and HTML template

---

## Suggestion B: Accordion / Collapsible Section Groups

Keep the single-column layout but group sections into **expandable groups** with a persistent **group nav strip** between the hero and the grid.

### Layout

```
┌──────────────────────────────────────────────┐
│  Operations:  [Pull Stats] [Macro Signals]   │
├──────────────────────────────────────────────┤
│  Hero / date picker                          │
├──────────────────────────────────────────────┤
│  [Momentum]  [Reversion]  [Compression] [⚙️] │
├──────────────────────────────────────────────┤
│  ┌─ Momentum Longs ───────────────────────┐ │
│  │  table data…   [TOP N: 10]            │ │
│  └────────────────────────────────────────┘ │
│  ┌─ Momentum Shorts ──────────────────────┐ │
│  │  table data…   [TOP N: 10]            │ │
│  └────────────────────────────────────────┘ │
├──────────────────────────────────────────────┤
│  Analysis card                               │
└──────────────────────────────────────────────┘
```

### Details

- **Group nav strip**: pills/buttons for *Momentum*, *Reversion*, *Compression*, and a gear icon for Config
- **Clicking a group** scrolls to the first section in that group
- Sections that belong to the same conceptual group sit adjacent in the vertical flow
- Groups are visually separated by a subtle divider or varying background tint
- **Config** gets a dedicated pill in the strip
- The strip is `position: sticky; top: 0` so it stays accessible while scrolling through long tables
- On date change or limit change, only the visible group's sections are re-fetched (lazy loading)

### Group Classification

| Group | Sections |
|-------|----------|
| Momentum | momentum_longs, momentum_shorts |
| Reversion | oversold_mean_reversion, overbought_mean_reversion |
| Compression | range_compression |

### Pros
- No layout disruption — works with the existing single-column template
- Reduces visual noise by grouping related sections
- Familiar accordion pattern, intuitive
- Config sits alongside data sections in the strip

### Cons
- Only 5 sections currently — groups are small (2 / 2 / 1), so the grouping adds mild value now but scales well
- Requires adding a `group` field to `SectionConfig` in `sections.py`
- Clicking a group item needs scroll-into-view logic in JS

---

## Suggestion C: Dropdown Section Switcher + Embedded Config

Replace the section grid with a **single-section view** controlled by a dropdown/pill selector. Only one section table is visible at a time. Config is embedded as an inline modal or side panel within the same page.

### Layout

```
┌──────────────────────────────────────────────┐
│  Operations:  [Pull Stats] [Macro Signals]   │
├──────────────────────────────────────────────┤
│  Hero / date picker                         │
├──────────────────────────────────────────────┤
│  Section: [Momentum Longs ▼]  [⚙️Columns]  │
├──────────────────────────────────────────────┤
│  ┌─ Momentum Longs ───────────────────────┐ │
│  │  TOP N: [10]                          │ │
│  │  ┌────┬──────┬──────┬──────┬────────┐ │ │
│  │  │Rank│Symbol│ Ret_1d│RVol_20│Score  │ │ │
│  │  ├────┼──────┼──────┼──────┼────────┤ │ │
│  │  │ 1  │ QLD  │ +2.3 │ 1.45 │ 78.5  │ │ │
│  │  │ 2  │ TQQQ │ +1.8 │ 0.92 │ 72.1  │ │ │
│  │  └────┴──────┴──────┴──────┴────────┘ │ │
│  └────────────────────────────────────────┘ │
├──────────────────────────────────────────────┤
│  Analysis card                               │
└──────────────────────────────────────────────┘
```

### Details

- **Dropdown selector** lists all 5 sections with a common prefix shortening
- **Gear icon** next to the dropdown opens an inline config panel (slide-out from right or a modal) — no separate `/config` page navigation
- The config panel shows checkboxes for all available columns per section, with the current selection pre-checked
- **Date changes** and **limit changes** work per-section as they do now
- Switching sections triggers a single `fetch` to `/api/section?key=...`
- The operations bar retains links to Pull Stats and Macro Signals

### Pros
- Maximum horizontal space for each table — no sidebar or multi-card layout competing
- Clean, focused view — users see exactly one section at a time
- Inline config eliminates the need to leave the main page to rearrange columns
- Fast: only one section's data is fetched at a time

### Cons
- Loses the at-a-glance overview of all sections — users must switch to compare
- Introduces a JS dropdown component where none exists today
- Requires building an inline config panel (JS + CSS)
- May frustrate users who want to see Momentum Longs and Shorts side by side

---

## Comparison Matrix

| Criteria | A — Sticky Sidebar | B — Accordion Groups | C — Dropdown Switcher |
|---|---|---|---|
| At-a-glance overview | ✅ All sections visible | ✅ All sections visible | ❌ One at a time |
| Horizontal table space | ⚠️ Reduced by ~200px | ✅ Full width | ✅ Full width |
| Config access | ✅ In sidebar | ✅ In group strip | ✅ Inline modal |
| Mobile friendliness | ⚠️ Collapses to tab bar | ✅ Works as-is | ✅ Dropdown is mobile-friendly |
| Implementation effort | 🔴 High (layout refactor + CSS) | 🟡 Medium (groups + sticky strip) | 🟡 Medium (dropdown + config panel) |
| Future scalability (10+ sections) | ⚠️ Sidebar gets long | ✅ Groups scale well | ✅ Dropdown scales well |
| Familiarity to finance users | 🔵 Common (terminal dashboards) | 🟢 Common (research platforms) | 🟢 Common (screening tools) |

---

## Recommendation

**Suggestion B (Accordion Groups)** is the most practical given the current codebase:
- It works within the existing single-column HTML + CSS template
- The sticky group strip adds minimal JS (just scroll-into-view on click)
- Grouping the 5 existing sections into 3 groups (Momentum, Reversion, Compression) provides immediate value
- Config can be added as a pill in the strip without a separate page navigation
- Scales well as new sections are added

Start by adding a `group` field to `SectionConfig`, then build the sticky group strip, and finally add the config pill.
