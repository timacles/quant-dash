const DEFAULT_SECTION_LIMIT = 10;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");
}

function sortValue(column, value) {
  if (value == null) return "";
  if (["ret_1d", "ret_3d", "ret_5d", "ret_10d"].includes(column)) {
    return (Number(value) * 100).toFixed(8);
  }
  if (typeof value === "number") {
    return value.toFixed(8);
  }
  return String(value);
}

function formatValue(column, value) {
  if (value == null) return "—";
  if (column === "rank") return String(value);
  if (["ret_1d", "ret_3d", "ret_5d", "ret_10d"].includes(column)) {
    return `${(Number(value) * 100).toFixed(2)}%`;
  }
  if (["rvol_20", "composite_score", "rs_5", "rs_10"].includes(column)) {
    return Number(value).toFixed(2);
  }
  if ([
    "zscore_close_20",
    "atr_stretch_20",
    "close_location_20",
    "volume_ratio_5_20",
    "range_compression_5_20",
    "range_compression_5_60",
    "atr_compression_5_20",
  ].includes(column)) {
    return Number(value).toFixed(3);
  }
  return String(value);
}

function valueClass(column, value) {
  if (value == null) return "";
  if (["rvol_20", "volume_ratio_5_20"].includes(column)) {
    const number = Number(value);
    if (number >= 2.0) return " etf-report__value--vol-extreme";
    if (number >= 1.0) return " etf-report__value--vol-high";
    return " etf-report__value--vol-low";
  }
  if (![
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "zscore_close_20",
    "atr_stretch_20",
  ].includes(column)) {
    return "";
  }
  const number = Number(value);
  if (number > 0) return " etf-report__value--pos";
  if (number < 0) return " etf-report__value--neg";
  return "";
}

function renderCell(column, row) {
  if (column === "symbol") {
    const sym = row.symbol ?? "";
    const finvizUrl = `https://finviz.com/quote.ashx?t=${encodeURIComponent(sym)}`;
    return `<td data-sort-value="${escapeHtml(sortValue(column, sym))}"><a class="etf-report__symbol-link" href="${finvizUrl}" target="_blank" rel="noopener noreferrer"><span class="etf-report__symbol">${escapeHtml(sym)}</span></a><span class="etf-report__name">${escapeHtml(row.asset_class ?? "")}</span></td>`;
  }
  if (column === "display_name") {
    return `<td data-sort-value="${escapeHtml(sortValue(column, row[column]))}">${escapeHtml(row[column] ?? "")}</td>`;
  }
  const classes = `etf-report__value${valueClass(column, row[column])}`.trim();
  return `<td class="${classes}" data-sort-value="${escapeHtml(sortValue(column, row[column]))}">${escapeHtml(formatValue(column, row[column]))}</td>`;
}

function formatSummaryPercent(value) {
  if (value == null) return "—";
  const number = Number(value);
  return `${number >= 0 ? "+" : ""}${(number * 100).toFixed(2)}%`;
}

function renderSummaryLoading(message) {
  return `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Macro Summary</h2><p class="etf-report__card-desc">Compact macro regime, leadership, bond internals, and top bond momentum.</p></div><div class="etf-report__empty">${escapeHtml(message)}</div></div>`;
}

function renderSummaryCard(summary) {
  if (!summary || !summary.macro) {
    return renderSummaryLoading("No macro summary data found for the selected date.");
  }

  const macro = summary.macro;
  const leaders = Array.isArray(summary.bond_leaders) ? summary.bond_leaders : [];
  const asOfDate = summary.date || macro.date || "—";

  const statusCards = [
    ["Macro Regime", macro.macro_regime || "—"],
    ["Credit Risk", macro.credit_risk_on_flag == null ? "—" : (macro.credit_risk_on_flag ? "ON" : "OFF")],
    ["Duration Bid", macro.duration_bid_flag == null ? "—" : (macro.duration_bid_flag ? "ON" : "OFF")],
    ["Inflation", macro.inflation_bid_flag == null ? "—" : (macro.inflation_bid_flag ? "ON" : "OFF")],
  ].map(([label, value]) => `<div class="etf-report__summary-box"><div class="etf-report__summary-label">${escapeHtml(label)}</div><div class="etf-report__summary-value">${escapeHtml(String(value))}</div></div>`).join("");

  const leadershipItems = [
    ["IWM/SPY 20D", macro.iwm_spy_ratio_ret_20d],
    ["QQQ/SPY 20D", macro.qqq_spy_ratio_ret_20d],
    ["HYG/LQD 20D", macro.hyg_lqd_ratio_ret_20d],
  ].map(([label, value]) => `<div class="etf-report__summary-item"><span class="etf-report__summary-label">${escapeHtml(label)}</span><span class="etf-report__value${valueClass("ret_5d", value)}">${escapeHtml(formatSummaryPercent(value))}</span></div>`).join("");

  const bondItems = [
    ["Credit Spread Proxy 20D", macro.credit_spread_proxy_20d],
    ["Duration Spread Proxy 20D", macro.duration_spread_proxy_20d],
  ].map(([label, value]) => `<div class="etf-report__summary-item"><span class="etf-report__summary-label">${escapeHtml(label)}</span><span class="etf-report__value${valueClass("ret_5d", value)}">${escapeHtml(formatSummaryPercent(value))}</span></div>`).join("");

  const leaderRows = leaders.length
    ? leaders.map((row) => `<tr><td>${escapeHtml(String(row.rank ?? "—"))}</td><td><span class="etf-report__symbol">${escapeHtml(row.symbol ?? "")}</span></td><td>${escapeHtml(row.display_name ?? "")}</td><td>${escapeHtml(row.bond_bucket ?? "—")}</td><td>${escapeHtml(row.composite_score == null ? "—" : Number(row.composite_score).toFixed(2))}</td></tr>`).join("")
    : `<tr><td colspan="5">No qualifying rows.</td></tr>`;

  return `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Macro Summary</h2><p class="etf-report__card-desc">Compact macro regime, leadership, bond internals, and top bond momentum.</p><div class="etf-report__card-meta"><span class="etf-report__badge">As of ${escapeHtml(asOfDate)}</span></div></div><div class="etf-report__summary-grid"><div class="etf-report__summary-status">${statusCards}</div><div class="etf-report__summary-panels"><div class="etf-report__summary-box"><div class="etf-report__summary-label">Cross-Asset Leadership</div><div class="etf-report__summary-list">${leadershipItems}</div></div><div class="etf-report__summary-box"><div class="etf-report__summary-label">Bond Internals</div><div class="etf-report__summary-list">${bondItems}</div></div><div class="etf-report__summary-box"><div class="etf-report__summary-label">Top Bond Momentum</div><table class="etf-report__summary-table"><thead><tr><th>Rank</th><th>Ticker</th><th>Name</th><th>Bucket</th><th>Score</th></tr></thead><tbody>${leaderRows}</tbody></table></div></div></div></div>`;
}

function renderSection(section, rows, limitValue) {
  const sectionControls = `<form class="etf-report__section-filter" data-section-filter-form data-section-key="${escapeHtml(section.key)}"><label class="etf-report__section-filter-label" for="top-n-${escapeHtml(section.key)}">TOP N</label><input class="etf-report__section-filter-input" id="top-n-${escapeHtml(section.key)}" type="number" inputmode="numeric" min="1" step="1" name="limit" value="${escapeHtml(limitValue)}"></form>`;
  if (!rows.length) {
    return `<div class="etf-report__card" data-section-card="${escapeHtml(section.key)}"><div class="etf-report__card-head"><div class="etf-report__card-title-row"><h2 class="etf-report__card-title">${escapeHtml(section.title)}</h2>${sectionControls}</div><p class="etf-report__card-desc">${escapeHtml(section.description)}</p></div><div class="etf-report__empty">No qualifying rows for the selected date.</div></div>`;
  }

  const asOfDate = rows[0].date;
  const marketRegime = rows[0].market_regime;
  const headerHtml = section.columns
    .map((column, idx) => `<th data-sort-index="${idx}" data-sort-direction="">${escapeHtml(section.column_labels[column] ?? column)}<span class="etf-report__sort-indicator"></span></th>`)
    .join("");
  const bodyHtml = rows
    .map((row) => `<tr>${section.columns.map((column) => renderCell(column, row)).join("")}</tr>`)
    .join("");
  const badges = [
    `<span class="etf-report__badge">As of ${escapeHtml(asOfDate)}</span>`,
    `<span class="etf-report__badge">${rows.length} rows</span>`,
  ];
  if (marketRegime) {
    badges.splice(1, 0, `<span class="etf-report__badge">Market Regime: ${escapeHtml(marketRegime)}</span>`);
  }

  return `<div class="etf-report__card" data-section-card="${escapeHtml(section.key)}"><div class="etf-report__card-head"><div class="etf-report__card-title-row"><h2 class="etf-report__card-title">${escapeHtml(section.title)}</h2>${sectionControls}</div><p class="etf-report__card-desc">${escapeHtml(section.description)}</p><div class="etf-report__card-meta">${badges.join("")}</div></div><div class="etf-report__table-wrap"><table class="etf-report__table"><thead><tr>${headerHtml}</tr></thead><tbody>${bodyHtml}</tbody></table></div></div>`;
}

function initTableSorters(root) {
  root.querySelectorAll(".etf-report__table").forEach(function (table) {
    if (table.dataset.sortReady === "true") return;
    table.dataset.sortReady = "true";

    const headers = Array.from(table.querySelectorAll("th[data-sort-index]"));
    const tbody = table.querySelector("tbody");
    if (!tbody) return;

    headers.forEach(function (header) {
      header.addEventListener("click", function () {
        const columnIndex = Number(header.dataset.sortIndex);
        const currentDirection = header.dataset.sortDirection === "asc" ? "asc" : "desc";
        const nextDirection = currentDirection === "asc" ? "desc" : "asc";
        const rows = Array.from(tbody.querySelectorAll("tr"));

        headers.forEach(function (other) {
          other.dataset.sortDirection = "";
          const indicator = other.querySelector(".etf-report__sort-indicator");
          if (indicator) indicator.textContent = "";
        });

        rows.sort(function (a, b) {
          const aCell = a.children[columnIndex];
          const bCell = b.children[columnIndex];
          const aValue = aCell ? aCell.dataset.sortValue || aCell.textContent.trim() : "";
          const bValue = bCell ? bCell.dataset.sortValue || bCell.textContent.trim() : "";
          const aNumber = Number(aValue);
          const bNumber = Number(bValue);

          let comparison = 0;
          if (!Number.isNaN(aNumber) && !Number.isNaN(bNumber) && aValue !== "" && bValue !== "") {
            comparison = aNumber - bNumber;
          } else {
            comparison = aValue.localeCompare(bValue);
          }

          return nextDirection === "asc" ? comparison : -comparison;
        });

        rows.forEach(function (row) {
          tbody.appendChild(row);
        });

        header.dataset.sortDirection = nextDirection;
        const indicator = header.querySelector(".etf-report__sort-indicator");
        if (indicator) indicator.textContent = nextDirection === "asc" ? "^" : "v";
      });
    });
  });
}

function initEyebrowClock() {
  const eyebrow = document.querySelector(".etf-report__eyebrow");
  if (!eyebrow) return;
  const now = new Date();
  const date = now.toLocaleDateString("en-US", { weekday: "long", year: "numeric", month: "long", day: "numeric" });
  const time = now.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true });
  eyebrow.textContent = `${date} · ${time}`;
}

document.addEventListener("DOMContentLoaded", function () {
  initEyebrowClock();

  const container = document.querySelector("[data-etf-report]");
  if (!container) return;

  const sections = JSON.parse(container.dataset.sections || "[]");
  const summaryContainer = document.querySelector("[data-etf-summary]");
  const grid = document.querySelector("[data-etf-grid]");
  const form = document.querySelector("[data-etf-filter-form]");
  const input = form ? form.querySelector("input[name='date']") : null;
  const button = form ? form.querySelector("button[type='submit']") : null;
  const analysisContainer = document.querySelector("[data-etf-analysis]");
  const sectionLimits = Object.fromEntries(sections.map((section) => [section.key, DEFAULT_SECTION_LIMIT]));

  function getSectionByKey(sectionKey) {
    return sections.find((section) => section.key === sectionKey);
  }

  function setMeta(dateValue) {
    if (input && dateValue) input.value = dateValue;
  }

  function renderLoadingCard(section, message, limitValue) {
    return `<div class="etf-report__card" data-section-card="${escapeHtml(section.key)}"><div class="etf-report__card-head"><div class="etf-report__card-title-row"><h2 class="etf-report__card-title">${escapeHtml(section.title)}</h2><form class="etf-report__section-filter" data-section-filter-form data-section-key="${escapeHtml(section.key)}"><label class="etf-report__section-filter-label" for="top-n-${escapeHtml(section.key)}">TOP N</label><input class="etf-report__section-filter-input" id="top-n-${escapeHtml(section.key)}" type="number" inputmode="numeric" min="1" step="1" name="limit" value="${escapeHtml(limitValue)}"></form></div><p class="etf-report__card-desc">${escapeHtml(section.description)}</p></div><div class="etf-report__empty">${escapeHtml(message)}</div></div>`;
  }

  function renderLoadingState() {
    if (summaryContainer) {
      summaryContainer.innerHTML = renderSummaryLoading("Loading data…");
    }
    if (!grid) return;
    grid.innerHTML = sections
      .map((section) => renderLoadingCard(section, "Loading data…", sectionLimits[section.key]))
      .join("");
  }

  function renderErrorState(message) {
    if (summaryContainer) {
      summaryContainer.innerHTML = renderSummaryLoading(message);
    }
    if (!grid) return;
    grid.innerHTML = `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Data unavailable</h2><p class="etf-report__card-desc">The report shell loaded, but the section data request failed.</p></div><div class="etf-report__empty">${escapeHtml(message)}</div></div>`;
    if (analysisContainer) {
      analysisContainer.innerHTML = `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Analysis</h2><p class="etf-report__card-desc">The analysis data request failed.</p></div><div class="etf-report__empty">${escapeHtml(message)}</div></div>`;
    }
  }

  function renderAnalysisCard(analysis) {
    if (!analysis || analysis.data == null) {
      return `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Analysis</h2><p class="etf-report__card-desc">Notes derived from the etf_analysis table.</p></div><div class="etf-report__empty">No analysis data found for the selected date.</div></div>`;
    }

    const payload = JSON.stringify(analysis.data, null, 2);
    const badges = [];
    if (analysis.date) {
      badges.push(`<span class="etf-report__badge">As of ${escapeHtml(analysis.date)}</span>`);
    }
    return `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Analysis</h2><p class="etf-report__card-desc">Notes derived from the etf_analysis table.</p><div class="etf-report__card-meta">${badges.join("")}</div></div><pre class="etf-report__analysis-pre">${escapeHtml(payload)}</pre></div>`;
  }

  function renderAnalysisLoading() {
    if (!analysisContainer) return;
    analysisContainer.innerHTML = `<div class="etf-report__card"><div class="etf-report__card-head"><h2 class="etf-report__card-title">Analysis</h2><p class="etf-report__card-desc">Notes derived from the etf_analysis table.</p></div><div class="etf-report__empty">Loading analysis…</div></div>`;
  }

async function loadSections(dateValue) {
    renderLoadingState();
    renderAnalysisLoading();
    if (button) button.disabled = true;

    const url = new URL("/api/sections", window.location.origin);
    if (dateValue) url.searchParams.set("date", dateValue);

    try {
      const response = await fetch(url);
      if (!response.ok) {
        let message = `Request failed with status ${response.status}`;
        try {
          const errorPayload = await response.json();
          if (errorPayload && errorPayload.error) {
            message = String(errorPayload.error);
          }
        } catch (error) {
        }
        throw new Error(message);
      }

      const payload = await response.json();
      setMeta(payload.as_of_date || payload.date || "");
      Object.entries(payload.limits || {}).forEach(function ([sectionKey, limitValue]) {
        sectionLimits[sectionKey] = limitValue;
      });

      grid.innerHTML = sections
        .map((section) => renderSection(section, payload.sections[section.key] || [], sectionLimits[section.key] || DEFAULT_SECTION_LIMIT))
        .join("");
      if (summaryContainer) {
        summaryContainer.innerHTML = renderSummaryCard(payload.macro_summary || null);
      }
      initTableSorters(grid);
      if (analysisContainer) {
        analysisContainer.innerHTML = renderAnalysisCard(payload.analysis || null);
      }

      const pageUrl = new URL(window.location.href);
      if (payload.date) {
        pageUrl.searchParams.set("date", payload.date);
      } else {
        pageUrl.searchParams.delete("date");
      }
      window.history.replaceState({}, "", pageUrl);
    } catch (error) {
      renderErrorState(error instanceof Error ? error.message : String(error));
    } finally {
      if (button) button.disabled = false;
    }
  }

  async function loadSingleSection(sectionKey, limitValue) {
    const section = getSectionByKey(sectionKey);
    const card = grid ? grid.querySelector(`[data-section-card="${sectionKey}"]`) : null;
    if (!section || !grid || !card) return;

    sectionLimits[sectionKey] = limitValue;
    card.outerHTML = renderLoadingCard(section, "Loading data…", limitValue);

    const url = new URL("/api/section", window.location.origin);
    if (input && input.value) url.searchParams.set("date", input.value);
    url.searchParams.set("key", sectionKey);
    url.searchParams.set("limit", String(limitValue));

    try {
      const response = await fetch(url);
      if (!response.ok) {
        let message = `Request failed with status ${response.status}`;
        try {
          const errorPayload = await response.json();
          if (errorPayload && errorPayload.error) {
            message = String(errorPayload.error);
          }
        } catch (error) {
        }
        throw new Error(message);
      }

      const payload = await response.json();
      sectionLimits[sectionKey] = payload.limit || limitValue;
      const nextCard = grid.querySelector(`[data-section-card="${sectionKey}"]`);
      if (nextCard) {
        nextCard.outerHTML = renderSection(section, payload.rows || [], sectionLimits[sectionKey]);
      }
      initTableSorters(grid);
      setMeta(payload.as_of_date || payload.date || "");
    } catch (error) {
      const nextCard = grid.querySelector(`[data-section-card="${sectionKey}"]`);
      if (nextCard) {
        nextCard.outerHTML = renderLoadingCard(section, error instanceof Error ? error.message : String(error), limitValue);
      }
    }
  }

  async function resolveInitialDate() {
    if (container.dataset.initialDate) {
      return container.dataset.initialDate;
    }

    try {
      const response = await fetch("/api/latest-date");
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }

      const payload = await response.json();
      const resolvedDate = payload.date || "";
      if (input && resolvedDate) input.value = resolvedDate;
      setMeta(resolvedDate);
      return resolvedDate;
    } catch (error) {
      return "";
    }
  }

  if (form) {
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      loadSections(input ? input.value : "");
    });
  }

  if (grid) {
    grid.addEventListener("submit", function (event) {
      const target = event.target;
      if (!(target instanceof HTMLFormElement)) return;
      if (!target.matches("[data-section-filter-form]")) return;

      event.preventDefault();
      const sectionKey = target.dataset.sectionKey || "";
      const limitInput = target.querySelector("input[name='limit']");
      const parsedLimit = Number(limitInput ? limitInput.value : DEFAULT_SECTION_LIMIT);
      const nextLimit = Number.isInteger(parsedLimit) && parsedLimit > 0 ? parsedLimit : DEFAULT_SECTION_LIMIT;
      if (limitInput) limitInput.value = String(nextLimit);
      loadSingleSection(sectionKey, nextLimit);
    });
  }

  renderLoadingState();
  renderAnalysisLoading();
  setMeta(container.dataset.initialDate || "");
  resolveInitialDate().then(function (resolvedDate) {
    loadSections(resolvedDate);
  });
});
