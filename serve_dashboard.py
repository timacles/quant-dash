#!/usr/bin/env python3
"""Serve ETF report sections dynamically from Postgres."""

from __future__ import annotations

import html
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse
from wsgiref.simple_server import WSGIServer, make_server

import psycopg2
from psycopg2 import sql
import tomllib


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.toml"
PULL_STATS_PATH = BASE_DIR / "pull_stats.py"
DEFAULT_SECTION_LIMIT = 10


CSS = """
<style>
.etf-report {
  --bg: #08111f;
  --panel: #0f1b2d;
  --panel-2: #13233a;
  --line: rgba(148, 163, 184, 0.18);
  --text: #e7edf7;
  --muted: #98a7bd;
  --accent: #7dd3fc;
  --accent-2: #38bdf8;
  --good: #22c55e;
  --bad: #fb7185;
  --warn: #fbbf24;
  --shadow: 0 24px 60px rgba(2, 6, 23, 0.45);
  color: var(--text);
  background:
    radial-gradient(circle at top right, rgba(56, 189, 248, 0.12), transparent 26%),
    radial-gradient(circle at top left, rgba(34, 197, 94, 0.08), transparent 18%),
    linear-gradient(180deg, #091120 0%, #060c18 100%);
  border: 1px solid var(--line);
  border-radius: 24px;
  box-shadow: var(--shadow);
  padding: 28px;
  font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
  color-scheme: dark;
}

.etf-report * {
  box-sizing: border-box;
}

.etf-report,
.etf-report h1,
.etf-report h2,
.etf-report p,
.etf-report div,
.etf-report a,
.etf-report code,
.etf-report input,
.etf-report button {
  -webkit-text-fill-color: currentColor;
}

.etf-report__utility-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 22px;
  padding: 12px 14px;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(8, 17, 31, 0.72);
}

.etf-report__utility-label {
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.etf-report__utility-links {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.etf-report__utility-link {
  display: inline-flex;
  align-items: center;
  padding: 7px 12px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: rgba(19, 35, 58, 0.9);
  color: var(--text);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-decoration: none;
  text-transform: uppercase;
}

.etf-report__utility-link:hover,
.etf-report__utility-link:focus-visible {
  border-color: rgba(125, 211, 252, 0.45);
  background: rgba(24, 45, 72, 0.96);
  color: var(--accent);
}

.etf-report__hero {
  display: flex;
  justify-content: space-between;
  gap: 20px;
  align-items: end;
  margin-bottom: 24px;
}

.etf-report__eyebrow {
  color: var(--accent);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  margin-bottom: 10px;
}

.etf-report__title {
  margin: 0;
  font-size: clamp(30px, 4vw, 40px);
  line-height: 1.05;
  letter-spacing: -0.03em;
}

.etf-report__subtitle {
  margin: 10px 0 0;
  color: var(--muted);
  max-width: 720px;
  line-height: 1.5;
}

.etf-report__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  justify-content: flex-end;
}

.etf-report__env-badge {
  display: inline-flex;
  align-items: center;
  padding: 6px 14px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  border: 1px solid currentColor;
}

.etf-report__env-badge--dev {
  color: #fbbf24;
  background: rgba(251, 191, 36, 0.12);
  border-color: rgba(251, 191, 36, 0.35);
}

.etf-report__env-badge--stage {
  color: #fb923c;
  background: rgba(251, 146, 60, 0.12);
  border-color: rgba(251, 146, 60, 0.35);
}

.etf-report__env-badge--prod {
  color: #22c55e;
  background: rgba(34, 197, 94, 0.12);
  border-color: rgba(34, 197, 94, 0.35);
}

.etf-report__env-badge--db {
  color: #c084fc;
  background: rgba(192, 132, 252, 0.12);
  border-color: rgba(192, 132, 252, 0.35);
}

.etf-report__filter-form {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
}

.etf-report__date-input,
.etf-report__button {
  border: 1px solid var(--line);
  border-radius: 999px;
  background: rgba(8, 17, 31, 0.7);
  color: var(--text);
  padding: 10px 14px;
  font: inherit;
}

.etf-report__button {
  cursor: pointer;
}

.etf-report__grid {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.etf-report__card {
  background: linear-gradient(180deg, rgba(19, 35, 58, 0.95), rgba(12, 21, 35, 0.98));
  border: 1px solid var(--line);
  border-radius: 20px;
  overflow: hidden;
  width: 100%;
}

.etf-report__card-head {
  padding: 18px 20px 14px;
  border-bottom: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(125, 211, 252, 0.07), transparent);
}

.etf-report__card-title {
  margin: 0;
  font-size: 20px;
  line-height: 1.1;
}

.etf-report__card-title-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.etf-report__card-desc {
  margin: 8px 0 0;
  color: var(--muted);
  font-size: 14px;
  line-height: 1.45;
}

.etf-report__card-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 12px;
}

.etf-report__section-filter {
  display: flex;
  align-items: center;
  gap: 8px;
}

.etf-report__section-filter-label {
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.etf-report__section-filter-input {
  width: 76px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: rgba(8, 17, 31, 0.7);
  color: var(--text);
  padding: 8px 12px;
  font: inherit;
  text-align: center;
}

.etf-report__badge {
  padding: 6px 10px;
  border-radius: 999px;
  font-size: 12px;
  color: var(--muted);
  border: 1px solid var(--line);
  background: rgba(8, 17, 31, 0.65);
}

.etf-report__table-wrap {
  overflow-x: auto;
}

.etf-report__summary {
  margin-bottom: 18px;
}

.etf-report__summary-grid {
  padding: 20px;
  display: grid;
  gap: 14px;
}

.etf-report__summary-status {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.etf-report__summary-panels {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.etf-report__summary-box {
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(8, 17, 31, 0.45);
  padding: 14px;
}

.etf-report__summary-label {
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.etf-report__summary-value {
  margin-top: 8px;
  font-size: 24px;
  font-weight: 700;
  letter-spacing: -0.02em;
}

.etf-report__summary-list {
  display: grid;
  gap: 9px;
  margin-top: 12px;
}

.etf-report__summary-item {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
  font-size: 13px;
}

.etf-report__summary-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 12px;
}

.etf-report__summary-table th,
.etf-report__summary-table td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--line);
  font-size: 13px;
  text-align: right;
}

.etf-report__summary-table th {
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.etf-report__summary-table th:first-child,
.etf-report__summary-table td:first-child,
.etf-report__summary-table th:nth-child(2),
.etf-report__summary-table td:nth-child(2),
.etf-report__summary-table th:nth-child(3),
.etf-report__summary-table td:nth-child(3) {
  text-align: left;
}

.etf-report__table {
  width: 100%;
  border-collapse: collapse;
  min-width: 760px;
}

.etf-report__table th,
.etf-report__table td {
  padding: 12px 14px;
  border-bottom: 1px solid var(--line);
  text-align: right;
  font-size: 13px;
  vertical-align: top;
}

.etf-report__table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: rgba(8, 17, 31, 0.96);
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  cursor: pointer;
  user-select: none;
}

.etf-report__table th:hover {
  color: var(--text);
}

.etf-report__table th:first-child,
.etf-report__table td:first-child,
.etf-report__table th:nth-child(2),
.etf-report__table td:nth-child(2),
.etf-report__table th:nth-child(3),
.etf-report__table td:nth-child(3) {
  text-align: left;
}

.etf-report__symbol {
  font-weight: 700;
  letter-spacing: 0.04em;
}

.etf-report__name {
  color: var(--muted);
  display: block;
  margin-top: 2px;
  font-size: 12px;
}

.etf-report__value--pos {
  color: var(--good);
}

.etf-report__value--neg {
  color: var(--bad);
}

.etf-report__value--vol-high {
  color: #22c55e;
}

.etf-report__value--vol-low {
  color: #fbbf24;
}

.etf-report__value--vol-extreme {
  color: #c084fc;
}

.etf-report__sort-indicator {
  display: inline-block;
  min-width: 12px;
  margin-left: 6px;
  color: var(--accent);
}

.etf-report__empty {
  padding: 22px 20px 24px;
  color: var(--muted);
  font-size: 14px;
}

.etf-report__analysis {
  margin-top: 24px;
}

.etf-report__analysis-pre {
  margin: 0;
  padding: 16px;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: rgba(8, 17, 31, 0.6);
  font-family: "IBM Plex Mono", "SFMono-Regular", "Consolas", monospace;
  font-size: 12px;
  line-height: 1.5;
  color: var(--text);
  white-space: pre-wrap;
  word-break: break-word;
}

@media (max-width: 900px) {
  .etf-report {
    padding: 18px;
    border-radius: 18px;
  }

  .etf-report__hero {
    flex-direction: column;
    align-items: flex-start;
  }

  .etf-report__meta {
    justify-content: flex-start;
  }

  .etf-report__summary-status,
  .etf-report__summary-panels {
    grid-template-columns: 1fr;
  }
}
</style>
"""

SCRIPT = """
<script>
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
    return `<td data-sort-value="${escapeHtml(sortValue(column, row.symbol))}"><span class="etf-report__symbol">${escapeHtml(row.symbol ?? "")}</span><span class="etf-report__name">${escapeHtml(row.asset_class ?? "")}</span></td>`;
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

document.addEventListener("DOMContentLoaded", function () {
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
</script>
"""

PULL_STATS_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pull Stats</title>
  <style>
    :root {
      --bg: #09111f;
      --panel: #0f1b2d;
      --panel-2: #13233a;
      --line: rgba(148, 163, 184, 0.18);
      --text: #e7edf7;
      --muted: #98a7bd;
      --accent: #7dd3fc;
      --warn: #fbbf24;
      --shadow: 0 24px 60px rgba(2, 6, 23, 0.45);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      padding: 24px;
      background:
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.12), transparent 26%),
        linear-gradient(180deg, #091120 0%, #060c18 100%);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    }

    .pull-stats {
      max-width: 1100px;
      margin: 0 auto;
      background: linear-gradient(180deg, rgba(15, 27, 45, 0.96), rgba(10, 18, 31, 0.98));
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      padding: 24px;
    }

    .pull-stats__title {
      margin: 0;
      font-size: clamp(28px, 4vw, 40px);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }

    .pull-stats__subtitle {
      margin: 10px 0 22px;
      color: var(--muted);
      line-height: 1.5;
      max-width: 720px;
    }

    .pull-stats__form {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
    }

    .pull-stats__field {
      display: grid;
      gap: 8px;
    }

    .pull-stats__field--check {
      align-self: center;
      padding-top: 22px;
    }

    .pull-stats__label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .pull-stats__input,
    .pull-stats__button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(8, 17, 31, 0.75);
      color: var(--text);
      padding: 12px 14px;
      font: inherit;
    }

    .pull-stats__checks {
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
      color: var(--muted);
    }

    .pull-stats__check {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 14px;
    }

    .pull-stats__button {
      cursor: pointer;
      background: linear-gradient(180deg, rgba(125, 211, 252, 0.18), rgba(56, 189, 248, 0.12));
      color: var(--accent);
      font-weight: 700;
    }

    .pull-stats__button:disabled {
      cursor: wait;
      opacity: 0.7;
    }

    .pull-stats__hint {
      margin: 14px 0 0;
      color: var(--warn);
      font-size: 13px;
    }

    .pull-stats__output {
      margin-top: 20px;
      min-height: 320px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(8, 17, 31, 0.8);
      padding: 18px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "IBM Plex Mono", "SFMono-Regular", "Consolas", monospace;
      font-size: 12px;
      line-height: 1.55;
    }

    @media (max-width: 900px) {
      .pull-stats__form {
        grid-template-columns: 1fr;
      }

      .pull-stats__field--check {
        padding-top: 0;
      }
    }
  </style>
</head>
<body>
  <section class="pull-stats">
    <h1 class="pull-stats__title">Pull Stats</h1>
    <p class="pull-stats__subtitle">Run the existing <code>pull_stats.py</code> CLI from the browser and stream stdout and stderr into this page while it runs.</p>
    <form class="pull-stats__form" data-pull-stats-form>
      <div class="pull-stats__field">
        <label class="pull-stats__label" for="symbol">Symbol</label>
        <input class="pull-stats__input" id="symbol" name="symbol" placeholder="Optional single ETF, e.g. XLF">
      </div>
      <div class="pull-stats__field">
        <label class="pull-stats__label" for="days-back">Days Back</label>
        <input class="pull-stats__input" id="days-back" name="days_back" type="number" min="1" step="1" value="7">
      </div>
      <div class="pull-stats__field">
        <label class="pull-stats__label" for="db-target">DB Target</label>
        <input class="pull-stats__input" id="db-target" name="db_target" placeholder="Optional, e.g. dev">
      </div>
      <div class="pull-stats__field pull-stats__field--check">
        <div class="pull-stats__checks">
          <label class="pull-stats__check"><input type="checkbox" name="print_data"> Print data</label>
          <label class="pull-stats__check"><input type="checkbox" name="desc"> Description only</label>
        </div>
      </div>
      <div class="pull-stats__field">
        <button class="pull-stats__button" type="submit">Run pull_stats.py</button>
      </div>
    </form>
    <p class="pull-stats__hint">This endpoint executes the same CLI script in-process as a subprocess. Long runs will occupy one worker thread.</p>
    <pre class="pull-stats__output" data-pull-stats-output>Waiting to start.</pre>
  </section>
  <script>
    document.addEventListener("DOMContentLoaded", function () {
      const form = document.querySelector("[data-pull-stats-form]");
      const output = document.querySelector("[data-pull-stats-output]");
      const button = form ? form.querySelector("button[type='submit']") : null;
      if (!form || !output || !button) return;

      function appendChunk(chunk) {
        output.textContent += chunk;
        output.scrollTop = output.scrollHeight;
      }

      form.addEventListener("submit", async function (event) {
        event.preventDefault();
        button.disabled = true;
        output.textContent = "";

        const params = new URLSearchParams();
        const formData = new FormData(form);
        const symbol = String(formData.get("symbol") || "").trim();
        const daysBack = String(formData.get("days_back") || "").trim();
        const dbTarget = String(formData.get("db_target") || "").trim();

        if (symbol) params.set("symbol", symbol);
        if (daysBack) params.set("days_back", daysBack);
        if (dbTarget) params.set("db_target", dbTarget);
        if (formData.get("print_data")) params.set("print_data", "1");
        if (formData.get("desc")) params.set("desc", "1");

        try {
          const response = await fetch(`/pull_stats/stream?${params.toString()}`);
          if (!response.ok || !response.body) {
            throw new Error(`Request failed with status ${response.status}`);
          }

          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          while (true) {
            const result = await reader.read();
            if (result.done) break;
            appendChunk(decoder.decode(result.value, { stream: true }));
          }
          appendChunk(decoder.decode());
        } catch (error) {
          appendChunk(`\\n[error] ${error instanceof Error ? error.message : String(error)}\\n`);
        } finally {
          button.disabled = false;
        }
      });
    });
  </script>
</body>
</html>
""".strip()


CONFIG_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Config</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #060c18;
      --line: rgba(148, 163, 184, 0.18);
      --text: #e7edf7;
      --muted: #98a7bd;
      --accent: #7dd3fc;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      padding: 32px 18px;
      background:
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.12), transparent 24%),
        linear-gradient(180deg, #091120 0%, var(--bg) 100%);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    }

    .config-page {
      max-width: 760px;
      margin: 0 auto;
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: linear-gradient(180deg, rgba(19, 35, 58, 0.95), rgba(12, 21, 35, 0.98));
      box-shadow: 0 24px 60px rgba(2, 6, 23, 0.45);
    }

    .config-page__eyebrow {
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.16em;
      text-transform: uppercase;
    }

    .config-page__title {
      margin: 12px 0 10px;
      font-size: clamp(30px, 4vw, 40px);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }

    .config-page__body {
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
    }

    .config-page__back {
      display: inline-flex;
      margin-top: 20px;
      color: var(--accent);
      text-decoration: none;
      font-weight: 700;
    }
  </style>
</head>
<body>
  <section class="config-page">
    <div class="config-page__eyebrow">Config</div>
    <h1 class="config-page__title">Configuration Placeholder</h1>
    <p class="config-page__body">Configuration details are not exposed here yet. This page is reserved for future dashboard settings and operational links.</p>
    <a class="config-page__back" href="/">Back to dashboard</a>
  </section>
</body>
</html>
""".strip()


@dataclass(frozen=True)
class SectionConfig:
    key: str
    title: str
    description: str
    type: str
    source: str


@dataclass(frozen=True)
class SectionDisplayConfig:
    columns: tuple[str, ...]
    column_labels: dict[str, str]


@dataclass(frozen=True)
class ResolvedSectionConfig:
    key: str
    title: str
    description: str
    type: str
    source: str
    columns: tuple[str, ...]
    column_labels: dict[str, str]


SECTIONS: tuple[SectionConfig, ...] = (
    SectionConfig(
        key="momentum_longs",
        title="Momentum Longs",
        description="Upside momentum leaders ranked by the existing cross-sectional composite score.",
        type="table",
        source="vw_etf_report_momentum_longs",
    ),
    SectionConfig(
        key="momentum_shorts",
        title="Momentum Shorts",
        description="Downside momentum leaders ranked by the existing cross-sectional composite score.",
        type="table",
        source="vw_etf_report_momentum_shorts",
    ),
    SectionConfig(
        key="oversold_mean_reversion",
        title="Oversold Mean Reversion",
        description="Long-reversion candidates with the strongest downside stretch and reversal setup.",
        type="table",
        source="vw_etf_report_oversold_mean_reversion",
    ),
    SectionConfig(
        key="overbought_mean_reversion",
        title="Overbought Mean Reversion",
        description="Short-reversion candidates with the most extended upside stretch.",
        type="table",
        source="vw_etf_report_overbought_mean_reversion",
    ),
    SectionConfig(
        key="range_compression",
        title="Range Compression",
        description="Tightening setups ranked by compression and accumulation features.",
        type="table",
        source="vw_etf_report_range_compression",
    ),
)

PERCENT_COLUMNS = {"ret_1d", "ret_3d", "ret_5d", "ret_10d", "rs_5", "rs_10"}
SIGNED_COLUMNS = {"ret_1d", "ret_3d", "ret_5d", "ret_10d", "rs_5", "rs_10", "zscore_close_20", "atr_stretch_20"}
DECIMAL_2_COLUMNS = {"rvol_20", "composite_score"}
DECIMAL_3_COLUMNS = {
    "zscore_close_20",
    "atr_stretch_20",
    "close_location_20",
    "volume_ratio_5_20",
    "range_compression_5_20",
    "range_compression_5_60",
    "atr_compression_5_20",
}


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("rb") as handle:
        return tomllib.load(handle)


def resolve_database_config(config: dict[str, Any]) -> dict[str, Any]:
    environment = str(config.get("site", {}).get("environment", "dev")).strip() or "dev"
    database_config = config.get("database", {})
    db_config = database_config.get(environment)
    if not isinstance(db_config, dict):
        raise ValueError(f"Missing database config for environment '{environment}'")
    return db_config


def build_connection_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    db_config = resolve_database_config(config)
    allowed_keys = ("dbname", "host", "port", "user", "password", "service")
    connect_kwargs = {key: value for key, value in db_config.items() if key in allowed_keys and value not in (None, "")}
    environment = str(config.get("site", {}).get("environment", "dev")).strip() or "dev"
    if not connect_kwargs:
        raise ValueError(f"Database config for environment '{environment}' is empty or invalid")
    return connect_kwargs


def parse_json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Expected a JSON object for dashboard column labels")


def fetch_section_display_configs(
    conn: psycopg2.extensions.connection,
    sections: Iterable[SectionConfig],
) -> dict[str, SectionDisplayConfig]:
    section_list = tuple(sections)
    section_keys = [section.key for section in section_list if section.type == "table"]
    if not section_keys:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT section_key, columns, column_labels
            FROM config.etf_dashboard_section_config
            WHERE section_key = ANY(%s)
            """,
            [section_keys],
        )
        config_rows = {
            str(section_key): SectionDisplayConfig(
                columns=tuple(columns or ()),
                column_labels={str(key): str(value) for key, value in parse_json_object(column_labels).items()},
            )
            for section_key, columns, column_labels in cur.fetchall()
        }

    view_names = [section.source for section in section_list if section.type == "table"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
            """,
            [view_names],
        )
        available_columns: dict[str, set[str]] = {}
        for table_name, column_name in cur.fetchall():
            available_columns.setdefault(str(table_name), set()).add(str(column_name))

    missing_sections = [section.key for section in section_list if section.type == "table" and section.key not in config_rows]
    if missing_sections:
        raise ValueError(f"Missing dashboard config for section(s): {', '.join(sorted(missing_sections))}")

    for section in section_list:
        if section.type != "table":
            continue
        display_config = config_rows[section.key]
        if not display_config.columns:
            raise ValueError(f"Dashboard config for section '{section.key}' must include at least one column")

        valid_columns = available_columns.get(section.source, set())
        if not valid_columns:
            raise ValueError(f"Source view '{section.source}' for section '{section.key}' has no discoverable columns")

        invalid_columns = [column for column in display_config.columns if column not in valid_columns]
        if invalid_columns:
            raise ValueError(
                f"Dashboard config for section '{section.key}' references invalid column(s): {', '.join(invalid_columns)}"
            )

        label_keys = set(display_config.column_labels)
        expected_keys = set(display_config.columns)
        if label_keys != expected_keys:
            missing_labels = sorted(expected_keys - label_keys)
            extra_labels = sorted(label_keys - expected_keys)
            problems: list[str] = []
            if missing_labels:
                problems.append(f"missing labels for {', '.join(missing_labels)}")
            if extra_labels:
                problems.append(f"extra labels for {', '.join(extra_labels)}")
            raise ValueError(f"Dashboard config for section '{section.key}' has invalid labels: {'; '.join(problems)}")

    return config_rows


def resolve_sections(
    sections: Iterable[SectionConfig],
    display_configs: dict[str, SectionDisplayConfig],
) -> tuple[ResolvedSectionConfig, ...]:
    resolved_sections = []
    for section in sections:
        display_config = display_configs.get(section.key)
        if section.type == "table" and display_config is None:
            raise ValueError(f"Missing dashboard display config for section '{section.key}'")
        resolved_sections.append(
            ResolvedSectionConfig(
                key=section.key,
                title=section.title,
                description=section.description,
                type=section.type,
                source=section.source,
                columns=display_config.columns if display_config else (),
                column_labels=display_config.column_labels if display_config else {},
            )
        )
    return tuple(resolved_sections)


def fetch_macro_summary(
    conn: psycopg2.extensions.connection,
    report_date: str | None,
) -> dict[str, Any]:
    try:
        date_sql = sql.SQL("%s") if report_date else sql.SQL("(SELECT max(date) FROM public.vw_macro_signal_dashboard)")
        query = sql.SQL(
            """
            SELECT *
            FROM public.vw_macro_signal_dashboard
            WHERE date = {date_sql}
            LIMIT 1
            """
        ).format(date_sql=date_sql)

        params: list[Any] = []
        if report_date:
            params.append(report_date)

        with conn.cursor() as cur:
            cur.execute(query, params)
            names = [desc[0] for desc in cur.description]
            macro_row = cur.fetchone()
            macro = dict(zip(names, macro_row)) if macro_row else None

        summary_date = serialize_date(macro["date"]) if macro and macro.get("date") else serialize_date(report_date)
        leaders: list[dict[str, Any]] = []
        if summary_date:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rank, symbol, display_name, bond_bucket, composite_score, date
                    FROM public.vw_etf_report_bond_credit_performance
                    WHERE date = %s
                    ORDER BY rank
                    LIMIT 3
                    """,
                    [summary_date],
                )
                names = [desc[0] for desc in cur.description]
                leaders = [dict(zip(names, row)) for row in cur.fetchall()]

        if macro and macro.get("date") is not None:
            macro["date"] = serialize_date(macro["date"])
        for row in leaders:
            if row.get("date") is not None:
                row["date"] = serialize_date(row["date"])

        return {
            "date": summary_date,
            "macro": macro,
            "bond_leaders": leaders,
        }
    except Exception:
        return {
            "date": serialize_date(report_date),
            "macro": None,
            "bond_leaders": [],
        }


def fetch_section_rows(
    conn: psycopg2.extensions.connection,
    section: SectionConfig,
    limit: int,
    report_date: str | None,
) -> list[dict[str, Any]]:
    if section.type != "table":
        return []

    date_sql = sql.SQL("%s") if report_date else sql.SQL("(SELECT max(date) FROM public.{view})").format(
        view=sql.Identifier(section.source)
    )
    query = sql.SQL(
        """
        SELECT *
        FROM public.{view}
        WHERE date = {date_sql}
        ORDER BY rank
        LIMIT %s
        """
    ).format(view=sql.Identifier(section.source), date_sql=date_sql)

    params: list[Any] = []
    if report_date:
        params.append(report_date)
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        names = [desc[0] for desc in cur.description]
        rows = [dict(zip(names, row)) for row in cur.fetchall()]
        for row in rows:
            if "date" in row:
                row["date"] = serialize_date(row["date"])
        return rows


def fetch_latest_report_date(conn: psycopg2.extensions.connection) -> Any:
    view_queries = [
        sql.SQL("SELECT max(date) AS latest_date FROM public.{view}").format(view=sql.Identifier(section.source))
        for section in SECTIONS
        if section.type == "table"
    ]
    if not view_queries:
        return None

    query = sql.SQL("SELECT max(latest_date) FROM ({views}) AS latest_dates").format(
        views=sql.SQL(" UNION ALL ").join(view_queries)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row[0] if row else None


def fetch_analysis_row(
    conn: psycopg2.extensions.connection,
    report_date: str | None,
) -> dict[str, Any] | None:
    if report_date:
        query = """
            SELECT created, data
            FROM public.etf_analysis
            WHERE created::date = %s
            ORDER BY created DESC
            LIMIT 1
        """
        params = [report_date]
    else:
        query = """
            SELECT created, data
            FROM public.etf_analysis
            ORDER BY created DESC
            LIMIT 1
        """
        params = []

    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        if not row:
            return None

    created, data = row
    date_value = created.date().isoformat() if hasattr(created, "date") else serialize_date(created)
    return {
        "date": date_value,
        "created": serialize_date(created),
        "data": data,
    }


def serialize_date(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def parse_limit(raw_value: str | None) -> int:
    try:
        limit = int(raw_value) if raw_value is not None else DEFAULT_SECTION_LIMIT
    except (TypeError, ValueError):
        return DEFAULT_SECTION_LIMIT
    return limit if limit > 0 else DEFAULT_SECTION_LIMIT


def get_section(section_key: str) -> SectionConfig | None:
    return next((section for section in SECTIONS if section.key == section_key), None)


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def sort_value(column: str, value: Any) -> str:
    if value is None:
        return ""
    if column in PERCENT_COLUMNS:
        return f"{float(value) * 100.0:.8f}"
    if isinstance(value, (int, float)):
        return f"{float(value):.8f}"
    return str(value)


def format_value(column: str, value: Any) -> str:
    if value is None:
        return "—"
    if column == "rank":
        return str(value)
    if column in PERCENT_COLUMNS:
        return f"{float(value) * 100.0:+.2f}%"
    if column in DECIMAL_2_COLUMNS:
        return f"{float(value):.2f}"
    if column in DECIMAL_3_COLUMNS:
        return f"{float(value):.3f}"
    return str(value)


def value_class(column: str, value: Any) -> str:
    if value is None or column not in SIGNED_COLUMNS.union(PERCENT_COLUMNS):
        return ""
    number = float(value)
    if number > 0:
        return " etf-report__value--pos"
    if number < 0:
        return " etf-report__value--neg"
    return ""


def render_cell(column: str, row: dict[str, Any]) -> str:
    if column == "symbol":
        return (
            f"<td data-sort-value='{escape(sort_value(column, row['symbol']))}'><span class='etf-report__symbol'>{escape(row['symbol'])}</span>"
            f"<span class='etf-report__name'>{escape(row.get('asset_class', ''))}</span></td>"
        )
    if column == "display_name":
        return f"<td data-sort-value='{escape(sort_value(column, row[column]))}'>{escape(row[column])}</td>"
    classes = "etf-report__value" + value_class(column, row.get(column))
    return (
        f"<td class='{classes.strip()}' data-sort-value='{escape(sort_value(column, row.get(column)))}'>"
        f"{escape(format_value(column, row.get(column)))}</td>"
    )


def render_table_section(section: ResolvedSectionConfig, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return (
            "<div class='etf-report__card'>"
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
            f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
            "<div class='etf-report__empty'>No qualifying rows for the selected date.</div>"
            "</div>"
        )

    as_of_date = rows[0]["date"]
    market_regime = rows[0].get("market_regime")
    header_html = "".join(
        f"<th data-sort-index='{idx}' data-sort-direction=''>{escape(section.column_labels.get(col, col))}<span class='etf-report__sort-indicator'></span></th>"
        for idx, col in enumerate(section.columns)
    )
    body_rows = []
    for row in rows:
        cell_html = "".join(render_cell(col, row) for col in section.columns)
        body_rows.append(f"<tr>{cell_html}</tr>")

    badges = [
        f"<span class='etf-report__badge'>As of {escape(as_of_date)}</span>",
        f"<span class='etf-report__badge'>{len(rows)} rows</span>",
    ]
    if market_regime:
        badges.insert(1, f"<span class='etf-report__badge'>Market Regime: {escape(market_regime)}</span>")

    return f"""
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">{escape(section.title)}</h2>
        <p class="etf-report__card-desc">{escape(section.description)}</p>
        <div class="etf-report__card-meta">{''.join(badges)}</div>
      </div>
      <div class="etf-report__table-wrap">
        <table class="etf-report__table">
          <thead><tr>{header_html}</tr></thead>
          <tbody>{''.join(body_rows)}</tbody>
        </table>
      </div>
    </div>
    """


def render_section(section: ResolvedSectionConfig, data: list[dict[str, Any]]) -> str:
    if section.type == "table":
        return render_table_section(section, data)
    return (
        "<div class='etf-report__card'>"
        f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
        f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
        "<div class='etf-report__empty'>Section type not implemented yet.</div>"
        "</div>"
    )


def render_fragment(
    site_config: dict[str, Any],
    sections_config: Iterable[ResolvedSectionConfig],
    selected_date: str | None,
    db_config: dict[str, Any] | None = None,
) -> str:
    date_value = escape(selected_date or "")
    sections_json = escape(
        json.dumps(
            [
                {
                    "key": section.key,
                    "title": section.title,
                    "description": section.description,
                    "columns": list(section.columns),
                    "column_labels": section.column_labels,
                }
                for section in sections_config
            ]
        )
    )
    loading_sections = "".join(
        (
            "<div class='etf-report__card'>"
            f"<div class='etf-report__card-head'><h2 class='etf-report__card-title'>{escape(section.title)}</h2>"
            f"<p class='etf-report__card-desc'>{escape(section.description)}</p></div>"
            "<div class='etf-report__empty'>Loading data…</div>"
            "</div>"
        )
        for section in sections_config
    )

    return f"""
{CSS}
<section class="etf-report" data-etf-report data-sections='{sections_json}' data-initial-date="{date_value}">
  <div class="etf-report__utility-bar">
    <div class="etf-report__utility-label">Operations</div>
    <div class="etf-report__utility-links">
      <a class="etf-report__utility-link" href="/pull_stats">Pull Stats</a>
      <a class="etf-report__utility-link" href="/config">Config</a>
    </div>
  </div>
  <div class="etf-report__hero">
    <div>
      <div class="etf-report__eyebrow">{escape(site_config.get('eyebrow', 'Momentum Snapshot'))}</div>
      <h1 class="etf-report__title">{escape(site_config.get('title', 'ETF Ranking Dashboard'))}</h1>
      <p class="etf-report__subtitle">{escape(site_config.get('subtitle', ''))}</p>
    </div>
    <div class="etf-report__meta">
      {f'<span class="etf-report__env-badge etf-report__env-badge--db">{escape((db_config or {}).get("dbname", ""))}</span>' if (db_config or {}).get("dbname") else ""}
      <span class="etf-report__env-badge etf-report__env-badge--{escape(site_config.get('environment', 'dev'))}">{escape(site_config.get('environment', 'dev'))}</span>
      <form class="etf-report__filter-form" method="get" data-etf-filter-form>
        <input class="etf-report__date-input" type="date" name="date" value="{date_value}" onchange="this.form.submit()">
      </form>
    </div>
  </div>
  <div class="etf-report__summary" data-etf-summary>
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">Macro Summary</h2>
        <p class="etf-report__card-desc">Compact macro regime, leadership, bond internals, and top bond momentum.</p>
      </div>
      <div class="etf-report__empty">Loading data…</div>
    </div>
  </div>
  <div class="etf-report__grid" data-etf-grid>
    {loading_sections}
  </div>
  <div class="etf-report__analysis" data-etf-analysis>
    <div class="etf-report__card">
      <div class="etf-report__card-head">
        <h2 class="etf-report__card-title">Analysis</h2>
        <p class="etf-report__card-desc">Notes derived from the etf_analysis table.</p>
      </div>
      <div class="etf-report__empty">Loading analysis…</div>
    </div>
  </div>
</section>
{SCRIPT}
""".strip()


def render_document(fragment: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
</head>
<body style="margin:0; padding:24px; background:#030712;">
{fragment}
</body>
</html>
"""


def json_error(start_response: Any, status: str, message: str) -> list[bytes]:
    start_response(status, [("Content-Type", "application/json; charset=utf-8")])
    return [json.dumps({"error": message}).encode("utf-8")]


def build_pull_stats_command(query: dict[str, list[str]]) -> list[str]:
    command = [sys.executable, "-u", str(PULL_STATS_PATH)]

    symbol = query.get("symbol", [""])[0].strip()
    if symbol:
        command.extend(["--symbol", symbol])

    days_back = query.get("days_back", [""])[0].strip()
    if days_back:
        command.extend(["--days-back", days_back])

    db_target = query.get("db_target", [""])[0].strip()
    if db_target:
        command.extend(["--db-target", db_target])

    if query.get("print_data", [""])[0] in {"1", "true", "yes", "on"}:
        command.append("--print-data")

    if query.get("desc", [""])[0] in {"1", "true", "yes", "on"}:
        command.append("--desc")

    return command


def stream_pull_stats(query: dict[str, list[str]]) -> Iterable[bytes]:
    command = build_pull_stats_command(query)
    pretty_command = " ".join(shlex.quote(part) for part in command)
    yield f"$ {pretty_command}\n\n".encode("utf-8")

    if not PULL_STATS_PATH.exists():
        yield f"pull_stats.py not found at {PULL_STATS_PATH}\n".encode("utf-8")
        return

    process = subprocess.Popen(
        command,
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    try:
        for line in process.stdout:
            yield line.encode("utf-8", errors="replace")
    finally:
        process.stdout.close()

    return_code = process.wait()
    status_line = f"\n[exit code {return_code}]\n"
    yield status_line.encode("utf-8")


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def build_page(
    config: dict[str, Any],
    resolved_sections: Iterable[ResolvedSectionConfig],
    report_date: str | None,
) -> str:
    fragment = render_fragment(config.get("site", {}), resolved_sections, report_date, resolve_database_config(config))
    return render_document(fragment, config.get("site", {}).get("title", "ETF Ranking Dashboard"))


def app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
    parsed = urlparse(environ.get("PATH_INFO", "/"))
    if parsed.path == "/healthz":
        start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"ok"]

    if parsed.path == "/pull_stats":
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [PULL_STATS_PAGE.encode("utf-8")]

    if parsed.path == "/config":
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [CONFIG_PAGE.encode("utf-8")]

    if parsed.path == "/pull_stats/stream":
        query = parse_qs(environ.get("QUERY_STRING", ""))
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/plain; charset=utf-8"),
                ("Cache-Control", "no-cache"),
            ],
        )
        return stream_pull_stats(query)

    if parsed.path == "/api/latest-date":
        config = load_config()
        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            latest_date = fetch_latest_report_date(conn)
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [json.dumps({"date": serialize_date(latest_date)}).encode("utf-8")]

    if parsed.path == "/api/sections":
        try:
            config = load_config()
            query = parse_qs(environ.get("QUERY_STRING", ""))
            report_date = query.get("date", [None])[0]
            limit = parse_limit(query.get("limit", [None])[0])
            connect_kwargs = build_connection_kwargs(config)
            payload: dict[str, Any] = {
                "date": report_date,
                "as_of_date": report_date,
                "limit": limit,
                "limits": {},
                "macro_summary": None,
                "sections": {},
                "analysis": None,
            }
            with psycopg2.connect(**connect_kwargs) as conn:
                fetch_section_display_configs(conn, SECTIONS)
                payload["macro_summary"] = fetch_macro_summary(conn, report_date)
                for section in SECTIONS:
                    rows = fetch_section_rows(conn, section, limit, report_date)
                    payload["sections"][section.key] = rows
                    payload["limits"][section.key] = limit
                payload["as_of_date"] = serialize_date(
                    next((rows[0]["date"] for rows in payload["sections"].values() if rows), report_date)
                )
                if not payload["as_of_date"] and payload["macro_summary"]:
                    payload["as_of_date"] = payload["macro_summary"].get("date")
                if not payload["date"]:
                    payload["date"] = payload["as_of_date"]
                payload["analysis"] = fetch_analysis_row(conn, payload["date"])
            start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
            return [json.dumps(payload).encode("utf-8")]
        except Exception as exc:
            return json_error(start_response, "500 Internal Server Error", str(exc))

    if parsed.path == "/api/section":
        try:
            config = load_config()
            query = parse_qs(environ.get("QUERY_STRING", ""))
            report_date = query.get("date", [None])[0]
            section_key = query.get("key", [""])[0]
            limit = parse_limit(query.get("limit", [None])[0])
            section = get_section(section_key)
            if section is None:
                return json_error(start_response, "404 Not Found", "Unknown section")

            connect_kwargs = build_connection_kwargs(config)
            with psycopg2.connect(**connect_kwargs) as conn:
                fetch_section_display_configs(conn, SECTIONS)
                rows = fetch_section_rows(conn, section, limit, report_date)
            as_of_date = serialize_date(rows[0]["date"]) if rows else report_date
            payload = {
                "key": section.key,
                "date": report_date or as_of_date,
                "as_of_date": as_of_date,
                "limit": limit,
                "rows": rows,
            }
            start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
            return [json.dumps(payload).encode("utf-8")]
        except Exception as exc:
            return json_error(start_response, "500 Internal Server Error", str(exc))

    try:
        config = load_config()
        query = parse_qs(environ.get("QUERY_STRING", ""))
        report_date = query.get("date", [None])[0]
        connect_kwargs = build_connection_kwargs(config)
        with psycopg2.connect(**connect_kwargs) as conn:
            display_configs = fetch_section_display_configs(conn, SECTIONS)
        resolved_sections = resolve_sections(SECTIONS, display_configs)
        body = build_page(config, resolved_sections, report_date)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body.encode("utf-8")]
    except Exception as exc:
        start_response("500 Internal Server Error", [("Content-Type", "text/plain; charset=utf-8")])
        return [f"Failed to render ETF site: {exc}\n".encode("utf-8")]


def main() -> int:
    config = load_config()
    server_config = config.get("server", {})
    host = server_config.get("host", "127.0.0.1")
    port = int(server_config.get("port", 8000))
    with make_server(host, port, app, server_class=ThreadingWSGIServer) as httpd:
        print(f"Serving ETF site on http://{host}:{port}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
