const { toFiniteNumber } = require('./datagov');

const DEFAULT_TABLE_ID = 'TS/M700071';
const DEFAULT_API_BASE = process.env.SINGSTAT_TABLEBUILDER_API_BASE || 'https://tablebuilder.singstat.gov.sg/api/table/tabledata';
const DEFAULT_TIMEOUT_MS = Number(process.env.SINGSTAT_TABLEBUILDER_TIMEOUT_MS || 30000);

const SERIES_MAP = {
  SORA: 'Singapore Overnight Rate Average',
  SGS_2Y: 'Government Securities - 2-Year Bond Yield',
  SGS_10Y: 'Government Securities - 10-Year Bond Yield'
};

function normalizeLabel(text) {
  return String(text || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[‐‑‒–—−]/g, '-')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function parseMonthLabel(periodLabel) {
  const m = String(periodLabel || '').trim().match(/^(\d{4})\s+([A-Za-z]{3})$/);
  if (!m) return null;
  const monthMap = {
    jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
    jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12'
  };
  const month = monthMap[m[2].toLowerCase()];
  if (!month) return null;
  return `${m[1]}-${month}-01`;
}

function findSeriesRowLabel(row) {
  const preferred = [
    'Data Series',
    'data series',
    'Series',
    'series',
    'rowText',
    'row_label',
    'label'
  ];
  for (const key of preferred) {
    const value = row?.[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  for (const [key, value] of Object.entries(row || {})) {
    if (/series|row|label/i.test(key) && typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
}

function toTidyRowsFromWide(data) {
  if (!Array.isArray(data) || !data.length) {
    throw new Error('SingStat response missing rows array');
  }
  const wideRows = data.filter((row) => typeof row === 'object' && row && findSeriesRowLabel(row));
  if (!wideRows.length) {
    throw new Error('SingStat response has no identifiable data-series rows');
  }

  const timeColumns = Object.keys(wideRows[0]).filter((key) => parseMonthLabel(key));
  if (!timeColumns.length) {
    throw new Error('SingStat response has no "YYYY Mon" time columns');
  }

  const tidy = [];
  for (const row of wideRows) {
    const seriesName = findSeriesRowLabel(row);
    for (const periodKey of timeColumns) {
      const date = parseMonthLabel(periodKey);
      const value = toFiniteNumber(row?.[periodKey]);
      if (!date || value == null) continue;
      tidy.push({ date, series_name: seriesName, value });
    }
  }

  tidy.sort((a, b) => (a.date === b.date ? a.series_name.localeCompare(b.series_name) : a.date.localeCompare(b.date)));
  return tidy;
}

function matchRequiredSeries(tidyRows) {
  const labelToKey = Object.entries(SERIES_MAP).map(([key, label]) => ({ key, label, normalized: normalizeLabel(label) }));
  const availableLabels = [...new Set(tidyRows.map((row) => row.series_name))];
  const selected = new Map();

  for (const label of availableLabels) {
    const normalized = normalizeLabel(label);
    for (const target of labelToKey) {
      if (normalized === target.normalized) {
        if (selected.has(target.key) && selected.get(target.key) !== label) {
          throw new Error(`Ambiguous match for ${target.key}: "${selected.get(target.key)}" vs "${label}"`);
        }
        selected.set(target.key, label);
      }
    }
  }

  const missing = labelToKey.filter((target) => !selected.has(target.key));
  if (missing.length) {
    throw new Error(`Missing required series labels: ${missing.map((m) => `"${m.label}"`).join(', ')}`);
  }

  return Object.fromEntries([...selected.entries()].map(([key, label]) => [key, tidyRows.filter((row) => row.series_name === label)]));
}

async function fetchSingStatTableWide({ tableId = DEFAULT_TABLE_ID, apiBase = DEFAULT_API_BASE, headers = {}, signal } = {}) {
  const endpoint = `${apiBase.replace(/\/+$/, '')}/${tableId}`;
  const response = await fetch(endpoint, {
    headers: {
      accept: 'application/json',
      ...headers
    },
    signal
  });
  if (!response.ok) throw new Error(`SingStat API request failed: HTTP ${response.status} (${endpoint})`);

  const payload = await response.json();
  const data = payload?.Data || payload?.data || payload?.rows || payload?.result?.records;
  if (!Array.isArray(data)) {
    throw new Error(`SingStat API response shape changed: expected array at Data/data/rows/result.records (${endpoint})`);
  }

  return { endpoint, data };
}

async function fetchSingStatRequiredSeries(options = {}) {
  const timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const { endpoint, data } = await fetchSingStatTableWide({ ...options, signal: controller.signal });
    const tidyRows = toTidyRowsFromWide(data);
    const selected = matchRequiredSeries(tidyRows);

    const result = {};
    for (const [key, rows] of Object.entries(selected)) {
      const sorted = [...rows].sort((a, b) => a.date.localeCompare(b.date));
      result[key] = {
        endpoint,
        rows: sorted,
        latest: sorted[sorted.length - 1] || null
      };
    }
    return result;
  } finally {
    clearTimeout(timer);
  }
}

module.exports = {
  DEFAULT_TABLE_ID,
  DEFAULT_API_BASE,
  SERIES_MAP,
  normalizeLabel,
  parseMonthLabel,
  toTidyRowsFromWide,
  matchRequiredSeries,
  fetchSingStatRequiredSeries
};
