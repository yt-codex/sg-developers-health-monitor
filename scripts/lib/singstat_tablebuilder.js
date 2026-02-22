const { toFiniteNumber } = require('./datagov');

const RATES_TABLE_ID = 'M700071';
const UNIT_LABOUR_TABLE_ID = 'M183741';
const CONSTRUCTION_GDP_TABLE_ID = 'M015792';
const DEFAULT_API_BASE = process.env.SINGSTAT_TABLEBUILDER_API_BASE || 'https://tablebuilder.singstat.gov.sg/api/table/tabledata';
const DEFAULT_TIMEOUT_MS = Number(process.env.SINGSTAT_TABLEBUILDER_TIMEOUT_MS || 30000);
const DEFAULT_RETRIES = Number(process.env.SINGSTAT_TABLEBUILDER_RETRIES || 3);

const SERIES_MAP = {
  SORA: 'Singapore Overnight Rate Average',
  SGS_2Y: 'Government Securities - 2-Year Bond Yield',
  SGS_10Y: 'Government Securities - 10-Year Bond Yield',
  UNIT_LABOUR_COST_CONSTRUCTION: 'Unit labour cost of construction',
  CONSTRUCTION_GDP_SA: 'Construction'
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeLabel(text) {
  return String(text || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[‐‑‒–—−]/g, '-')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function parseMonthLabel(periodLabel) {
  const label = String(periodLabel || '').trim();
  const monthMatch = label.match(/^(\d{4})\s+([A-Za-z]{3})$/);
  if (monthMatch) {
    const monthMap = {
      jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
      jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12'
    };
    const month = monthMap[monthMatch[2].toLowerCase()];
    if (!month) return null;
    return `${monthMatch[1]}-${month}-01`;
  }

  const quarterMatch = label.match(/^(?:([Qq]([1-4]))\s*(\d{4})|(\d{4})\s*(?:[Qq]([1-4])|([1-4])[Qq]))$/);
  if (quarterMatch) {
    const year = quarterMatch[3] || quarterMatch[4];
    const quarter = Number(quarterMatch[2] || quarterMatch[5] || quarterMatch[6]);
    return `${year}-${String((quarter - 1) * 3 + 1).padStart(2, '0')}-01`;
  }

  if (/^\d{4}$/.test(label)) return `${label}-01-01`;
  return null;
}

function isoDateToQuarterPeriod(dateString) {
  const match = String(dateString || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const month = Number(match[2]);
  if (month < 1 || month > 12) return null;
  const quarter = Math.floor((month - 1) / 3) + 1;
  return `${match[1]}Q${quarter}`;
}

function findSeriesRowLabel(row) {
  const preferred = ['Data Series', 'data series', 'Series', 'series', 'rowText', 'row_label', 'label'];
  for (const key of preferred) {
    const value = row?.[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  for (const [key, value] of Object.entries(row || {})) {
    if (/series|row|label/i.test(key) && typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
}

function walk(value, visit) {
  if (value == null) return;
  visit(value);
  if (Array.isArray(value)) {
    for (const item of value) walk(item, visit);
    return;
  }
  if (typeof value === 'object') {
    for (const item of Object.values(value)) walk(item, visit);
  }
}

function findTimePeriodColumns(payload) {
  const discovered = new Set();

  walk(payload, (node) => {
    if (typeof node === 'string' && parseMonthLabel(node)) {
      discovered.add(node.trim());
      return;
    }

    if (!Array.isArray(node)) return;
    for (const item of node) {
      if (typeof item === 'string' && parseMonthLabel(item)) discovered.add(item.trim());
      if (!item || typeof item !== 'object') continue;
      for (const value of Object.values(item)) {
        if (typeof value === 'string' && parseMonthLabel(value)) discovered.add(value.trim());
      }
    }
  });

  return [...discovered];
}

function candidateDataRows(payload) {
  const rows = [];
  walk(payload, (node) => {
    if (!Array.isArray(node)) return;
    if (!node.length || !node.every((item) => item && typeof item === 'object' && !Array.isArray(item))) return;
    for (const row of node) {
      if (findSeriesRowLabel(row)) rows.push(row);
    }
  });
  return rows;
}

function parseNumericValueCandidate(rawValue) {
  return toFiniteNumber(rawValue);
}

function findValueInPeriodCell(cell) {
  if (cell == null) return null;
  if (typeof cell !== 'object' || Array.isArray(cell)) return parseNumericValueCandidate(cell);

  const preferredValueKeys = ['value', 'data', 'obsValue', 'cellValue', 'amount'];
  for (const key of preferredValueKeys) {
    const parsed = parseNumericValueCandidate(cell[key]);
    if (parsed != null) return parsed;
  }

  for (const [key, raw] of Object.entries(cell)) {
    if (/label|period|time|month|year|date|key|code|name/i.test(key)) continue;
    const parsed = parseNumericValueCandidate(raw);
    if (parsed != null) return parsed;
  }

  return null;
}

function periodLabelForCell(cell) {
  if (!cell || typeof cell !== 'object') return null;
  const candidateKeys = ['label', 'period', 'time', 'month', 'date', 'key', 'column', 'col', 'name'];
  for (const key of candidateKeys) {
    const value = cell[key];
    if (typeof value === 'string' && parseMonthLabel(value)) return value;
  }
  for (const value of Object.values(cell)) {
    if (typeof value === 'string' && parseMonthLabel(value)) return value;
  }
  return null;
}

function extractValueForPeriod(row, periodLabel) {
  const direct = parseNumericValueCandidate(row?.[periodLabel]);
  if (direct != null) return direct;

  for (const value of Object.values(row || {})) {
    if (!Array.isArray(value)) continue;
    for (const cell of value) {
      const label = periodLabelForCell(cell);
      if (!label || parseMonthLabel(label) !== parseMonthLabel(periodLabel)) continue;
      const parsed = findValueInPeriodCell(cell);
      if (parsed != null) return parsed;
    }
  }

  return null;
}

function parseTableBuilderPivotJson(payload) {
  const rows = candidateDataRows(payload);
  if (!rows.length) {
    throw new Error('SingStat response has no identifiable data-series rows');
  }

  const metadataPeriods = findTimePeriodColumns(payload);
  const periodSet = new Set(metadataPeriods);
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      if (parseMonthLabel(key)) periodSet.add(key);
    }
  }

  if (!periodSet.size) {
    throw new Error('SingStat response has no supported time period columns (e.g., "YYYY Mon", "YYYY Qn")');
  }

  const deduped = new Map();
  for (const row of rows) {
    const seriesName = findSeriesRowLabel(row);
    if (!seriesName) continue;

    for (const periodLabel of periodSet) {
      const date = parseMonthLabel(periodLabel);
      if (!date) continue;
      const value = extractValueForPeriod(row, periodLabel);
      if (value == null) continue;
      deduped.set(`${seriesName}|${date}`, { date, series_name: seriesName, value });
    }
  }

  const tidy = [...deduped.values()].sort((a, b) => (
    a.date === b.date ? a.series_name.localeCompare(b.series_name) : a.date.localeCompare(b.date)
  ));

  if (!tidy.length) {
    throw new Error('SingStat response parsing yielded zero numeric observations');
  }
  return tidy;
}

function matchRequiredSeries(tidyRows, wantedSeries) {
  const availableLabels = [...new Set(tidyRows.map((row) => row.series_name))];
  const selected = new Map();

  for (const label of availableLabels) {
    const normalized = normalizeLabel(label);
    for (const wanted of wantedSeries) {
      const isMatch = wanted.pattern ? wanted.pattern.test(normalized) : normalized === wanted.normalized;
      if (!isMatch) continue;
      if (selected.has(wanted.key) && selected.get(wanted.key) !== label) {
        throw new Error(`Ambiguous match for ${wanted.key}: "${selected.get(wanted.key)}" vs "${label}"`);
      }
      selected.set(wanted.key, label);
    }
  }

  const missing = wantedSeries.filter((wanted) => !selected.has(wanted.key));
  if (missing.length) {
    throw new Error(`Missing required series labels: ${missing.map((m) => `"${m.label}"`).join(', ')}`);
  }

  return Object.fromEntries([...selected.entries()].map(([key, label]) => [key, tidyRows.filter((row) => row.series_name === label)]));
}

async function fetchTableBuilderJson({ tableId, apiBase = DEFAULT_API_BASE, headers = {}, signal, timeoutMs = DEFAULT_TIMEOUT_MS, maxRetries = DEFAULT_RETRIES } = {}) {
  if (!tableId) throw new Error('tableId is required');
  const endpoint = `${apiBase.replace(/\/+$/, '')}/${tableId}`;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const controller = !signal ? new AbortController() : null;
    const timer = setTimeout(() => {
      if (controller) controller.abort();
    }, timeoutMs);

    try {
      const response = await fetch(endpoint, {
        headers: { accept: 'application/json', ...headers },
        signal: signal || controller.signal
      });
      if (!response.ok) {
        if (response.status >= 500 && attempt < maxRetries) {
          throw new Error(`HTTP ${response.status}`);
        }
        throw new Error(`SingStat API request failed: HTTP ${response.status} (${endpoint})`);
      }
      return { endpoint, payload: await response.json() };
    } catch (err) {
      lastError = err;
      if (attempt < maxRetries) await sleep(300 * 2 ** (attempt - 1));
    } finally {
      clearTimeout(timer);
    }
  }

  throw new Error(`SingStat API request failed after ${maxRetries} attempts (${endpoint}): ${lastError?.message || 'unknown error'}`);
}

async function extractSeries({ tableId, wantedSeries, ...options }) {
  const { endpoint, payload } = await fetchTableBuilderJson({ tableId, ...options });
  const tidyRows = parseTableBuilderPivotJson(payload);
  const selected = matchRequiredSeries(tidyRows, wantedSeries);

  const result = {};
  for (const wanted of wantedSeries) {
    const rows = [...(selected[wanted.key] || [])].sort((a, b) => a.date.localeCompare(b.date));
    result[wanted.key] = {
      endpoint,
      rows,
      latest: rows[rows.length - 1] || null
    };
  }
  return result;
}

async function fetchSingStatRequiredSeries(options = {}) {
  return extractSeries({
    tableId: options.tableId || RATES_TABLE_ID,
    wantedSeries: [
      { key: 'SORA', label: SERIES_MAP.SORA, normalized: normalizeLabel(SERIES_MAP.SORA) },
      { key: 'SGS_2Y', label: SERIES_MAP.SGS_2Y, normalized: normalizeLabel(SERIES_MAP.SGS_2Y) },
      { key: 'SGS_10Y', label: SERIES_MAP.SGS_10Y, normalized: normalizeLabel(SERIES_MAP.SGS_10Y) }
    ],
    ...options
  });
}

async function fetchUnitLabourCostConstructionSeries(options = {}) {
  return extractSeries({
    tableId: options.tableId || UNIT_LABOUR_TABLE_ID,
    wantedSeries: [{
      key: 'UNIT_LABOUR_COST_CONSTRUCTION',
      label: SERIES_MAP.UNIT_LABOUR_COST_CONSTRUCTION,
      pattern: /\bunit\b.*\blabou?r\b.*\bcost\b.*\bconstruction\b/
    }],
    ...options
  });
}

async function fetchConstructionGdpSeries(options = {}) {
  return extractSeries({
    tableId: options.tableId || CONSTRUCTION_GDP_TABLE_ID,
    wantedSeries: [{
      key: 'CONSTRUCTION_GDP_SA',
      label: SERIES_MAP.CONSTRUCTION_GDP_SA,
      pattern: /^construction$/
    }],
    ...options
  });
}

module.exports = {
  RATES_TABLE_ID,
  UNIT_LABOUR_TABLE_ID,
  CONSTRUCTION_GDP_TABLE_ID,
  DEFAULT_API_BASE,
  SERIES_MAP,
  normalizeLabel,
  parseMonthLabel,
  isoDateToQuarterPeriod,
  parseTableBuilderPivotJson,
  matchRequiredSeries,
  fetchTableBuilderJson,
  extractSeries,
  fetchSingStatRequiredSeries,
  fetchUnitLabourCostConstructionSeries,
  fetchConstructionGdpSeries
};
