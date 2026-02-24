const fs = require('fs/promises');
const path = require('path');

const METRIC_SCHEMA = {
  marketCap: { label: 'Market Capitalization', aliases: ['Market Capitalization'], unit: 'millions SGD', type: 'number' },
  netDebtToEbitda: { label: 'Net Debt / EBITDA Ratio', aliases: ['Net Debt / EBITDA Ratio', 'Net Debt / EBITDA'], type: 'number' },
  debtToEquity: { label: 'Debt / Equity Ratio', aliases: ['Debt / Equity Ratio', 'Debt / Equity'], type: 'number' },
  netDebtToEquity: { label: 'Net Debt / Equity Ratio', aliases: ['Net Debt / Equity Ratio', 'Net Debt / Equity'], type: 'number' },
  debtToEbitda: { label: 'Debt / EBITDA Ratio', aliases: ['Debt / EBITDA Ratio', 'Debt / EBITDA'], type: 'number' },
  quickRatio: { label: 'Quick Ratio', aliases: ['Quick Ratio'], type: 'number' },
  currentRatio: { label: 'Current Ratio', aliases: ['Current Ratio'], type: 'number' },
  roic: { label: 'ROIC', aliases: ['ROIC', 'Return on Invested Capital (ROIC)'], type: 'percent' },
  roe: { label: 'ROE', aliases: ['ROE', 'Return on Equity (ROE)'], type: 'percent' },
  payoutRatio: { label: 'Payout Ratio', aliases: ['Payout Ratio'], type: 'percent' },
  assetTurnover: { label: 'Asset Turnover', aliases: ['Asset Turnover', 'Asset Turnover Ratio'], type: 'number' }
};

const MISSING_VALUE_RE = /^(?:-|--|n\/?a|na|none|null)?$/i;

function nowIso() {
  return new Date().toISOString();
}

function normalizeTicker(value = '') {
  return String(value).trim().toUpperCase().replace(/\.SI$/, '');
}

function normalizeLabel(value = '') {
  return value
    .toLowerCase()
    .replace(/\(.*?\)/g, '')
    .replace(/[^a-z0-9/% ]/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/ ratio$/g, '')
    .trim();
}

const aliasLookup = new Map(
  Object.entries(METRIC_SCHEMA).flatMap(([key, metric]) => metric.aliases.map((alias) => [normalizeLabel(alias), key]))
);

function parsePeriodLabel(raw = '') {
  const cleaned = String(raw).trim();
  if (!cleaned) return null;
  if (cleaned.toLowerCase() === 'current') return 'Current';
  const match = cleaned.match(/^(?:FY\s*)?(20\d{2})$/i);
  if (match) return `FY ${match[1]}`;
  return cleaned;
}

function parseNumeric(raw) {
  if (raw == null) return null;
  const text = String(raw).replace(/\u00a0/g, ' ').trim();
  if (!text || MISSING_VALUE_RE.test(text)) return null;
  const cleaned = text.replace(/,/g, '').replace(/×/g, '').replace(/^[A-Za-z$]+/, '').replace(/\s+/g, '').replace(/%$/, '');
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function emptyMetrics() {
  return Object.fromEntries(
    Object.entries(METRIC_SCHEMA).map(([key, schema]) => [
      key,
      { label: schema.label, unit: schema.unit || null, values: {}, rawValues: {} }
    ])
  );
}

function buildCurrent(metrics) {
  return Object.fromEntries(Object.keys(METRIC_SCHEMA).map((metricKey) => [metricKey, metrics[metricKey]?.values?.Current ?? null]));
}

function buildRatiosUrl(ticker) {
  return `https://stockanalysis.com/quote/sgx/${ticker}/financials/ratios/`;
}

function countParsedMetrics(metrics) {
  return Object.values(metrics).reduce((acc, metric) => (metric.values.Current != null ? acc + 1 : acc), 0);
}

function extractTableRows(html) {
  const tableMatch = html.match(/<table[^>]*>([\s\S]*?)<\/table>/i);
  if (!tableMatch) return [];
  const rowMatches = [...tableMatch[0].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  return rowMatches.map((row) => {
    const cellMatches = [...row[1].matchAll(/<(?:th|td)[^>]*>([\s\S]*?)<\/(?:th|td)>/gi)];
    return cellMatches.map((cell) => cell[1].replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim());
  });
}

function parseRatiosTable(html, log = () => {}) {
  const rows = extractTableRows(html);
  const headerRowIndex = rows.findIndex((row) => row[0] && (/ratio/i.test(row[0]) || row.some((cell) => /current/i.test(cell))));
  if (headerRowIndex < 0) {
    throw new Error('Unable to locate StockAnalysis ratios table header');
  }

  const periodCells = rows[headerRowIndex].slice(1).map(parsePeriodLabel);
  const periods = periodCells.filter(Boolean);
  if (!periods.length) {
    throw new Error('No periods parsed from ratios table');
  }

  const periodEndingMap = {};
  const metrics = emptyMetrics();

  rows.slice(headerRowIndex + 1).forEach((row) => {
    if (!row.length) return;
    const rowLabel = row[0];
    if (/^period ending$/i.test(rowLabel)) {
      periods.forEach((periodLabel, idx) => {
        periodEndingMap[periodLabel] = row[idx + 1] || null;
      });
      return;
    }

    const metricKey = aliasLookup.get(normalizeLabel(rowLabel));
    if (!metricKey) return;

    periods.forEach((periodLabel, idx) => {
      const rawValue = row[idx + 1] || null;
      metrics[metricKey].rawValues[periodLabel] = rawValue;
      const parsedValue = parseNumeric(rawValue);
      metrics[metricKey].values[periodLabel] = parsedValue;
      if (rawValue && parsedValue == null && !MISSING_VALUE_RE.test(rawValue)) {
        log(`parse-failure metric=${metricKey} period=${periodLabel} raw=${rawValue}`);
      }
    });
  });

  return {
    periods: periods.map((periodLabel) => ({ label: periodLabel, periodEnding: periodEndingMap[periodLabel] || null })),
    metrics,
    current: buildCurrent(metrics)
  };
}

async function fetchWithRetry(url, { retries = 2, timeoutMs = 30000, logger = console } = {}) {
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      const response = await fetch(url, {
        headers: {
          'user-agent': 'Mozilla/5.0 (SGDevelopersHealthMonitor/1.0)',
          accept: 'text/html,application/xhtml+xml'
        },
        signal: controller.signal
      });
      clearTimeout(timer);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return await response.text();
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        const delayMs = 500 * (2 ** attempt);
        logger.warn(`Retrying ${url} after error: ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }

  throw lastError;
}


function parseCsvLine(line = '') {
  const out = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  out.push(current);
  return out;
}

async function readDeveloperCsv(csvPath) {
  const content = await fs.readFile(csvPath, 'utf8');
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = Object.fromEntries(headers.map((h, idx) => [h, (cols[idx] || '').trim()]));
    const ticker = normalizeTicker(row.stockanalysis_symbol || row.sgx_ticker);
    return {
      ticker,
      name: row.company_name || '',
      segment: row.market_segment || '',
      stockanalysis_ratios_url: row.stockanalysis_ratios_url || buildRatiosUrl(ticker)
    };
  });
}

module.exports = {
  METRIC_SCHEMA,
  nowIso,
  normalizeTicker,
  buildRatiosUrl,
  parseRatiosTable,
  fetchWithRetry,
  readDeveloperCsv,
  emptyMetrics,
  buildCurrent,
  countParsedMetrics
};
