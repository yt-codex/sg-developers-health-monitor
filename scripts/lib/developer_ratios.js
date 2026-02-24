const fs = require('fs/promises');

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
const TABLE_SELECTORS = ['table'];
const BLOCKED_CONTENT_PATTERNS = [
  /cf-browser-verification/i,
  /cloudflare/i,
  /attention required/i,
  /captcha/i,
  /verify you are human/i,
  /access denied/i,
  /enable javascript/i,
  /checking your browser/i,
  /security check/i
];

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

function formatMarketCapCompact(millionsValue, currency = 'S$') {
  if (millionsValue == null || !Number.isFinite(millionsValue)) return null;
  if (Math.abs(millionsValue) >= 1000) return `${currency}${(millionsValue / 1000).toFixed(1)}B`;
  return `${currency}${millionsValue.toFixed(1)}M`;
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
  const tableMatches = [...html.matchAll(/<table[^>]*>([\s\S]*?)<\/table>/gi)];
  if (!tableMatches.length) return [];
  return tableMatches.map((tableMatch) => {
    const rowMatches = [...tableMatch[0].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
    return rowMatches.map((row) => {
      const cellMatches = [...row[1].matchAll(/<(?:th|td)[^>]*>([\s\S]*?)<\/(?:th|td)>/gi)];
      return cellMatches.map((cell) => cell[1].replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim());
    });
  });
}

function findMetricKeyByLabel(rowLabel = '') {
  return aliasLookup.get(normalizeLabel(rowLabel));
}

function sanitizeHtmlSnippet(html = '', maxLength = 300) {
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/\s+/g, ' ')
    .slice(0, maxLength)
    .trim();
}

function parseRatiosTable(html, options = {}) {
  const log = typeof options === 'function' ? options : (options.log || (() => {}));
  const debug = typeof options === 'object' && options ? (options.debug || null) : null;
  const tables = extractTableRows(html);

  const blockedPattern = BLOCKED_CONTENT_PATTERNS.find((pattern) => pattern.test(html));
  if (blockedPattern) {
    const blockedError = new Error(`blocked/interstitial content detected (${blockedPattern.source})`);
    blockedError.code = 'BLOCKED_INTERSTITIAL_CONTENT';
    throw blockedError;
  }

  if (debug) {
    debug.dom = {
      selectorsAttempted: TABLE_SELECTORS,
      tableCount: tables.length,
      targetTableFound: false,
      targetTableIndex: null,
      rowCount: 0,
      headerCellCount: 0,
      extractedHeaders: []
    };
  }

  let selectedTable = [];
  let headerRowIndex = -1;
  for (let tableIndex = 0; tableIndex < tables.length; tableIndex += 1) {
    const rows = tables[tableIndex];
    const maybeHeaderIndex = rows.findIndex((row) => row[0] && (/ratio/i.test(row[0]) || row.some((cell) => /current/i.test(cell))));
    if (maybeHeaderIndex >= 0) {
      selectedTable = rows;
      headerRowIndex = maybeHeaderIndex;
      if (debug) {
        debug.dom.targetTableFound = true;
        debug.dom.targetTableIndex = tableIndex;
      }
      break;
    }
  }

  if (headerRowIndex < 0) {
    const parseError = new Error('ratios table parse failure: unable to locate StockAnalysis ratios table header');
    parseError.code = 'RATIOS_TABLE_PARSE_FAILURE';
    throw parseError;
  }

  if (debug) {
    debug.dom.rowCount = selectedTable.length;
    debug.dom.headerCellCount = selectedTable[headerRowIndex]?.length || 0;
  }

  const periods = selectedTable[headerRowIndex].slice(1).map(parsePeriodLabel).filter(Boolean);
  if (debug) debug.dom.extractedHeaders = periods;
  if (!periods.length) {
    const parseError = new Error('ratios table parse failure: no periods parsed from table header');
    parseError.code = 'RATIOS_TABLE_PARSE_FAILURE';
    throw parseError;
  }

  const periodEndingMap = {};
  const metrics = emptyMetrics();
  const metricRows = [];

  selectedTable.slice(headerRowIndex + 1).forEach((row) => {
    if (!row.length) return;
    const rowLabel = row[0];
    if (/^period ending$/i.test(rowLabel)) {
      periods.forEach((periodLabel, idx) => {
        periodEndingMap[periodLabel] = row[idx + 1] || null;
      });
      return;
    }

    const metricKey = findMetricKeyByLabel(rowLabel);
    if (!metricKey) return;

    const metricDebug = {
      canonicalKey: metricKey,
      aliasesAttempted: METRIC_SCHEMA[metricKey].aliases,
      matchedRowLabel: rowLabel,
      rawRowCells: row.slice(1),
      parsedValues: {},
      status: 'matched'
    };

    periods.forEach((periodLabel, idx) => {
      const rawValue = row[idx + 1] || null;
      metrics[metricKey].rawValues[periodLabel] = rawValue;
      const parsedValue = parseNumeric(rawValue);
      metrics[metricKey].values[periodLabel] = parsedValue;
      metricDebug.parsedValues[periodLabel] = parsedValue;
      if (rawValue && parsedValue == null && !MISSING_VALUE_RE.test(rawValue)) {
        metricDebug.status = 'parse_error';
        log(`parse-failure metric=${metricKey} period=${periodLabel} raw=${rawValue}`);
      }
    });

    metricRows.push(metricDebug);
  });

  if (debug) {
    const seen = new Set(metricRows.map((row) => row.canonicalKey));
    debug.metricRows = [
      ...metricRows,
      ...Object.keys(METRIC_SCHEMA)
        .filter((key) => !seen.has(key))
        .map((key) => ({
          canonicalKey: key,
          aliasesAttempted: METRIC_SCHEMA[key].aliases,
          matchedRowLabel: null,
          rawRowCells: [],
          parsedValues: {},
          status: 'missing'
        }))
    ];
  }

  return {
    periods: periods.map((periodLabel) => ({ label: periodLabel, periodEnding: periodEndingMap[periodLabel] || null })),
    metrics,
    current: buildCurrent(metrics)
  };
}

async function fetchWithRetry(url, { retries = 2, timeoutMs = 30000, logger = console } = {}) {
  const requestProfiles = [
    {
      label: 'primary',
      headers: {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-SG,en-US;q=0.9,en;q=0.8',
        referer: 'https://stockanalysis.com/',
        'cache-control': 'no-cache',
        pragma: 'no-cache'
      }
    },
    {
      label: 'fallback',
      headers: {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        referer: 'https://www.google.com/',
        'cache-control': 'max-age=0'
      }
    }
  ];

  const createFetchError = ({ message, reason, attempt, profile, status, responseUrl, timeout }) => {
    const details = {
      url,
      attempt,
      profile,
      status: status || null,
      responseUrl: responseUrl || null,
      timeout: Boolean(timeout),
      reason: reason || null
    };
    const error = new Error(`${message}; context=${JSON.stringify(details)}`);
    error.context = details;
    return error;
  };

  let lastError;
  for (const profile of requestProfiles) {
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      const controller = new AbortController();
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        controller.abort(new Error(`timeout ${timeoutMs}ms`));
      }, timeoutMs);

      try {
        const response = await fetch(url, {
          headers: profile.headers,
          signal: controller.signal
        });
        clearTimeout(timer);

        if (!response.ok) {
          throw createFetchError({
            message: `HTTP ${response.status}`,
            reason: `non-2xx response`,
            attempt,
            profile: profile.label,
            status: response.status,
            responseUrl: response.url || url
          });
        }

        const html = await response.text();
        return {
          html,
          status: response.status,
          contentType: response.headers.get('content-type') || null,
          finalUrl: response.url || url,
          profile: profile.label
        };
      } catch (error) {
        clearTimeout(timer);
        if (error?.name === 'AbortError' || timedOut) {
          lastError = createFetchError({
            message: 'request aborted',
            reason: timedOut ? `timeout after ${timeoutMs}ms` : (error.message || 'abort signal'),
            attempt,
            profile: profile.label,
            timeout: true
          });
        } else if (error?.context) {
          lastError = error;
        } else {
          lastError = createFetchError({
            message: 'network/request failure',
            reason: error?.message || String(error),
            attempt,
            profile: profile.label
          });
        }

        if (attempt < retries || profile !== requestProfiles[requestProfiles.length - 1]) {
          const delayMs = 500 * (2 ** attempt);
          logger.warn(`Retrying ${url} using ${profile.label} profile after error: ${lastError.message}`);
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
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
  parseNumeric,
  formatMarketCapCompact,
  findMetricKeyByLabel,
  sanitizeHtmlSnippet,
  buildRatiosUrl,
  parseRatiosTable,
  fetchWithRetry,
  readDeveloperCsv,
  emptyMetrics,
  buildCurrent,
  countParsedMetrics
};
