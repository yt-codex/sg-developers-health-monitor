const MAS_I6_API_URL = 'https://www.mas.gov.sg/api/v1/MAS/chart/table_i_6_commercial_banks_loan_limits_granted_to_non_bank_customers_by_industry';

const REQUIRED_TIME_COLUMN = 'End of Period';
const GRANTED_HEADER = 'Building and Construction - Limits Granted (S$M)';
const UTILISED_HEADER = 'Building and Construction - Utilised (%)';

const DEFAULT_HEADERS = {
  accept: 'application/json',
  'user-agent': 'macro-indicator-bot/1.0'
};

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, opts = {}) {
  const {
    headers = {},
    method = 'GET',
    body,
    maxRetries = 5
  } = opts;

  let lastError;
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const res = await fetch(url, {
        method,
        body,
        headers: {
          ...DEFAULT_HEADERS,
          ...headers
        }
      });
      if (res.ok) return await res.json();

      const retriable = res.status === 429 || (res.status >= 500 && res.status <= 599);
      const message = `HTTP ${res.status}`;
      if (!retriable || attempt >= maxRetries) {
        throw new Error(message);
      }
      lastError = new Error(message);
    } catch (err) {
      lastError = err;
      if (attempt >= maxRetries) break;
    }

    const backoffMs = Math.min(16_000, 500 * 2 ** (attempt - 1));
    await delay(backoffMs);
  }

  throw new Error(`MAS API request failed after ${maxRetries} attempts: ${lastError?.message || 'unknown error'}`);
}

function parseMasPeriod(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;

  const prelim = /\(\s*p\s*\)/i.test(raw);
  const monthMatch = raw.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i);
  const yearMatch4 = raw.match(/\b(20\d{2})\b/);
  const yearMatch2 = !yearMatch4 ? raw.match(/(?:^|[^\d])(\d{2})(?:[^\d]|$)/) : null;

  if (!monthMatch || (!yearMatch4 && !yearMatch2)) return null;

  const monthToken = monthMatch[1].slice(0, 3).toLowerCase();
  const monthMap = {
    jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
    jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12'
  };
  const month = monthMap[monthToken];
  if (!month) return null;

  const year = yearMatch4 ? Number(yearMatch4[1]) : 2000 + Number(yearMatch2[1]);
  if (!Number.isFinite(year)) return null;

  return {
    period: `${String(year).padStart(4, '0')}-${month}`,
    prelim
  };
}

function normalizeColumns(columns) {
  if (!Array.isArray(columns)) return [];
  return columns
    .map((col) => {
      if (typeof col === 'string') return col;
      return String(col?.name || col?.label || col?.field || col?.id || '').trim();
    })
    .filter(Boolean);
}

function getRowsFromPayload(payload) {
  if (!payload || typeof payload !== 'object') return null;

  if (Array.isArray(payload.rows)) {
    return { columns: normalizeColumns(payload.columns), rows: payload.rows };
  }

  const objectCandidates = [payload.data, payload.result, payload.results, payload.response].filter(
    (item) => item && typeof item === 'object'
  );

  for (const candidate of objectCandidates) {
    if (Array.isArray(candidate.rows)) {
      return {
        columns: normalizeColumns(candidate.columns || payload.columns),
        rows: candidate.rows
      };
    }
    if (Array.isArray(candidate.data)) {
      return {
        columns: normalizeColumns(candidate.columns || payload.columns),
        rows: candidate.data
      };
    }
  }

  if (Array.isArray(payload.data)) {
    return { columns: normalizeColumns(payload.columns), rows: payload.data };
  }

  return null;
}

function collectColumnNames(columns, rows) {
  const names = new Set(columns || []);
  for (const row of Array.isArray(rows) ? rows : []) {
    if (row && typeof row === 'object' && !Array.isArray(row)) {
      for (const key of Object.keys(row)) names.add(key);
    }
  }
  return [...names];
}

function isEmptyDataset(parsed) {
  if (!parsed) return true;
  return !Array.isArray(parsed.rows) || parsed.rows.length === 0;
}

function responseHintsMissingParams(payload) {
  const text = JSON.stringify(payload || {}).toLowerCase();
  return /missing\s+param|required\s+param|frequency|from|to/.test(text) && /error|message/.test(text);
}

function buildFallbackUrl() {
  const now = new Date();
  const yyyyMm = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;

  const attempts = [
    new URLSearchParams({ frequency: 'Monthly', from: '2021-07', to: yyyyMm }),
    new URLSearchParams({ freq: 'monthly', from: '2021-07', to: yyyyMm })
  ];
  return attempts.map((params) => `${MAS_I6_API_URL}?${params.toString()}`);
}

function parseMasI6Data(payload, verifyMode = false) {
  const parsed = getRowsFromPayload(payload);
  if (!parsed || !Array.isArray(parsed.rows)) {
    throw new Error('Unexpected MAS API response shape');
  }

  const availableColumns = collectColumnNames(parsed.columns, parsed.rows);
  const hasRequiredColumns = [REQUIRED_TIME_COLUMN, GRANTED_HEADER, UTILISED_HEADER].every((name) => availableColumns.includes(name));
  if (!hasRequiredColumns) {
    const first30 = availableColumns.slice(0, 30).join(' | ') || '(none)';
    throw new Error(`Required MAS API columns missing. First 30 columns: ${first30}`);
  }

  const grantedMap = new Map();
  const utilisedMap = new Map();
  const newestPeriods = [];

  for (const row of parsed.rows) {
    const rowObj = Array.isArray(row)
      ? Object.fromEntries((parsed.columns || []).map((name, idx) => [name, row[idx]]))
      : row;

    const rawPeriod = rowObj?.[REQUIRED_TIME_COLUMN];
    const parsedPeriod = parseMasPeriod(rawPeriod);
    if (!parsedPeriod) {
      if (verifyMode) {
        console.log(`[verify-mas-api-i6] skipped_unparseable_period raw=${JSON.stringify(rawPeriod)}`);
      }
      continue;
    }

    const parseValue = (value) => {
      const num = Number(String(value ?? '').replace(/,/g, '').trim());
      return Number.isFinite(num) ? num : null;
    };

    const grantedValue = parseValue(rowObj?.[GRANTED_HEADER]);
    const utilisedValue = parseValue(rowObj?.[UTILISED_HEADER]);
    if (grantedValue == null || utilisedValue == null) continue;

    const pointGranted = { period: parsedPeriod.period, prelim: parsedPeriod.prelim, value: grantedValue };
    const pointUtilised = { period: parsedPeriod.period, prelim: parsedPeriod.prelim, value: utilisedValue };

    grantedMap.set(pointGranted.period, pointGranted);
    utilisedMap.set(pointUtilised.period, pointUtilised);
    newestPeriods.push({ raw: String(rawPeriod || ''), period: parsedPeriod.period, prelim: parsedPeriod.prelim });
  }

  const sortAsc = (a, b) => a.period.localeCompare(b.period);
  const grantedValues = [...grantedMap.values()].sort(sortAsc);
  const utilisedValues = [...utilisedMap.values()].sort(sortAsc);
  newestPeriods.sort((a, b) => b.period.localeCompare(a.period));

  return {
    availableColumns,
    extractedRowCount: parsed.rows.length,
    grantedValues,
    utilisedValues,
    newestPeriods: newestPeriods.slice(0, 3)
  };
}

async function fetchMasI6LoanLimits({ verifyMode = false } = {}) {
  const attemptUrls = [MAS_I6_API_URL, ...buildFallbackUrl()];
  let lastError;

  for (const url of attemptUrls) {
    try {
      const json = await fetchJsonWithRetry(url);
      const parsedShape = getRowsFromPayload(json);
      const shouldFallback = isEmptyDataset(parsedShape) || responseHintsMissingParams(json);
      if (shouldFallback && url === MAS_I6_API_URL) {
        if (verifyMode) console.log(`[verify-mas-api-i6] fallback_needed url=${url}`);
        continue;
      }

      const parsed = parseMasI6Data(json, verifyMode);
      if (verifyMode) {
        console.log(`[verify-mas-api-i6] url_used=${url}`);
        console.log(`[verify-mas-api-i6] row_count=${parsed.extractedRowCount}`);
        console.log(`[verify-mas-api-i6] required_columns_present=true`);
        console.log(`[verify-mas-api-i6] newest_3_periods=${parsed.newestPeriods.map((p) => `${p.raw} -> ${p.period}, prelim=${p.prelim}`).join(' | ')}`);
        const grantedLatest = parsed.grantedValues[parsed.grantedValues.length - 1];
        const utilisedLatest = parsed.utilisedValues[parsed.utilisedValues.length - 1];
        if (grantedLatest) {
          console.log(`[verify-mas-api-i6] granted_latest=${grantedLatest.period} value=${grantedLatest.value} prelim=${grantedLatest.prelim}`);
        }
        if (utilisedLatest) {
          console.log(`[verify-mas-api-i6] utilised_latest=${utilisedLatest.period} value=${utilisedLatest.value} prelim=${utilisedLatest.prelim}`);
        }
      }
      return { ...parsed, urlUsed: url };
    } catch (err) {
      lastError = err;
      if (verifyMode) {
        console.log(`[verify-mas-api-i6] attempt_failed url=${url} error=${err.message}`);
      }
    }
  }

  throw new Error(`MAS API I.6 fetch/parse failed: ${lastError?.message || 'unknown error'}`);
}

module.exports = {
  MAS_I6_API_URL,
  GRANTED_HEADER,
  UTILISED_HEADER,
  fetchJsonWithRetry,
  parseMasPeriod,
  fetchMasI6LoanLimits
};
