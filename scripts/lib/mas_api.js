const MAS_I6_API_URL = 'https://www.mas.gov.sg/api/v1/MAS/chart/table_i_6_commercial_banks_loan_limits_granted_to_non_bank_customers_by_industry';

const REQUIRED_FIELDS = ['year', 'month', 'bc_lmtgrtd', 'bc_utl', 'p_ind'];

const DEFAULT_HEADERS = {
  accept: 'application/json',
  'user-agent': 'macro-indicator-bot/1.0'
};

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function summarizeFieldsPresence(row) {
  if (!row || typeof row !== 'object') return [];
  return REQUIRED_FIELDS.filter((field) => Object.prototype.hasOwnProperty.call(row, field));
}

function parseUpdateDate(value) {
  if (value == null || value === '') return null;
  const date = new Date(value);
  const ts = date.getTime();
  return Number.isFinite(ts) ? ts : null;
}

function shouldReplaceDuplicate(prevRow, nextRow) {
  const prevTs = parseUpdateDate(prevRow?.update_date);
  const nextTs = parseUpdateDate(nextRow?.update_date);

  if (prevTs != null || nextTs != null) {
    if (prevTs == null) return true;
    if (nextTs == null) return false;
    if (nextTs > prevTs) return true;
    if (nextTs < prevTs) return false;
  }

  return true;
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

      const status = res.status;
      const contentType = res.headers.get('content-type') || '';
      const rawText = await res.text();

      if (!res.ok) {
        const retriable = status === 429 || (status >= 500 && status <= 599);
        const message = `fetch failed (HTTP status ${status})`;
        if (!retriable || attempt >= maxRetries) {
          throw new Error(message);
        }
        lastError = new Error(message);
      } else {
        try {
          const json = JSON.parse(rawText);
          return { json, status, contentType };
        } catch (err) {
          throw new Error(`JSON parse failed: ${err.message}`);
        }
      }
    } catch (err) {
      lastError = err;
      if (attempt >= maxRetries) break;
    }

    const backoffMs = Math.min(16_000, 500 * 2 ** (attempt - 1));
    await delay(backoffMs);
  }

  throw new Error(`MAS API request failed after ${maxRetries} attempts: ${lastError?.message || 'unknown error'}`);
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

function locateRowsArray(payload) {
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.result)) return payload.result;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload)) return payload;
  return null;
}

function parseMasI6Data(payload) {
  const rows = locateRowsArray(payload);
  if (!rows) throw new Error('could not locate rows array');
  if (!rows.length) throw new Error('zero rows returned');

  const firstRow = rows[0];
  const missingFields = REQUIRED_FIELDS.filter((field) => !Object.prototype.hasOwnProperty.call(firstRow || {}, field));
  if (missingFields.length) {
    throw new Error(`missing required fields: ${missingFields.join(',')}`);
  }

  const byPeriod = new Map();

  for (const row of rows) {
    const year = Number(row?.year);
    const month = Number(row?.month);
    const grantedValue = Number(row?.bc_lmtgrtd);
    const utilisedValue = Number(row?.bc_utl);

    if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) continue;
    if (!Number.isFinite(grantedValue) || !Number.isFinite(utilisedValue)) continue;

    const period = `${year}-${String(month).padStart(2, '0')}`;
    const point = {
      period,
      prelim: row?.p_ind === 'P',
      grantedValue,
      utilisedValue,
      updateDate: row?.update_date,
      _rawRow: row
    };

    const prev = byPeriod.get(period);
    if (!prev || shouldReplaceDuplicate(prev._rawRow, row)) {
      byPeriod.set(period, point);
    }
  }

  const sortAsc = (a, b) => a.period.localeCompare(b.period);
  const deduped = [...byPeriod.values()].sort(sortAsc);

  const grantedValues = deduped.map((row) => ({ period: row.period, prelim: row.prelim, value: row.grantedValue }));
  const utilisedValues = deduped.map((row) => ({ period: row.period, prelim: row.prelim, value: row.utilisedValue }));

  return {
    rows,
    extractedRowCount: rows.length,
    grantedValues,
    utilisedValues
  };
}

function logPayloadDiagnostics({ verifyMode, status, contentType, json, rows }) {
  const jsonKeys = json && typeof json === 'object' && !Array.isArray(json) ? Object.keys(json) : [];
  const rowCount = Array.isArray(rows) ? rows.length : 0;
  const firstRow = rowCount > 0 ? rows[0] : null;
  const hasFields = summarizeFieldsPresence(firstRow);

  if (verifyMode) {
    console.log(`[verify-mas-api-i6] status=${status} content_type=${contentType || '(none)'}`);
    console.log(`[verify-mas-api-i6] top_level_keys=${jsonKeys.join(',') || '(array_or_none)'}`);
    console.log(`[verify-mas-api-i6] rowCount=${rowCount}`);
    if (rowCount > 0) {
      console.log(`[verify-mas-api-i6] row[0]=${JSON.stringify(rows[0], null, 0)}`);
    }
    if (rowCount > 1) {
      console.log(`[verify-mas-api-i6] row[1]=${JSON.stringify(rows[1], null, 0)}`);
    }
    console.log(`[verify-mas-api-i6] required_fields_present=${hasFields.join(',') || '(none)'}`);
    return;
  }

  console.log(`[mas-i6] status=${status} rows=${rowCount} hasFields=${hasFields.join(',')}`);
}

async function fetchMasI6LoanLimits({ verifyMode = false } = {}) {
  const attemptUrls = [MAS_I6_API_URL, ...buildFallbackUrl()];
  let lastError;

  for (const url of attemptUrls) {
    try {
      const { json, status, contentType } = await fetchJsonWithRetry(url);
      const rows = locateRowsArray(json);
      logPayloadDiagnostics({ verifyMode, status, contentType, json, rows });

      const parsed = parseMasI6Data(json);

      if (verifyMode) {
        const latestThree = parsed.grantedValues.slice(-3);
        const utilisedLatestThree = parsed.utilisedValues.slice(-3);
        const lastSixPrelimCount = parsed.grantedValues.slice(-6).filter((row) => row.prelim).length;
        console.log(`[verify-mas-api-i6] latest3_bc_lmtgrtd=${latestThree.map((row) => `${row.period}:${row.value}`).join(' | ')}`);
        console.log(`[verify-mas-api-i6] latest3_bc_utl=${utilisedLatestThree.map((row) => `${row.period}:${row.value}`).join(' | ')}`);
        console.log(`[verify-mas-api-i6] prelim_count_last6=${lastSixPrelimCount}`);
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
  fetchJsonWithRetry,
  fetchMasI6LoanLimits
};
