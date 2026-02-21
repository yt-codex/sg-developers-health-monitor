const fs = require('fs/promises');
const path = require('path');

const MAS_I6_API_URL = 'https://www.mas.gov.sg/api/v1/MAS/chart/table_i_6_commercial_banks_loan_limits_granted_to_non_bank_customers_by_industry';

const REQUIRED_FIELDS = ['year', 'month', 'bc_lmtgrtd', 'bc_utl', 'p_ind'];

const DEFAULT_HEADERS = {
  accept: 'application/json,text/plain,*/*',
  'accept-language': 'en-US,en;q=0.9',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  referer: 'https://www.mas.gov.sg/statistics/monthly-statistical-bulletin/i-6-commercial-banks-loan-limits-granted-to-non-bank-customers-by-industry',
  'cache-control': 'no-cache',
  pragma: 'no-cache'
};

function makeRetryableError(message) {
  const err = new Error(message);
  err.retryable = true;
  return err;
}

async function writeNonJsonArtifact(rawText) {
  const outDir = path.join(process.cwd(), 'artifacts');
  await fs.mkdir(outDir, { recursive: true });
  const outFile = path.join(outDir, 'mas_i6_nonjson.html');
  await fs.writeFile(outFile, rawText, 'utf8');
}

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
    maxRetries = 5,
    verifyMode = false
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
      const finalUrl = res.url || url;
      const rawText = await res.text();
      const bodyTrimmed = rawText.trim();
      const bodyStartsWithHtml = /^<!doctype|^<html/i.test(bodyTrimmed);
      const contentTypeIsJson = /application\/json/i.test(contentType);
      const isNonJson = !contentTypeIsJson || bodyStartsWithHtml;

      console.log(`[mas-i6-fetch] status=${status} content_type=${contentType || '(none)'} response_url=${finalUrl}`);
      if (isNonJson) {
        console.log(`[mas-i6-fetch] body_head_nonjson=${bodyTrimmed.slice(0, 120)}`);
      }

      if (!res.ok) {
        const retriable = status === 429 || (status >= 500 && status <= 599);
        const message = `fetch failed (HTTP status ${status})`;
        if (!retriable || attempt >= maxRetries) {
          throw new Error(message);
        }
        lastError = new Error(message);
      } else {
        if (isNonJson) {
          if (verifyMode) {
            await writeNonJsonArtifact(rawText);
          }
          throw makeRetryableError('received non-JSON response from MAS I.6 endpoint');
        }
        try {
          const json = JSON.parse(rawText);
          return { json, status, contentType, responseUrl: finalUrl };
        } catch (err) {
          throw makeRetryableError(`JSON parse failed: ${err.message}`);
        }
      }
    } catch (err) {
      lastError = err;
      if (attempt >= maxRetries) break;
      if (!err?.retryable) break;
    }

    const backoffMs = Math.min(16_000, 500 * 2 ** (attempt - 1));
    await delay(backoffMs);
  }

  throw new Error(`MAS API request failed after ${maxRetries} attempts: ${lastError?.message || 'unknown error'}`);
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
    console.log(`[verify-mas-api-i6] json.name=${json?.name ?? '(missing)'}`);
    const jsonElements = Array.isArray(json?.elements) ? json.elements : [];
    console.log(`[verify-mas-api-i6] json.elements.length=${jsonElements.length}`);
    if (jsonElements.length > 0) {
      console.log(`[verify-mas-api-i6] json.elements[0]=${JSON.stringify(jsonElements[0])}`);
    }
    console.log(`[verify-mas-api-i6] required_fields_present=${hasFields.join(',') || '(none)'}`);
    return;
  }

  console.log(`[mas-i6] status=${status} rows=${rowCount} hasFields=${hasFields.join(',')}`);
}

async function fetchMasI6LoanLimits({ verifyMode = false } = {}) {
  const attemptUrls = [
    MAS_I6_API_URL,
    `${MAS_I6_API_URL}?t=${Date.now()}`
  ];
  let lastError;

  for (const url of attemptUrls) {
    try {
      const { json, status, contentType } = await fetchJsonWithRetry(url, { verifyMode });
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
