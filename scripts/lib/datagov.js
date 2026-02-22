const CKAN_BASE = 'https://data.gov.sg/api/action/datastore_search';
const CKAN_PAGE_LIMIT = 10000;
const DATAGOV_REQUEST_INTERVAL_MS = 1200;
const DEFAULT_HEADERS = {
  accept: 'application/json',
  'user-agent': 'macro-indicator-bot/1.0'
};

let queue = Promise.resolve();
let lastRequestAt = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterMs(value) {
  if (!value) return null;
  const n = Number(value);
  if (Number.isFinite(n)) return Math.max(0, n * 1000);
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return Math.max(0, d.getTime() - Date.now());
}

function buildHeaders(apiKey) {
  if (!apiKey) throw new Error('DATA_GOV_SG_API_KEY is required for data.gov.sg requests');
  return { ...DEFAULT_HEADERS, 'X-API-KEY': apiKey };
}

async function enqueueRequest(fn) {
  const run = async () => {
    const waitMs = Math.max(0, DATAGOV_REQUEST_INTERVAL_MS - (Date.now() - lastRequestAt));
    if (waitMs > 0) await sleep(waitMs);
    try {
      return await fn();
    } finally {
      lastRequestAt = Date.now();
    }
  };

  const scheduled = queue.then(run, run);
  queue = scheduled.catch(() => undefined);
  return scheduled;
}

async function fetchCkanPage({ resourceId, offset = 0, limit = CKAN_PAGE_LIMIT, apiKey, verifyMode = false }) {
  const params = new URLSearchParams({ resource_id: resourceId, offset: String(offset), limit: String(limit) });
  const url = `${CKAN_BASE}?${params.toString()}`;
  if (verifyMode) console.log(`[verify-url] ${resourceId} ${url}`);

  let tries429 = 0;
  let tries5xx = 0;

  while (true) {
    const res = await enqueueRequest(() => fetch(url, { headers: buildHeaders(apiKey) }));
    if (res.ok) return res.json();

    if (res.status === 429) {
      tries429 += 1;
      if (tries429 > 5) throw new Error(`HTTP 429 for ${resourceId} offset=${offset}`);
      const retryAfter = parseRetryAfterMs(res.headers.get('retry-after'));
      const delayMs = retryAfter != null ? retryAfter : Math.min(30000, 1000 * 2 ** (tries429 - 1));
      console.warn(`[retry] ${resourceId} offset=${offset}: HTTP 429 retry ${tries429}/5 in ${Math.round(delayMs)}ms`);
      await sleep(delayMs);
      continue;
    }

    if (res.status >= 500 && res.status <= 599) {
      tries5xx += 1;
      if (tries5xx > 3) throw new Error(`HTTP ${res.status} for ${resourceId} offset=${offset}`);
      const delayMs = Math.min(30000, 1000 * 2 ** (tries5xx - 1));
      console.warn(`[retry] ${resourceId} offset=${offset}: HTTP ${res.status} retry ${tries5xx}/3 in ${delayMs}ms`);
      await sleep(delayMs);
      continue;
    }

    throw new Error(`HTTP ${res.status} for ${resourceId} offset=${offset}`);
  }
}

async function fetchAllRecords(resourceId, apiKey, { verifyMode = false } = {}) {
  const records = [];
  let fields = [];
  let offset = 0;
  let total = 0;

  while (offset === 0 || offset < total) {
    const json = await fetchCkanPage({ resourceId, offset, limit: CKAN_PAGE_LIMIT, apiKey, verifyMode });
    if (!json?.success || !Array.isArray(json?.result?.records)) {
      throw new Error(`Unexpected CKAN payload for ${resourceId}`);
    }

    const page = json.result.records;
    if (!fields.length && Array.isArray(json.result.fields)) fields = json.result.fields;
    total = Number(json?.result?.total || 0);
    records.push(...page);

    if (!page.length) break;
    offset += CKAN_PAGE_LIMIT;
  }

  return { records, fields, total };
}

const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };

function parseMonthlyFieldId(fieldId) {
  const raw = String(fieldId || '').trim();
  let m = raw.match(/^(\d{4})([A-Za-z]{3})$/);
  if (m) {
    const month = MONTHS[m[2].toLowerCase()];
    if (!month) return null;
    return { periodType: 'M', year: Number(m[1]), month, sortKey: Number(m[1]) * 100 + month };
  }
  m = raw.match(/^(\d{4})\s+([A-Za-z]{3})(\b.*)?$/);
  if (m) {
    const month = MONTHS[m[2].toLowerCase()];
    if (!month) return null;
    return { periodType: 'M', year: Number(m[1]), month, sortKey: Number(m[1]) * 100 + month };
  }
  m = raw.match(/^(\d{4})-(\d{2})$/);
  if (m) {
    const month = Number(m[2]);
    if (month < 1 || month > 12) return null;
    return { periodType: 'M', year: Number(m[1]), month, sortKey: Number(m[1]) * 100 + month };
  }
  return null;
}

function parseQuarterlyFieldId(fieldId) {
  const raw = String(fieldId || '').trim();
  let m = raw.match(/^(\d{4})Q([1-4])$/i);
  if (m) return { periodType: 'Q', year: Number(m[1]), quarter: Number(m[2]), sortKey: Number(m[1]) * 10 + Number(m[2]) };
  m = raw.match(/^(\d{4})([1-4])Q$/i);
  if (m) return { periodType: 'Q', year: Number(m[1]), quarter: Number(m[2]), sortKey: Number(m[1]) * 10 + Number(m[2]) };
  m = raw.match(/^(\d{4})\s+Q([1-4])$/i);
  if (m) return { periodType: 'Q', year: Number(m[1]), quarter: Number(m[2]), sortKey: Number(m[1]) * 10 + Number(m[2]) };
  return null;
}

function detectTimeFields(fields) {
  if (!Array.isArray(fields)) return [];
  return fields
    .map((field) => {
      const id = field?.id;
      const monthly = parseMonthlyFieldId(id);
      if (monthly) return { id, ...monthly };
      const quarterly = parseQuarterlyFieldId(id);
      if (quarterly) return { id, ...quarterly };
      return null;
    })
    .filter(Boolean)
    .sort((a, b) => b.sortKey - a.sortKey);
}

function isMissingValue(value) {
  if (value == null) return true;
  const s = String(value).trim();
  return s === '' || /^na$/i.test(s) || /^n\.a\.$/i.test(s) || s === '-';
}

function toFiniteNumber(value) {
  if (isMissingValue(value)) return null;
  const num = Number(String(value).replace(/,/g, '').trim());
  return Number.isFinite(num) ? num : null;
}

function extractLatest(row, timeFields) {
  for (const field of timeFields) {
    const n = toFiniteNumber(row?.[field.id]);
    if (n != null) return { latest_period: field.id, latest_value: n };
  }
  return null;
}

function extractSeriesValues(row, timeFields) {
  return [...(timeFields || [])]
    .sort((a, b) => a.sortKey - b.sortKey)
    .map((field) => ({ period: field.id, value: toFiniteNumber(row?.[field.id]) }))
    .filter((point) => point.value != null);
}

function normalizeSeriesFieldName(name) {
  return String(name || '').toLowerCase().replace(/[\s_]+/g, '');
}

function detectSeriesFieldId(fields) {
  const ids = (fields || []).map((f) => f?.id).filter(Boolean);
  const exact = ids.find((id) => {
    const n = normalizeSeriesFieldName(id);
    return n === 'dataseries';
  });
  if (exact) return exact;
  return ids.find((id) => /series/i.test(id)) || null;
}

function sortRecordsStable(records) {
  return (records || [])
    .map((record, index) => ({ record, index }))
    .sort((a, b) => {
      const aId = Number(a.record?._id);
      const bId = Number(b.record?._id);
      const aFinite = Number.isFinite(aId);
      const bFinite = Number.isFinite(bId);
      if (aFinite && bFinite && aId !== bId) return aId - bId;
      if (aFinite && !bFinite) return -1;
      if (!aFinite && bFinite) return 1;
      return a.index - b.index;
    })
    .map((x) => x.record);
}

function findSeriesFieldId(fields) {
  return detectSeriesFieldId(fields);
}

function tokenize(text) {
  return String(text || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().split(/\s+/).filter(Boolean);
}

function findSeriesRow(records, seriesFieldId, seriesName) {
  const exact = records.find((record) => String(record?.[seriesFieldId] || '').trim() === seriesName);
  if (exact) return { row: exact, matchedSeriesName: seriesName, matchType: 'exact' };

  const requiredTokens = tokenize(seriesName);
  const candidates = records.filter((record) => {
    const label = String(record?.[seriesFieldId] || '').trim();
    const labelTokens = new Set(tokenize(label));
    return requiredTokens.every((token) => labelTokens.has(token));
  });

  if (candidates.length === 1) {
    return {
      row: candidates[0],
      matchedSeriesName: String(candidates[0]?.[seriesFieldId] || '').trim(),
      matchType: 'token'
    };
  }

  if (candidates.length > 1) return { error: `ambiguous fallback (${candidates.length} candidates)` };
  return { error: 'series not found' };
}

function numericAt(row, periodFieldId) {
  return toFiniteNumber(row?.[periodFieldId]);
}

function getLatestCommonPeriod(rows, timeFields) {
  for (const field of timeFields || []) {
    const allPresent = (rows || []).every((row) => numericAt(row, field.id) != null);
    if (allPresent) return field.id;
  }
  return null;
}

function approxEqual(a, b, tol = 1e-6, relTol = 1e-3) {
  if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
  const diff = Math.abs(a - b);
  const scale = Math.max(Math.abs(a), Math.abs(b), 1);
  return diff <= tol + relTol * scale;
}

function sumCheck(totalRow, componentRows, periodFieldId, absTol = 1, relTol = 1e-3) {
  const totalVal = numericAt(totalRow, periodFieldId);
  const components = (componentRows || []).map((row) => numericAt(row, periodFieldId));
  if (totalVal == null || components.some((v) => v == null)) {
    return { period: periodFieldId, total: totalVal, sum_components: null, diff: null, pass: false, missing: true };
  }
  const sumComponents = components.reduce((acc, value) => acc + value, 0);
  const diff = totalVal - sumComponents;
  return {
    period: periodFieldId,
    total: totalVal,
    sum_components: sumComponents,
    diff,
    pass: approxEqual(totalVal, sumComponents, absTol, relTol)
  };
}

module.exports = {
  DEFAULT_HEADERS,
  CKAN_PAGE_LIMIT,
  fetchAllRecords,
  detectTimeFields,
  parseQuarterlyFieldId,
  sortRecordsStable,
  findSeriesFieldId,
  detectSeriesFieldId,
  findSeriesRow,
  getLatestCommonPeriod,
  numericAt,
  approxEqual,
  sumCheck,
  extractLatest,
  extractSeriesValues,
  toFiniteNumber,
  isMissingValue
};
