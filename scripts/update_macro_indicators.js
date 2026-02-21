#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');

const VERIFY_MODE = process.argv.includes('--verify_sources');
const DATA_FILE = path.join(process.cwd(), 'data', 'macro_indicators.json');
const CKAN_BASE = 'https://data.gov.sg/api/action/datastore_search';
const MAS_SORA_URL = 'https://eservices.mas.gov.sg/statistics/dir/domesticinterestrates.aspx';
const MAS_MSB_I6_CSV_URL = 'https://www.mas.gov.sg/-/media/mas-media-library/statistics/monthly-statistical-bulletin/msb-historical/money-and-banking--i6--yearly.csv';
const DEFAULT_JSON_HEADERS = {
  accept: 'application/json',
  'user-agent': 'macro-indicator-bot/1.0'
};
const DATAGOV_REQUEST_INTERVAL_MS = 1200;

const DATASET_IDS = [
  'd_5fe5a4bb4a1ecc4d8a56a095832e2b24',
  'd_29f7b431ad79f61f19a731a6a86b0247',
  'd_ba3c493ad160125ce347d5572712f14f',
  'd_f9fc9b5420d96bcab45bc31eeb8ae3c3',
  'd_055b6549444dedb341c50805d9682a41',
  'd_e47c0f0674b46981c4994d5257de5be4',
  'd_4dca06508cd9d0a8076153443c17ea5f',
  'd_e9cc9d297b1cf8024cf99db4b12505cc',
  'd_df200b7f89f94e52964ff45cd7878a30',
  'd_af0415517a3a3a94b3b74039934ef976'
];

const REQUIRED_DATASETS = {
  d_5fe5a4bb4a1ecc4d8a56a095832e2b24: [
    {
      key: 'sgs_10y',
      target: 'Government Securities - 10-Year Bond Yield',
      aliases: ['SGS 10-Year Bond Yield']
    },
    {
      key: 'sgs_2y',
      target: 'Government Securities - 2-Year Bond Yield',
      aliases: ['SGS 2-Year Bond Yield']
    }
  ],
  d_f9fc9b5420d96bcab45bc31eeb8ae3c3: [
    { key: 'unit_labour_cost_construction', target: 'Unit Labour Cost Of Construction' }
  ],
  d_af0415517a3a3a94b3b74039934ef976: [
    { key: 'loan_bc_total', target: 'Loans To Businesses - Building And Construction - Total' },
    { key: 'loan_bc_construction', target: 'Loans To Businesses - Building And Construction - Construction' },
    { key: 'loan_bc_real_property', target: 'Loans To Businesses - Building And Construction - Real Property And Development Of Land' }
  ]
};

const MAX_HTTP_RETRIES = 5;
const TIME_FIELD_PATTERNS = [
  /^\d{4}\s[A-Za-z]{3}.*$/,
  /^\d{4}-\d{2}$/,
  /^\d{4}\s?Q[1-4]$/i
];

let dataGovQueue = Promise.resolve();
let lastDataGovRequestAt = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterSeconds(retryAfterValue) {
  if (!retryAfterValue) return null;
  const asNumber = Number(retryAfterValue);
  if (Number.isFinite(asNumber)) return Math.max(0, asNumber);

  const asDate = new Date(retryAfterValue);
  if (Number.isNaN(asDate.getTime())) return null;
  return Math.max(0, Math.ceil((asDate.getTime() - Date.now()) / 1000));
}

async function fetchWithRetry(url, options = {}, { label = url, retries = MAX_HTTP_RETRIES } = {}) {
  let lastError = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const res = await fetch(url, options);
      if (res.ok) return res;

      const shouldRetry = res.status === 429 || res.status >= 500;
      if (!shouldRetry || attempt === retries) {
        throw new Error(`HTTP ${res.status} for ${label}`);
      }

      const retryAfterSeconds = parseRetryAfterSeconds(res.headers.get('retry-after'));
      const backoffMs = retryAfterSeconds != null
        ? retryAfterSeconds * 1000
        : (res.status === 429
          ? Math.min(8000, 1000 * 2 ** (attempt - 1))
          : Math.min(30_000, 1000 * 2 ** (attempt - 1)));

      console.warn(
        `[retry] ${label}: HTTP ${res.status} (attempt ${attempt}/${retries}), waiting ${backoffMs}ms before retry`
      );
      await sleep(backoffMs);
    } catch (err) {
      lastError = err;
      if (attempt === retries) break;
      const backoffMs = Math.min(30_000, 1000 * 2 ** (attempt - 1));
      console.warn(
        `[retry] ${label}: ${err.message} (attempt ${attempt}/${retries}), waiting ${backoffMs}ms before retry`
      );
      await sleep(backoffMs);
    }
  }

  throw lastError || new Error(`Failed to fetch ${label}`);
}

async function fetchJsonWithRetry(url, options = {}, meta = {}) {
  const res = await fetchWithRetry(
    url,
    {
      ...options,
      headers: {
        ...DEFAULT_JSON_HEADERS,
        ...(options.headers || {})
      }
    },
    meta
  );
  return res.json();
}

async function enqueueDataGovRequest(task) {
  const run = async () => {
    const waitMs = Math.max(0, DATAGOV_REQUEST_INTERVAL_MS - (Date.now() - lastDataGovRequestAt));
    if (waitMs > 0) await sleep(waitMs);
    const result = await task();
    lastDataGovRequestAt = Date.now();
    return result;
  };

  const scheduled = dataGovQueue.then(run, run);
  dataGovQueue = scheduled.catch(() => undefined);
  return scheduled;
}

function discoverFieldIdsFromCkanFields(fields) {
  const normalizedFields = Array.isArray(fields) ? fields : [];
  const seriesField = normalizedFields.find((field) => {
    const id = String(field?.id || '').trim();
    const lower = id.toLowerCase();
    return lower === 'data series' || (lower.includes('data') && lower.includes('series'));
  });
  const timeFieldIds = normalizedFields
    .map((field) => String(field?.id || '').trim())
    .filter((id) => id && TIME_FIELD_PATTERNS.some((pattern) => pattern.test(id)));
  return {
    seriesFieldId: seriesField?.id || null,
    timeFieldIds
  };
}

function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

function scoreSimilarity(a, b) {
  const aTokens = new Set(tokenize(a));
  const bTokens = tokenize(b);
  let score = 0;
  for (const token of bTokens) {
    if (aTokens.has(token)) score += 3;
    else if ([...aTokens].some((at) => at.includes(token) || token.includes(at))) score += 1;
  }
  return score;
}

function toNumber(value) {
  if (value == null || value === '') return null;
  const cleaned = String(value).replace(/,/g, '').trim();
  if (!cleaned || /^na$/i.test(cleaned) || cleaned === '-') return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

function parsePeriodToDate(period) {
  const p = String(period).trim();
  let m = p.match(/^(\d{4})\s*Q([1-4])$/i);
  if (m) {
    const year = Number(m[1]);
    const quarter = Number(m[2]);
    return new Date(Date.UTC(year, quarter * 3 - 1, 1));
  }
  m = p.match(/^(\d{4})[-/](\d{2})([-/](\d{2}))?$/);
  if (m) return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[4] || '1')));
  m = p.match(/^([A-Za-z]{3,})\s+(\d{4})$/);
  if (m) {
    const month = new Date(`${m[1].slice(0, 3)} 1, 2000`).getMonth();
    if (!Number.isNaN(month)) return new Date(Date.UTC(Number(m[2]), month, 1));
  }
  m = p.match(/^(\d{4})\s+([A-Za-z]{3,})$/);
  if (m) {
    const month = new Date(`${m[2].slice(0, 3)} 1, 2000`).getMonth();
    if (!Number.isNaN(month)) return new Date(Date.UTC(Number(m[1]), month, 1));
  }
  return null;
}

function detectTimeColumns(record) {
  return Object.keys(record)
    .map((key) => ({ key, dt: parsePeriodToDate(key) }))
    .filter((x) => x.dt)
    .sort((a, b) => a.dt - b.dt);
}

function extractLatestFromRecord(record) {
  const timeColumns = detectTimeColumns(record);
  for (let i = timeColumns.length - 1; i >= 0; i -= 1) {
    const period = timeColumns[i].key;
    const value = toNumber(record[period]);
    if (value != null) return { period, value };
  }
  return null;
}


function extractLatestFromRecordUsingFields(record, timeFieldIds) {
  const candidates = (Array.isArray(timeFieldIds) ? timeFieldIds : [])
    .map((period) => ({ period, value: toNumber(record?.[period]), dt: parsePeriodToDate(period) }))
    .filter((x) => x.dt)
    .sort((a, b) => a.dt - b.dt);

  for (let i = candidates.length - 1; i >= 0; i -= 1) {
    if (candidates[i].value != null) {
      return { period: candidates[i].period, value: candidates[i].value };
    }
  }
  return null;
}

function detectSeriesField(records) {
  const keys = Object.keys(records[0] || {});
  const preferred = keys.find((k) => /data\s*series/i.test(k));
  if (preferred) return preferred;
  return keys.find((k) => /series|description|category|indicator/i.test(k));
}

async function fetchDataset(datasetId, limit = 5000) {
  const url = `${CKAN_BASE}?resource_id=${datasetId}&limit=${limit}`;
  if (VERIFY_MODE) console.log(`[verify-url] ${datasetId} ${url}`);
  const json = await enqueueDataGovRequest(() => fetchJsonWithRetry(url, {}, { label: datasetId }));
  if (json?.success === true && Array.isArray(json?.result?.records)) {
    return json.result.records;
  }
  if (typeof json?.code === 'number' && Array.isArray(json?.data?.rows)) {
    throw new Error('wrong endpoint/schema');
  }
  if (!json?.success || !Array.isArray(json?.result?.records)) {
    throw new Error(`Unexpected CKAN payload for ${datasetId}`);
  }
}

async function fetchDatasetByQuery(datasetId, query, limit = 200) {
  const effectiveLimit = Math.max(200, limit);
  const params = new URLSearchParams({ resource_id: datasetId, limit: String(effectiveLimit), q: query });
  const url = `${CKAN_BASE}?${params.toString()}`;
  if (VERIFY_MODE) console.log(`[verify-url] ${datasetId} ${url}`);
  const json = await enqueueDataGovRequest(() => fetchJsonWithRetry(url, {}, { label: `${datasetId} (q=${query})` }));
  if (json?.success === true && Array.isArray(json?.result?.records)) {
    return json.result.records;
  }
  if (typeof json?.code === 'number' && Array.isArray(json?.data?.rows)) {
    throw new Error('wrong endpoint/schema');
  }
  if (!json?.success || !Array.isArray(json?.result?.records)) {
    throw new Error(`Unexpected CKAN payload for ${datasetId} (q=${query})`);
  }
}

async function fetchDatasetWithFilters(datasetId, filters, limit = 100) {
  const params = new URLSearchParams({
    resource_id: datasetId,
    filters: JSON.stringify(filters),
    limit: String(limit)
  });
  const url = `${CKAN_BASE}?${params.toString()}`;
  if (VERIFY_MODE) console.log(`[verify-url] ${datasetId} ${url}`);
  const json = await enqueueDataGovRequest(
    () => fetchJsonWithRetry(url, {}, { label: `${datasetId} (filters=${JSON.stringify(filters)})` })
  );
  if (json?.success === true && Array.isArray(json?.result?.records)) {
    return json.result.records;
  }
  if (typeof json?.code === 'number' && Array.isArray(json?.data?.rows)) {
    throw new Error('wrong endpoint/schema');
  }
  if (!json?.success || !Array.isArray(json?.result?.records)) {
    throw new Error(`Unexpected CKAN payload for ${datasetId} with filters`);
  }
}

async function discoverDatasetFields(datasetId) {
  const params = new URLSearchParams({ resource_id: datasetId, limit: '1' });
  const url = `${CKAN_BASE}?${params.toString()}`;
  if (VERIFY_MODE) console.log(`[verify-url] ${datasetId} ${url}`);
  const json = await enqueueDataGovRequest(() => fetchJsonWithRetry(url, {}, { label: `${datasetId} (limit=1)` }));
  if (typeof json?.code === 'number' && Array.isArray(json?.data?.rows)) {
    throw new Error('wrong endpoint/schema');
  }
  if (!json?.success || !json?.result) {
    throw new Error(`Unexpected CKAN payload for ${datasetId} (limit=1)`);
  }

  const discovered = discoverFieldIdsFromCkanFields(json.result.fields || []);
  const fallbackField = detectSeriesField(json.result.records || []);
  const seriesFieldId = discovered.seriesFieldId || fallbackField || null;
  const sampleTimeFields = discovered.timeFieldIds.slice(0, 3);
  console.log(
    `[ckan-discovery] ${datasetId} seriesFieldId=${seriesFieldId || 'n/a'} timeFieldIds=${sampleTimeFields.join(', ') || 'n/a'}`
  );
  if (!seriesFieldId) {
    throw new Error(`Unable to detect Data Series field for ${datasetId} from limit=1 sample`);
  }
  return { seriesFieldId, timeFieldIds: discovered.timeFieldIds };
}

async function fetchSeriesRowByExactMatch(datasetId, requirement, discovery, filteredCallCache) {
  const seriesField = discovery.seriesFieldId;
  const seriesName = requirement.target;
  const filterKey = `${datasetId}::${seriesField}::${seriesName}`;
  if (!filteredCallCache.has(filterKey)) {
    filteredCallCache.set(filterKey, fetchDatasetWithFilters(datasetId, { [seriesField]: seriesName }, 100));
  }
  const records = await filteredCallCache.get(filterKey);
  if (!records.length) {
    throw new Error(`0 records (filter mismatch) seriesFieldId=${seriesField} seriesName=${seriesName}`);
  }
  const exact = records.find((r) => String(r[seriesField] || '').trim() === seriesName);
  if (exact) return { row: exact, seriesField, matchedName: seriesName };
  throw new Error(`Missing required series in dataset ${datasetId}: ${requirement.target}`);
}

function findSeriesOrThrow(records, seriesField, requirement, datasetId) {
  const seriesNames = [...new Set(records.map((r) => String(r[seriesField] || '').trim()).filter(Boolean))];
  const acceptedTargets = [requirement.target, ...(requirement.aliases || [])];
  let found = records.find((r) => acceptedTargets.includes(String(r[seriesField] || '').trim()));
  if (!found) {
    const ranked = seriesNames
      .map((name) => ({ name, score: scoreSimilarity(name, requirement.target) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 20)
      .map((x) => x.name);
    throw new Error(
      [
        `Missing required series in dataset ${datasetId}`,
        `Required key: ${requirement.key}`,
        `Required exact target: ${requirement.target}`,
        ...(requirement.aliases?.length ? [`Accepted aliases: ${requirement.aliases.join(' | ')}`] : []),
        `Closest 20 Data Series names:`,
        ...ranked.map((n) => `  - ${n}`)
      ].join('\n')
    );
  }
  return found;
}

function pickByKeywords(records, seriesField, keywords, limit = 2) {
  return records
    .map((r) => ({
      row: r,
      score: keywords.reduce((acc, kw) => acc + (String(r[seriesField] || '').toLowerCase().includes(kw) ? 1 : 0), 0)
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((x) => x.row);
}

function addSeries(series, key, freq, latest, units, metadata = {}) {
  if (!latest) throw new Error(`Unable to extract latest non-null value for ${key}`);
  series[key] = {
    freq,
    latest_period: latest.period,
    latest_value: latest.value,
    units,
    ...metadata
  };
  console.log(`- ${key}: ${latest.period} = ${latest.value}`);
}

function summarizeError(err) {
  const message = String(err?.message || err || 'unknown error').split('\n')[0].trim();
  const httpMatch = message.match(/HTTP\s+(\d{3})/i);
  if (httpMatch) return `HTTP ${httpMatch[1]} ${message.replace(/.*HTTP\s+\d{3}\s*/i, '').trim()}`.trim();
  return message;
}

function recordOk(results, payload) {
  const result = { ...payload, status: 'ok' };
  results.push(result);
  console.log(`[OK] ${result.key} ${result.latest_period || '-'} ${result.latest_value ?? '-'}`);
  return result;
}

function recordFail(results, payload) {
  const result = { ...payload, status: 'failed' };
  results.push(result);
  console.log(`[FAIL] ${result.key} ${result.source} ${result.dataset_ref} ${result.error_summary}`);
  return result;
}

function printRunSummary(results) {
  const ok = results.filter((x) => x.status === 'ok');
  const failed = results.filter((x) => x.status === 'failed');
  console.log('--- Macro indicator extraction summary ---');
  console.log(`OK: ${ok.length}`);
  console.log(`FAILED: ${failed.length}`);
  for (const item of failed) {
    console.log(`${item.key} | ${item.source} | ${item.dataset_ref} | ${item.error_summary}`);
  }
  return { ok_count: ok.length, failed_count: failed.length, failed_items: failed };
}

function decodeHtmlEntities(text) {
  return String(text || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#39;/gi, "'")
    .replace(/&quot;/gi, '"');
}

function stripHtmlTags(text) {
  return decodeHtmlEntities(String(text || '').replace(/<[^>]*>/g, ' ')).replace(/\s+/g, ' ').trim();
}

function parseSimpleHtmlTableRows(html) {
  const rows = [];
  const rowMatches = String(html || '').match(/<tr\b[^>]*>[\s\S]*?<\/tr>/gi) || [];
  for (const rowHtml of rowMatches) {
    const cellMatches = rowHtml.match(/<t[dh]\b[^>]*>[\s\S]*?<\/t[dh]>/gi) || [];
    const cells = cellMatches.map((cellHtml) => stripHtmlTags(cellHtml));
    if (cells.length) rows.push(cells);
  }
  return rows;
}

function parseSelectDefinitions(html) {
  const selects = [];
  const selectRegex = /<select\b([^>]*)>([\s\S]*?)<\/select>/gi;
  let selectMatch;
  while ((selectMatch = selectRegex.exec(html)) !== null) {
    const attrs = selectMatch[1] || '';
    const body = selectMatch[2] || '';
    const name = ((attrs.match(/\bname\s*=\s*"([^"]*)"/i) || [])[1]
      || (attrs.match(/\bname\s*=\s*'([^']*)'/i) || [])[1]
      || '').trim();
    const id = ((attrs.match(/\bid\s*=\s*"([^"]*)"/i) || [])[1]
      || (attrs.match(/\bid\s*=\s*'([^']*)'/i) || [])[1]
      || '').trim();

    const options = [];
    const optionRegex = /<option\b([^>]*)>([\s\S]*?)<\/option>/gi;
    let optionMatch;
    while ((optionMatch = optionRegex.exec(body)) !== null) {
      const optionAttrs = optionMatch[1] || '';
      const text = stripHtmlTags(optionMatch[2] || '');
      const value = ((optionAttrs.match(/\bvalue\s*=\s*"([^"]*)"/i) || [])[1]
        || (optionAttrs.match(/\bvalue\s*=\s*'([^']*)'/i) || [])[1]
        || text).trim();
      const selected = /\bselected\b/i.test(optionAttrs);
      options.push({ value, text, selected });
    }

    selects.push({ name, id, options });
  }
  return selects;
}

function parseCsvLine(line) {
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
      continue;
    }
    if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
}

function parseSimpleCsv(text) {
  const lines = String(text || '').replace(/\uFEFF/g, '').split(/\r?\n/).filter((line) => line.trim() !== '');
  if (!lines.length) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((header, i) => {
      row[header] = cols[i] == null ? '' : cols[i];
    });
    return row;
  });
}

function parseSoraRows(html) {
  const rows = [];
  const tableRows = parseSimpleHtmlTableRows(html);
  tableRows.forEach((cells) => {
    if (cells.length < 2) return;
    const dateText = cells.find((c) => /\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}/.test(c) || /\d{4}-\d{2}-\d{2}/.test(c));
    const valueText = cells.find((c) => /^-?[\d,.]+$/.test(c.replace('%', '').trim()));
    if (!dateText || !valueText) return;
    const normalizedDate = dateText.includes('-')
      ? dateText
      : (() => {
          const parts = dateText.split(/[\/]/).map((p) => p.trim());
          if (parts.length !== 3) return null;
          const [d, m, y] = parts;
          const fullYear = y.length === 2 ? `20${y}` : y;
          return `${fullYear}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
        })();
    if (!normalizedDate) return;
    const num = toNumber(valueText.replace('%', ''));
    if (num == null) return;
    rows.push({ date: normalizedDate, value: num });
  });
  const dedup = new Map(rows.map((r) => [r.date, r]));
  return [...dedup.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function buildSoraPostPayload(html) {
  const payload = {};
  const hiddenInputRegex = /<input\b([^>]*\btype\s*=\s*["']hidden["'][^>]*)>/gi;
  let hiddenMatch;
  while ((hiddenMatch = hiddenInputRegex.exec(html)) !== null) {
    const attrs = hiddenMatch[1] || '';
    const name = (attrs.match(/\bname\s*=\s*"([^"]*)"/i) || attrs.match(/\bname\s*=\s*'([^']*)'/i) || [])[1];
    const value = (attrs.match(/\bvalue\s*=\s*"([^"]*)"/i) || attrs.match(/\bvalue\s*=\s*'([^']*)'/i) || [])[1] || '';
    if (name) payload[name] = decodeHtmlEntities(value);
  }

  const selects = parseSelectDefinitions(html);

  const now = new Date();
  const start = new Date(now);
  start.setDate(start.getDate() - 14);

  const setSelectValue = (regex, desiredTextRegex, fallbackValue) => {
    const el = selects.find((s) => regex.test(s.name || s.id || ''));
    if (!el || !el.name) return;
    let chosen = fallbackValue;
    if (desiredTextRegex) {
      const opt = el.options.find((o) => desiredTextRegex.test(o.text));
      if (opt) chosen = opt.value;
    }
    if (chosen == null) {
      const selectedOption = el.options.find((o) => o.selected) || el.options[0];
      if (selectedOption) chosen = selectedOption.value;
    }
    if (chosen != null) payload[el.name] = chosen;
  };

  const startYearSelector = /start.*year|from.*year/i;
  const endYearSelector = /end.*year|to.*year/i;
  const startMonthSelector = /start.*month|from.*month/i;
  const endMonthSelector = /end.*month|to.*month/i;
  const frequencySelector = /freq|frequency/i;
  const seriesSelector = /series|rate|stat/i;

  setSelectValue(startYearSelector, null, String(start.getUTCFullYear()));
  setSelectValue(endYearSelector, null, String(now.getUTCFullYear()));
  setSelectValue(startMonthSelector, null, String(start.getUTCMonth() + 1));
  setSelectValue(endMonthSelector, null, String(now.getUTCMonth() + 1));
  setSelectValue(frequencySelector, /daily/i, null);
  setSelectValue(seriesSelector, /sora/i, null);

  const hasSelector = (regex) =>
    selects.some((s) => regex.test(s.name || s.id || ''));
  const expectedControls = [
    ['start year', startYearSelector],
    ['end year', endYearSelector],
    ['start month', startMonthSelector],
    ['end month', endMonthSelector],
    ['frequency', frequencySelector],
    ['series', seriesSelector]
  ];
  const missingControls = expectedControls.filter(([, regex]) => !hasSelector(regex)).map(([label]) => label);
  if (missingControls.length) {
    throw new Error(`SORA page missing expected selector controls: ${missingControls.join(', ')}`);
  }

  const submitInputMatch = html.match(/<input\b([^>]*\btype\s*=\s*["']submit["'][^>]*)>/i);
  const submitButtonMatch = html.match(/<button\b([^>]*\btype\s*=\s*["']submit["'][^>]*)>/i);
  const submitAttrs = (submitInputMatch && submitInputMatch[1]) || (submitButtonMatch && submitButtonMatch[1]) || '';
  const submitName = (submitAttrs.match(/\bname\s*=\s*"([^"]*)"/i) || submitAttrs.match(/\bname\s*=\s*'([^']*)'/i) || [])[1];
  const submitValue = (submitAttrs.match(/\bvalue\s*=\s*"([^"]*)"/i) || submitAttrs.match(/\bvalue\s*=\s*'([^']*)'/i) || [])[1] || 'Submit';
  if (submitName) payload[submitName] = decodeHtmlEntities(submitValue);

  return payload;
}

async function fetchSoraSeries() {
  const getRes = await fetch(MAS_SORA_URL, {
    headers: {
      ...DEFAULT_JSON_HEADERS
    }
  });
  if (!getRes.ok) throw new Error(`SORA GET failed: HTTP ${getRes.status}`);
  const landingHtml = await getRes.text();
  if (!/__VIEWSTATE/i.test(landingHtml) || !/__EVENTVALIDATION/i.test(landingHtml)) {
    throw new Error('SORA page missing expected ASP.NET hidden fields (__VIEWSTATE / __EVENTVALIDATION).');
  }
  if (!/sora/i.test(landingHtml)) {
    throw new Error('SORA page does not contain expected SORA label.');
  }

  const payload = buildSoraPostPayload(landingHtml);
  const postRes = await fetch(MAS_SORA_URL, {
    method: 'POST',
    headers: {
      ...DEFAULT_JSON_HEADERS,
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(payload)
  });
  if (!postRes.ok) throw new Error(`SORA POST failed: HTTP ${postRes.status}`);
  const postHtml = await postRes.text();
  const rows = parseSoraRows(postHtml);

  if (!rows.length) {
    throw new Error(
      [
        'SORA parsing yielded 0 rows.',
        `Response length: ${postHtml.length}`,
        `First 400 chars: ${postHtml.slice(0, 400).replace(/\s+/g, ' ')}`,
        `Posted parameter keys: ${Object.keys(payload).join(', ')}`
      ].join('\n')
    );
  }

  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - 180);
  const kept = rows.filter((r) => new Date(`${r.date}T00:00:00Z`) >= cutoff);
  return kept;
}

function latestFromCsvRow(row) {
  const candidates = Object.entries(row)
    .map(([k, v]) => ({ period: k, value: toNumber(v), dt: parsePeriodToDate(k) }))
    .filter((x) => x.dt && x.value != null)
    .sort((a, b) => a.dt - b.dt);
  return candidates[candidates.length - 1] || null;
}

async function fetchMasMsbI6() {
  const res = await fetch(MAS_MSB_I6_CSV_URL, { headers: DEFAULT_JSON_HEADERS });
  if (!res.ok) throw new Error(`MAS MSB CSV URL not reachable: ${MAS_MSB_I6_CSV_URL} (HTTP ${res.status})`);
  const text = await res.text();
  const rows = parseSimpleCsv(text);
  if (!rows.length) throw new Error('MAS MSB CSV parsed but contains no rows');

  const headers = Object.keys(rows[0]);
  const rowLabel = (r) => headers.map((h) => String(r[h] || '')).join(' | ');

  const granted = rows.find((r) => /building\s*&?\s*construction/i.test(rowLabel(r)) && /loan\s*limits.*granted/i.test(rowLabel(r)));
  const utilised = rows.find((r) => /building\s*&?\s*construction/i.test(rowLabel(r)) && /loan\s*limits.*utili[sz]ed/i.test(rowLabel(r)));

  if (!granted || !utilised) {
    const preview = rows.slice(0, 5).map((r) => {
      const copy = {};
      for (const k of headers.slice(0, 6)) copy[k] = r[k];
      return copy;
    });
    throw new Error(
      [
        'Unable to extract MAS MSB I.6 construction granted/utilised rows.',
        `URL: ${MAS_MSB_I6_CSV_URL}`,
        `Detected header row: ${headers.join(' | ')}`,
        `First 5 parsed rows: ${JSON.stringify(preview, null, 2)}`
      ].join('\n')
    );
  }

  return { headers, granted, utilised };
}

async function buildMacroIndicators(verifyOnly = false) {
  const series = {};
  const results = [];

  const tryIndicator = async (meta, fn) => {
    try {
      const payload = await fn();
      return recordOk(results, { ...meta, ...payload });
    } catch (err) {
      return recordFail(results, { ...meta, error_summary: summarizeError(err) });
    }
  };

  const datasetCache = new Map();
  const discoveryCache = new Map();
  const filteredCallCache = new Map();
  const getDataset = async (datasetId) => {
    if (!datasetCache.has(datasetId)) {
      datasetCache.set(datasetId, await fetchDataset(datasetId));
    }
    return datasetCache.get(datasetId);
  };
  const getDiscovery = async (datasetId) => {
    if (!discoveryCache.has(datasetId)) {
      discoveryCache.set(datasetId, await discoverDatasetFields(datasetId));
    }
    return discoveryCache.get(datasetId);
  };

  for (const [datasetId, requirements] of Object.entries(REQUIRED_DATASETS)) {
    const discovery = await getDiscovery(datasetId);
    for (const requirement of requirements) {
      await tryIndicator(
        { key: requirement.key, source: 'data.gov.sg', dataset_ref: datasetId },
        async () => {
          const { row } = await fetchSeriesRowByExactMatch(datasetId, requirement, discovery, filteredCallCache);
          const latest = extractLatestFromRecordUsingFields(row, discovery.timeFieldIds);
          if (!latest) throw new Error(`0 time fields seriesFieldId=${discovery.seriesFieldId} seriesName=${requirement.target}`);
          const units = requirement.key.startsWith('loan_') ? 'S$ million' : '%';
          const freq = datasetId === 'd_f9fc9b5420d96bcab45bc31eeb8ae3c3' ? 'Q' : 'M';
          series[requirement.key] = { freq, latest_period: latest.period, latest_value: latest.value, units };
          return { latest_period: latest.period, latest_value: latest.value };
        }
      );
    }
  }

  const optionalDatasetSpecs = [
    {
      datasetId: 'd_29f7b431ad79f61f19a731a6a86b0247', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['steel', 'cement', 'sand', 'ready mixed'], 3).map((row) => ({
        key: `construction_material_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '')}`,
        freq: 'M', units: 'index', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_ba3c493ad160125ce347d5572712f14f', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['demand', 'construction'], 2).map((row) => ({
        key: `construction_material_demand_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '')}`,
        freq: 'M', units: 'index', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_055b6549444dedb341c50805d9682a41', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['private', 'residential', 'pipeline', 'uncompleted'], 2).map((row) => ({
        key: `private_residential_pipeline_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: 'Q', units: 'units', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_e47c0f0674b46981c4994d5257de5be4', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['commercial', 'industrial', 'pipeline'], 2).map((row) => ({
        key: `commercial_industrial_pipeline_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: 'Q', units: 'units', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_4dca06508cd9d0a8076153443c17ea5f', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['industrial', 'space', 'supply'], 2).map((row) => ({
        key: `industrial_space_supply_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: 'Q', units: 'sqm', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_e9cc9d297b1cf8024cf99db4b12505cc', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['private', 'available', 'office', 'retail', 'industrial'], 4)
        .filter((row) => /private/i.test(String(row[sf])) && /available/i.test(String(row[sf])))
        .map((row) => ({
          key: `private_available_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
          freq: 'Q', units: 'sqm', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
        }))
    },
    {
      datasetId: 'd_df200b7f89f94e52964ff45cd7878a30', source: 'data.gov.sg',
      build: (records, sf) => pickByKeywords(records, sf, ['construction', 'real estate'], 3).map((row) => ({
        key: `gdp_industry_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: 'Q', units: 'S$ million', latest: extractLatestFromRecord(row), metadata: { source_series_name: row[sf] }
      }))
    }
  ];

  for (const spec of optionalDatasetSpecs) {
    await tryIndicator({ key: `dataset_${spec.datasetId}`, source: spec.source, dataset_ref: spec.datasetId }, async () => {
      const records = await getDataset(spec.datasetId);
      if (!records.length) throw new Error('dataset returned 0 rows');
      const seriesField = detectSeriesField(records);
      if (!seriesField) throw new Error('no Data Series field');
      const entries = spec.build(records, seriesField);
      if (!entries.length) throw new Error('no matching series');
      let latestSeen = null;
      for (const entry of entries) {
        if (!entry.latest) continue;
        latestSeen = entry.latest;
        if (!verifyOnly) {
          series[entry.key] = {
            freq: entry.freq,
            latest_period: entry.latest.period,
            latest_value: entry.latest.value,
            units: entry.units,
            ...(entry.metadata || {})
          };
        }
      }
      if (!latestSeen) throw new Error('0 time columns detected');
      return { latest_period: latestSeen.period, latest_value: latestSeen.value };
    });
  }

  await tryIndicator({ key: 'sora_overnight', source: 'MAS eServices', dataset_ref: MAS_SORA_URL }, async () => {
    const soraRows = await fetchSoraSeries();
    if (!soraRows.length) throw new Error('html parse 0 rows');
    const latest = soraRows[soraRows.length - 1];
    if (!verifyOnly) {
      series.sora_overnight = { freq: 'D', window_days: 180, values: soraRows, units: '%pa' };
    }
    return { latest_period: latest.date, latest_value: latest.value };
  });

  await tryIndicator({ key: 'loan_limits_granted_building_construction', source: 'MAS MSB', dataset_ref: MAS_MSB_I6_CSV_URL }, async () => {
    const msb = await fetchMasMsbI6();
    const grantedLatest = latestFromCsvRow(msb.granted);
    if (!grantedLatest) throw new Error('0 time columns detected');
    if (!verifyOnly) {
      series.loan_limits_granted_building_construction = { freq: 'M', latest_period: grantedLatest.period, latest_value: grantedLatest.value, units: 'S$ million' };
    }
    return { latest_period: grantedLatest.period, latest_value: grantedLatest.value };
  });

  await tryIndicator({ key: 'loan_limits_utilised_building_construction', source: 'MAS MSB', dataset_ref: MAS_MSB_I6_CSV_URL }, async () => {
    const msb = await fetchMasMsbI6();
    const utilisedLatest = latestFromCsvRow(msb.utilised);
    if (!utilisedLatest) throw new Error('0 time columns detected');
    if (!verifyOnly) {
      series.loan_limits_utilised_building_construction = { freq: 'M', latest_period: utilisedLatest.period, latest_value: utilisedLatest.value, units: 'S$ million' };
    }
    return { latest_period: utilisedLatest.period, latest_value: utilisedLatest.value };
  });

  await tryIndicator({ key: 'term_spread_10y_2y', source: 'derived', dataset_ref: 'sgs_10y-sgs_2y' }, async () => {
    if (!series.sgs_10y || !series.sgs_2y) throw new Error('no matching series');
    const latestValue = Number((series.sgs_10y.latest_value - series.sgs_2y.latest_value).toFixed(4));
    if (!verifyOnly) {
      series.term_spread_10y_2y = {
        freq: 'M',
        latest_period: series.sgs_10y.latest_period,
        latest_value: latestValue,
        units: 'pp'
      };
    }
    return { latest_period: series.sgs_10y.latest_period, latest_value: latestValue };
  });

  const updateRun = printRunSummary(results);
  return { series, updateRun, results };
}

async function main() {
  if (VERIFY_MODE) {
    console.log('Running source verification only...');
    const { updateRun } = await buildMacroIndicators(true);
    if (updateRun.ok_count > 0) {
      console.log('Source verification completed with usable indicators.');
      return;
    }
    process.exitCode = 1;
    throw new Error('All indicators failed verification');
  }

  const existing = JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
  if (Array.isArray(existing.indicators)) {
    existing.indicators = existing.indicators.map((indicator) => ({ ...indicator, is_mock: true }));
  }

  const nowIso = new Date().toISOString();
  const existingSeries = existing?.macro_indicators?.series || {};
  const { series: fetchedSeries, updateRun, results } = await buildMacroIndicators(false);

  const mergedSeries = { ...existingSeries };
  for (const [key, value] of Object.entries(fetchedSeries)) {
    mergedSeries[key] = {
      ...(existingSeries[key] || {}),
      ...value,
      status: 'ok',
      last_ok_utc: nowIso
    };
    delete mergedSeries[key].error_summary;
  }

  for (const result of results.filter((r) => r.status === 'failed')) {
    mergedSeries[result.key] = {
      ...(mergedSeries[result.key] || {}),
      status: 'failed',
      error_summary: result.error_summary
    };
  }

  existing.macro_indicators = {
    ...(existing.macro_indicators || {}),
    last_updated_utc: nowIso,
    update_run: {
      ok_count: updateRun.ok_count,
      failed_count: updateRun.failed_count,
      failed_items: updateRun.failed_items.map((item) => ({
        name: item.key,
        source: item.source,
        dataset_ref: item.dataset_ref,
        error_summary: item.error_summary
      }))
    },
    sources: [
      { name: 'data.gov.sg', method: 'datastore_search', dataset_ids: DATASET_IDS },
      { name: 'MAS eServices', method: 'html_form_parse', url: MAS_SORA_URL },
      { name: 'MAS MSB', method: 'csv_download', url: MAS_MSB_I6_CSV_URL }
    ],
    series: mergedSeries
  };

  await fs.writeFile(DATA_FILE, `${JSON.stringify(existing, null, 2)}\n`, 'utf8');
  console.log(`Updated ${path.relative(process.cwd(), DATA_FILE)} with ${Object.keys(fetchedSeries).length} successful series updates.`);

  if (updateRun.ok_count === 0) {
    process.exitCode = 1;
    throw new Error('Update completed but zero indicators were successfully updated');
  }
}

main().catch((err) => {
  console.error(`update_macro_indicators failed: ${err.message}`);
  process.exit(1);
});
