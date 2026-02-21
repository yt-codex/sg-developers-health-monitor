#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');
const {
  DEFAULT_HEADERS,
  fetchAllRecords,
  detectTimeFields,
  parseQuarterlyFieldId,
  detectSeriesFieldId,
  findSeriesRow,
  extractLatest,
  toFiniteNumber
} = require('./lib/datagov');

const VERIFY_MODE = process.argv.includes('--verify_sources');
const IS_GITHUB_ACTIONS = process.env.GITHUB_ACTIONS === 'true';
const DATA_FILE = path.join(process.cwd(), 'data', 'macro_indicators.json');
const MAS_SORA_URL = 'https://eservices.mas.gov.sg/statistics/dir/domesticinterestrates.aspx';
const MAS_MSB_I6_CSV_URL = 'https://www.mas.gov.sg/-/media/mas-media-library/statistics/monthly-statistical-bulletin/msb-historical/money-and-banking--i6--yearly.csv';
const SGS_DATASET_ID = 'd_5fe5a4bb4a1ecc4d8a56a095832e2b24';
const SGS_10Y_SERIES = 'Government Securities - 10-Year Bond Yield';
const SGS_2Y_SERIES = 'Government Securities - 2-Year Bond Yield';
const DEFAULT_JSON_HEADERS = { ...DEFAULT_HEADERS };

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
  d_f9fc9b5420d96bcab45bc31eeb8ae3c3: [
    { key: 'unit_labour_cost_construction', target: 'Unit Labour Cost Of Construction' }
  ],
  d_af0415517a3a3a94b3b74039934ef976: [
    { key: 'loan_bc_total', target: 'Loans To Businesses - Building And Construction - Total' },
    { key: 'loan_bc_construction', target: 'Loans To Businesses - Building And Construction - Construction' },
    { key: 'loan_bc_real_property', target: 'Loans To Businesses - Building And Construction - Real Property And Development Of Land' }
  ]
};

let dataGovApiKey = process.env.DATA_GOV_SG_API_KEY || '';

function parseEnvLine(line) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) return null;
  const eq = trimmed.indexOf('=');
  if (eq === -1) return null;

  const key = trimmed.slice(0, eq).trim();
  if (!key) return null;
  let value = trimmed.slice(eq + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1);
  }
  return [key, value];
}

async function loadLocalEnvIfPresent() {
  if (IS_GITHUB_ACTIONS) return;
  const envPath = path.join(process.cwd(), '.env');
  try {
    const content = await fs.readFile(envPath, 'utf8');
    for (const line of content.split(/\r?\n/)) {
      const parsed = parseEnvLine(line);
      if (!parsed) continue;
      const [key, value] = parsed;
      if (process.env[key] == null || process.env[key] === '') {
        process.env[key] = value;
      }
    }
  } catch (err) {
    if (err.code !== 'ENOENT') {
      throw new Error(`Unable to load .env file: ${err.message}`);
    }
  }

  dataGovApiKey = process.env.DATA_GOV_SG_API_KEY || '';
}

function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
}

const toNumber = toFiniteNumber;

function inferFrequencyFromTimeFields(timeFields) {
  const monthlyCount = timeFields.filter((x) => x.periodType === 'M').length;
  const quarterlyCount = timeFields.filter((x) => x.periodType === 'Q').length;
  return quarterlyCount > monthlyCount ? 'Q' : 'M';
}

async function fetchDataset(datasetId) {
  const dataset = await fetchAllRecords(datasetId, dataGovApiKey, { verifyMode: VERIFY_MODE });
  const timeFields = detectTimeFields(dataset.fields);
  const seriesField = detectSeriesFieldId(dataset.fields);
  if (VERIFY_MODE) {
    console.log(
      `[dataset] ${datasetId} rows=${dataset.records.length} seriesField=${seriesField || 'n/a'} timeFields=${timeFields.length} newest=${timeFields.slice(0, 3).map((x) => x.id).join(',')}`
    );
  }
  return {
    ...dataset,
    timeFields,
    seriesField,
    frequency: inferFrequencyFromTimeFields(timeFields)
  };
}

function quarterlyParserSelfTest() {
  const checks = [
    { input: '20152Q', year: 2015, quarter: 2 },
    { input: '2015Q2', year: 2015, quarter: 2 },
    { input: '2015 Q2', year: 2015, quarter: 2 }
  ];
  for (const check of checks) {
    const parsed = parseQuarterlyFieldId(check.input);
    if (!parsed || parsed.year !== check.year || parsed.quarter !== check.quarter) {
      throw new Error(`quarter parse failed for ${check.input}`);
    }
    if (VERIFY_MODE) {
      console.log(`[quarterly-parse-ok] ${check.input} => year=${parsed.year} quarter=${parsed.quarter}`);
    }
  }
}

function parsePeriodToDate(period) {
  const p = String(period).trim().replace(/\s*\(p\)\s*$/i, '').trim();
  let m = p.match(/^(\d{4})\s*Q([1-4])$/i);
  if (m) {
    const year = Number(m[1]);
    const quarter = Number(m[2]);
    return new Date(Date.UTC(year, quarter * 3 - 1, 1));
  }
  m = p.match(/^(\d{4})[-/](\d{2})([-/](\d{2}))?$/);
  if (m) return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[4] || '1')));
  m = p.match(/^(\d{4})Q([1-4])$/i);
  if (m) {
    const year = Number(m[1]);
    const quarter = Number(m[2]);
    return new Date(Date.UTC(year, quarter * 3 - 1, 1));
  }
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

async function extractSgs2y10y() {
  const { records, timeFields, seriesField } = await fetchDataset(SGS_DATASET_ID);
  if (!records.length) throw new Error('No records returned');
  const monthColumns = timeFields.filter((x) => x.periodType === 'M').map((x) => x.id);
  if (!monthColumns.length) throw new Error('No month columns detected');
  if (!seriesField) throw new Error('no Data Series field');
  const match10 = findSeriesRow(records, seriesField, SGS_10Y_SERIES);
  const match2 = findSeriesRow(records, seriesField, SGS_2Y_SERIES);
  if (!match10.row) throw new Error(`${SGS_10Y_SERIES}: ${match10.error || 'series not found'}`);
  if (!match2.row) throw new Error(`${SGS_2Y_SERIES}: ${match2.error || 'series not found'}`);

  const latest10 = extractLatest(match10.row, timeFields.filter((x) => x.periodType === 'M'));
  const latest2 = extractLatest(match2.row, timeFields.filter((x) => x.periodType === 'M'));
  if (!latest10) throw new Error(`No numeric values found for: ${SGS_10Y_SERIES}`);
  if (!latest2) throw new Error(`No numeric values found for: ${SGS_2Y_SERIES}`);

  let spread = null;
  let spreadPeriod = null;
  for (const col of monthColumns) {
    const v10 = toNumber(match10.row[col]);
    const v2 = toNumber(match2.row[col]);
    if (v10 != null && v2 != null) {
      spread = Number((v10 - v2).toFixed(4));
      spreadPeriod = col;
      break;
    }
  }

  return {
    sgs_10y: { freq: 'M', latest_period: latest10.latest_period, latest_value: latest10.latest_value, units: '%' },
    sgs_2y: { freq: 'M', latest_period: latest2.latest_period, latest_value: latest2.latest_value, units: '%' },
    term_spread_10y_2y: spread != null
      ? { freq: 'M', latest_period: spreadPeriod, latest_value: spread, units: 'pp' }
      : null
  };
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

function summarizeError(err) {
  const message = String(err?.message || err || 'unknown error').split('\n')[0].trim();
  const httpMatch = message.match(/HTTP\s+(\d{3})/i);
  if (httpMatch) return `HTTP ${httpMatch[1]} ${message.replace(/.*HTTP\s+\d{3}\s*/i, '').trim()}`.trim();
  return message;
}

function recordOk(results, payload) {
  const result = { ...payload, status: 'ok' };
  results.push(result);
  console.log(`[OK] ${result.key} ${result.dataset_ref} ${result.series_name || '-'} ${result.latest_period || '-'} ${result.latest_value ?? '-'}`);
  return result;
}

function recordFail(results, payload) {
  const result = { ...payload, status: 'failed' };
  results.push(result);
  console.log(`[FAIL] ${result.key} ${result.dataset_ref} ${result.series_name || '-'} ${result.error_summary}`);
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
  const getDataset = async (datasetId) => {
    if (!datasetCache.has(datasetId)) {
      const dataset = await fetchDataset(datasetId);
      datasetCache.set(datasetId, dataset);
    }
    return datasetCache.get(datasetId);
  };

  let sgsBundlePromise;
  const getSgsBundle = async () => {
    if (!sgsBundlePromise) sgsBundlePromise = extractSgs2y10y();
    return sgsBundlePromise;
  };

  await tryIndicator({ key: 'sgs_10y', source: 'data.gov.sg', dataset_ref: SGS_DATASET_ID, series_name: SGS_10Y_SERIES }, async () => {
    const sgs = await getSgsBundle();
    if (!verifyOnly) series.sgs_10y = sgs.sgs_10y;
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetId=${SGS_DATASET_ID} required=${SGS_10Y_SERIES} latest=${sgs.sgs_10y.latest_period}=${sgs.sgs_10y.latest_value}`);
    }
    return { latest_period: sgs.sgs_10y.latest_period, latest_value: sgs.sgs_10y.latest_value };
  });

  await tryIndicator({ key: 'sgs_2y', source: 'data.gov.sg', dataset_ref: SGS_DATASET_ID, series_name: SGS_2Y_SERIES }, async () => {
    const sgs = await getSgsBundle();
    if (!verifyOnly) series.sgs_2y = sgs.sgs_2y;
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetId=${SGS_DATASET_ID} required=${SGS_2Y_SERIES} latest=${sgs.sgs_2y.latest_period}=${sgs.sgs_2y.latest_value}`);
    }
    return { latest_period: sgs.sgs_2y.latest_period, latest_value: sgs.sgs_2y.latest_value };
  });

  await tryIndicator({ key: 'term_spread_10y_2y', source: 'derived', dataset_ref: 'sgs_10y-sgs_2y' }, async () => {
    const sgs = await getSgsBundle();
    if (!sgs.term_spread_10y_2y) throw new Error('no overlapping monthly observations for SGS 10Y and 2Y');
    if (!verifyOnly) series.term_spread_10y_2y = sgs.term_spread_10y_2y;
    return { latest_period: sgs.term_spread_10y_2y.latest_period, latest_value: sgs.term_spread_10y_2y.latest_value };
  });

  for (const [datasetId, requirements] of Object.entries(REQUIRED_DATASETS)) {
    const dataset = await getDataset(datasetId);
    const records = dataset.records;
    for (const requirement of requirements) {
      await tryIndicator(
        { key: requirement.key, source: 'data.gov.sg', dataset_ref: datasetId, series_name: requirement.target },
        async () => {
          if (!records.length) throw new Error('dataset returned 0 rows');
          if (!dataset.seriesField) throw new Error('no Data Series field');
          if (!dataset.timeFields.length) throw new Error('0 time fields');
          const match = findSeriesRow(records, dataset.seriesField, requirement.target);
          if (!match.row) throw new Error(match.error || 'series not found');
          const latest = extractLatest(match.row, dataset.timeFields);
          if (!latest) throw new Error('no numeric values');
          const units = requirement.key.startsWith('loan_') ? 'S$ million' : '%';
          const freq = dataset.frequency;
          if (VERIFY_MODE) {
            console.log(`[verify-series] datasetId=${datasetId} required=${requirement.target} matched=${match.matchedSeriesName} (${match.matchType}) latest=${latest.latest_period}=${latest.latest_value}`);
          }
          series[requirement.key] = { freq, latest_period: latest.latest_period, latest_value: latest.latest_value, units };
          return { latest_period: latest.latest_period, latest_value: latest.latest_value, series_name: requirement.target };
        }
      );
    }
  }

  const optionalDatasetSpecs = [
    {
      datasetId: 'd_29f7b431ad79f61f19a731a6a86b0247', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['steel', 'cement', 'sand', 'ready mixed'], 3).map((row) => ({
        key: `construction_material_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'index', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_ba3c493ad160125ce347d5572712f14f', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['demand', 'construction'], 2).map((row) => ({
        key: `construction_material_demand_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'index', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_055b6549444dedb341c50805d9682a41', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['private', 'residential', 'pipeline', 'uncompleted'], 2).map((row) => ({
        key: `private_residential_pipeline_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'units', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_e47c0f0674b46981c4994d5257de5be4', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['commercial', 'industrial', 'pipeline'], 2).map((row) => ({
        key: `commercial_industrial_pipeline_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'units', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_4dca06508cd9d0a8076153443c17ea5f', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['industrial', 'space', 'supply'], 2).map((row) => ({
        key: `industrial_space_supply_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'sqm', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    },
    {
      datasetId: 'd_e9cc9d297b1cf8024cf99db4b12505cc', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['private', 'available', 'office', 'retail', 'industrial'], 4)
        .filter((row) => /private/i.test(String(row[sf])) && /available/i.test(String(row[sf])))
        .map((row) => ({
          key: `private_available_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
          freq: inferFrequencyFromTimeFields(tf), units: 'sqm', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
        }))
    },
    {
      datasetId: 'd_df200b7f89f94e52964ff45cd7878a30', source: 'data.gov.sg',
      build: (records, sf, tf) => pickByKeywords(records, sf, ['construction', 'real estate'], 3).map((row) => ({
        key: `gdp_industry_${String(row[sf]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`,
        freq: inferFrequencyFromTimeFields(tf), units: 'S$ million', latest: extractLatest(row, tf), metadata: { source_series_name: row[sf] }
      }))
    }
  ];

  for (const spec of optionalDatasetSpecs) {
    await tryIndicator({ key: `dataset_${spec.datasetId}`, source: spec.source, dataset_ref: spec.datasetId }, async () => {
      const dataset = await getDataset(spec.datasetId);
      const records = dataset.records;
      if (!records.length) throw new Error('dataset returned 0 rows');
      const seriesField = dataset.seriesField;
      const timeFields = dataset.timeFields;
      if (!seriesField) throw new Error('no Data Series field');
      if (!timeFields.length) throw new Error('0 time fields');
      const entries = spec.build(records, seriesField, timeFields);
      if (!entries.length) throw new Error('no matching series');
      let latestSeen = null;
      for (const entry of entries) {
        if (!entry.latest) continue;
        latestSeen = entry.latest;
        if (!verifyOnly) {
          series[entry.key] = {
            freq: entry.freq,
            latest_period: entry.latest.latest_period,
            latest_value: entry.latest.latest_value,
            units: entry.units,
            ...(entry.metadata || {})
          };
        }
      }
      if (!latestSeen) throw new Error('0 time columns detected');
      return { latest_period: latestSeen.latest_period, latest_value: latestSeen.latest_value };
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


  const updateRun = printRunSummary(results);
  return { series, updateRun, results };
}

async function main() {
  await loadLocalEnvIfPresent();

  const hasApiKey = Boolean((process.env.DATA_GOV_SG_API_KEY || '').trim());
  dataGovApiKey = process.env.DATA_GOV_SG_API_KEY || '';
  if (VERIFY_MODE) {
    console.log(`[auth] data.gov.sg API key present: ${hasApiKey ? 'yes' : 'no'}`);
  }

  if (!hasApiKey) {
    if (IS_GITHUB_ACTIONS) {
      throw new Error('Missing DATA_GOV_SG_API_KEY in GitHub Actions. Add repository secret DATA_GOV_SG_API_KEY and pass it to workflow env.');
    }
    throw new Error('Missing DATA_GOV_SG_API_KEY. Create a local .env with DATA_GOV_SG_API_KEY=... or export it in your shell.');
  }

  if (VERIFY_MODE) {
    quarterlyParserSelfTest();
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
