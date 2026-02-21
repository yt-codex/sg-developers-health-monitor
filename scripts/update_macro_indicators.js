#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');
const { load } = require('cheerio');
const { parse } = require('csv-parse/sync');

const VERIFY_MODE = process.argv.includes('--verify_sources');
const DATA_FILE = path.join(process.cwd(), 'data', 'macro_indicators.json');
const CKAN_BASE = 'https://data.gov.sg/api/action/datastore_search';
const MAS_SORA_URL = 'https://eservices.mas.gov.sg/statistics/dir/domesticinterestrates.aspx';
// NOTE: this URL can change if MAS reorganises MSB publication paths.
const MAS_MSB_I6_CSV_URL = 'https://www.mas.gov.sg/-/media/MAS/Statistics/Time-Series-Data/Monthly-Statistical-Bulletin/CSV/I6.csv';

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
    { key: 'sgs_10y', target: 'SGS 10-Year Bond Yield' },
    { key: 'sgs_2y', target: 'SGS 2-Year Bond Yield' }
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
  let m = p.match(/^(\d{4})\s+Q([1-4])$/i);
  if (m) {
    const year = Number(m[1]);
    const quarter = Number(m[2]);
    return new Date(Date.UTC(year, quarter * 3 - 1, 1));
  }
  m = p.match(/^(\d{4})[-/](\d{2})([-/](\d{2}))?$/);
  if (m) return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[4] || '1')));
  m = p.match(/^([A-Za-z]{3,})\s+(\d{4})$/);
  if (m) {
    const month = new Date(`${m[1]} 1, 2000`).getMonth();
    if (!Number.isNaN(month)) return new Date(Date.UTC(Number(m[2]), month, 1));
  }
  m = p.match(/^(\d{4})\s+([A-Za-z]{3,})$/);
  if (m) {
    const month = new Date(`${m[2]} 1, 2000`).getMonth();
    if (!Number.isNaN(month)) return new Date(Date.UTC(Number(m[1]), month, 1));
  }
  return null;
}

function extractLatestFromRecord(record) {
  const entries = Object.entries(record)
    .map(([k, v]) => ({ period: k, value: toNumber(v), dt: parsePeriodToDate(k) }))
    .filter((x) => x.dt && x.value != null)
    .sort((a, b) => a.dt - b.dt);

  if (!entries.length) return null;
  const latest = entries[entries.length - 1];
  return { period: latest.period, value: latest.value };
}

function detectSeriesField(records) {
  const keys = Object.keys(records[0] || {});
  const preferred = keys.find((k) => /data\s*series/i.test(k));
  if (preferred) return preferred;
  return keys.find((k) => /series|description|category|indicator/i.test(k));
}

async function fetchDataset(datasetId, limit = 5000) {
  const url = `${CKAN_BASE}?resource_id=${datasetId}&limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${datasetId}`);
  const json = await res.json();
  if (!json?.success || !Array.isArray(json?.result?.records)) {
    throw new Error(`Unexpected CKAN payload for ${datasetId}`);
  }
  return json.result.records;
}

function findSeriesOrThrow(records, seriesField, requirement, datasetId) {
  const seriesNames = [...new Set(records.map((r) => String(r[seriesField] || '').trim()).filter(Boolean))];
  let found = records.find((r) => String(r[seriesField] || '').trim() === requirement.target);
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

function parseSoraRows(html) {
  const $ = load(html);
  const rows = [];
  $('table tr').each((_, tr) => {
    const cells = $(tr)
      .find('td')
      .map((__, td) => $(td).text().trim())
      .get();
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
  const $ = load(html);
  const payload = {};
  $('input[type="hidden"]').each((_, el) => {
    const name = $(el).attr('name');
    if (name) payload[name] = $(el).attr('value') || '';
  });

  const now = new Date();
  const start = new Date(now);
  start.setDate(start.getDate() - 14);

  const setSelectValue = (regex, desiredTextRegex, fallbackValue) => {
    const el = $('select').filter((_, s) => regex.test($(s).attr('name') || $(s).attr('id') || ''));
    if (!el.length) return;
    const name = el.first().attr('name');
    let chosen = fallbackValue;
    if (desiredTextRegex) {
      const opt = el
        .first()
        .find('option')
        .filter((__, o) => desiredTextRegex.test($(o).text()))
        .first();
      if (opt.length) chosen = opt.attr('value');
    }
    payload[name] = chosen;
  };

  setSelectValue(/start.*year|from.*year/i, null, String(start.getUTCFullYear()));
  setSelectValue(/end.*year|to.*year/i, null, String(now.getUTCFullYear()));
  setSelectValue(/start.*month|from.*month/i, null, String(start.getUTCMonth() + 1));
  setSelectValue(/end.*month|to.*month/i, null, String(now.getUTCMonth() + 1));
  setSelectValue(/freq|frequency/i, /daily/i, null);
  setSelectValue(/series|rate|stat/i, /sora/i, null);

  const submit = $('input[type="submit"],button[type="submit"]').first();
  const submitName = submit.attr('name');
  if (submitName) payload[submitName] = submit.attr('value') || 'Submit';

  return payload;
}

async function fetchSoraSeries() {
  const getRes = await fetch(MAS_SORA_URL);
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
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
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
  const res = await fetch(MAS_MSB_I6_CSV_URL);
  if (!res.ok) throw new Error(`MAS MSB CSV URL not reachable: ${MAS_MSB_I6_CSV_URL} (HTTP ${res.status})`);
  const text = await res.text();
  const rows = parse(text, { columns: true, skip_empty_lines: true });
  if (!rows.length) throw new Error('MAS MSB CSV parsed but contains no rows');

  const headers = Object.keys(rows[0]);
  const rowLabel = (r) => headers.slice(0, 3).map((h) => String(r[h] || '')).join(' | ');

  const granted = rows.find((r) => /building\s*&?\s*construction/i.test(rowLabel(r)) && /granted/i.test(rowLabel(r)));
  const utilised = rows.find((r) => /building\s*&?\s*construction/i.test(rowLabel(r)) && /utili[sz]ed/i.test(rowLabel(r)));

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

  for (const datasetId of DATASET_IDS) {
    const records = await fetchDataset(datasetId);
    if (!records.length) throw new Error(`Dataset ${datasetId} returned 0 rows`);
    const seriesField = detectSeriesField(records);
    if (!seriesField) throw new Error(`Dataset ${datasetId} has no detectable series field`);

    if (REQUIRED_DATASETS[datasetId]) {
      for (const requirement of REQUIRED_DATASETS[datasetId]) {
        const row = findSeriesOrThrow(records, seriesField, requirement, datasetId);
        const latest = extractLatestFromRecord(row);
        if (!latest) throw new Error(`Latest extraction failed for ${datasetId} -> ${requirement.target}`);
        console.log(`[verify] ${datasetId} matched "${requirement.target}"`);

        if (!verifyOnly) {
          const units = requirement.key.startsWith('loan_') ? 'S$ million' : '%';
          const freq = datasetId === 'd_f9fc9b5420d96bcab45bc31eeb8ae3c3' ? 'Q' : 'M';
          addSeries(series, requirement.key, freq, latest, units);
        }
      }
    }

    if (verifyOnly) continue;

    if (datasetId === 'd_29f7b431ad79f61f19a731a6a86b0247') {
      const picked = pickByKeywords(records, seriesField, ['steel', 'cement', 'sand', 'ready mixed'], 3);
      for (const row of picked) {
        const name = String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
        addSeries(series, `construction_material_${name}`, 'M', extractLatestFromRecord(row), 'index', {
          source_series_name: row[seriesField]
        });
      }
    }

    if (datasetId === 'd_ba3c493ad160125ce347d5572712f14f') {
      const picked = pickByKeywords(records, seriesField, ['demand', 'construction'], 2);
      for (const row of picked) {
        const key = String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
        addSeries(series, `construction_material_demand_${key}`, 'M', extractLatestFromRecord(row), 'index', {
          source_series_name: row[seriesField]
        });
      }
    }

    if (datasetId === 'd_055b6549444dedb341c50805d9682a41') {
      const picked = pickByKeywords(records, seriesField, ['private', 'residential', 'pipeline', 'uncompleted'], 2);
      for (const row of picked) {
        addSeries(series, `private_residential_pipeline_${String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`, 'Q', extractLatestFromRecord(row), 'units', { source_series_name: row[seriesField] });
      }
    }

    if (datasetId === 'd_e47c0f0674b46981c4994d5257de5be4') {
      const picked = pickByKeywords(records, seriesField, ['commercial', 'industrial', 'pipeline'], 2);
      for (const row of picked) {
        addSeries(series, `commercial_industrial_pipeline_${String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`, 'Q', extractLatestFromRecord(row), 'units', { source_series_name: row[seriesField] });
      }
    }

    if (datasetId === 'd_4dca06508cd9d0a8076153443c17ea5f') {
      const picked = pickByKeywords(records, seriesField, ['industrial', 'space', 'supply'], 2);
      for (const row of picked) {
        addSeries(series, `industrial_space_supply_${String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`, 'Q', extractLatestFromRecord(row), 'sqm', { source_series_name: row[seriesField] });
      }
    }

    if (datasetId === 'd_e9cc9d297b1cf8024cf99db4b12505cc') {
      const picked = pickByKeywords(records, seriesField, ['private', 'available', 'office', 'retail', 'industrial'], 4)
        .filter((row) => /private/i.test(String(row[seriesField])) && /available/i.test(String(row[seriesField])));
      for (const row of picked) {
        addSeries(series, `private_available_${String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`, 'Q', extractLatestFromRecord(row), 'sqm', { source_series_name: row[seriesField] });
      }
    }

    if (datasetId === 'd_df200b7f89f94e52964ff45cd7878a30') {
      const picked = pickByKeywords(records, seriesField, ['construction', 'real estate'], 3);
      for (const row of picked) {
        addSeries(series, `gdp_industry_${String(row[seriesField]).toLowerCase().replace(/[^a-z0-9]+/g, '_')}`, 'Q', extractLatestFromRecord(row), 'S$ million', { source_series_name: row[seriesField] });
      }
    }
  }

  const soraRows = await fetchSoraSeries();
  console.log(`[verify] SORA parsed rows: ${soraRows.length}`);
  if (!verifyOnly) {
    series.sora_overnight = {
      freq: 'D',
      window_days: 180,
      values: soraRows,
      units: '%pa'
    };
    const latest = soraRows[soraRows.length - 1];
    console.log(`- sora_overnight: ${latest.date} = ${latest.value}`);
  }

  const msb = await fetchMasMsbI6();
  const grantedLatest = latestFromCsvRow(msb.granted);
  const utilisedLatest = latestFromCsvRow(msb.utilised);
  if (!grantedLatest || !utilisedLatest) {
    throw new Error('MAS MSB latest extraction failed for granted/utilised building & construction rows');
  }
  console.log('[verify] MAS MSB I.6 rows extracted for granted/utilised Building & Construction');

  if (!verifyOnly) {
    addSeries(series, 'loan_limits_granted_building_construction', 'M', grantedLatest, 'S$ million');
    addSeries(series, 'loan_limits_utilised_building_construction', 'M', utilisedLatest, 'S$ million');
  }

  if (!verifyOnly) {
    if (series.sgs_10y && series.sgs_2y) {
      series.term_spread_10y_2y = {
        freq: 'M',
        latest_period: series.sgs_10y.latest_period,
        latest_value: Number((series.sgs_10y.latest_value - series.sgs_2y.latest_value).toFixed(4)),
        units: 'pp'
      };
      console.log(`- term_spread_10y_2y: ${series.term_spread_10y_2y.latest_period} = ${series.term_spread_10y_2y.latest_value}`);
    } else {
      throw new Error('Cannot compute term_spread_10y_2y because sgs_10y or sgs_2y is missing');
    }
  }

  return series;
}

async function main() {
  if (VERIFY_MODE) {
    console.log('Running source verification only...');
    await buildMacroIndicators(true);
    console.log('Source verification passed.');
    return;
  }

  const existing = JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
  if (Array.isArray(existing.indicators)) {
    existing.indicators = existing.indicators.map((indicator) => ({ ...indicator, is_mock: true }));
  }

  const series = await buildMacroIndicators(false);
  existing.macro_indicators = {
    last_updated_utc: new Date().toISOString(),
    sources: [
      { name: 'data.gov.sg', method: 'datastore_search', dataset_ids: DATASET_IDS },
      { name: 'MAS eServices', method: 'html_form_parse', url: MAS_SORA_URL },
      { name: 'MAS MSB', method: 'csv_download', url: MAS_MSB_I6_CSV_URL }
    ],
    series
  };

  await fs.writeFile(DATA_FILE, `${JSON.stringify(existing, null, 2)}\n`, 'utf8');
  console.log(`Updated ${path.relative(process.cwd(), DATA_FILE)} with ${Object.keys(series).length} series.`);
}

main().catch((err) => {
  console.error(`update_macro_indicators failed: ${err.message}`);
  process.exit(1);
});
