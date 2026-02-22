#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');
const {
  fetchAllRecords,
  detectTimeFields,
  parseQuarterlyFieldId,
  findSeriesFieldId,
  findSeriesRow,
  sortRecordsStable,
  getLatestCommonPeriod,
  sumCheck,
  extractLatest,
  extractSeriesValues
} = require('./lib/datagov');
const { MAS_I6_API_URL, fetchMasI6LoanLimits } = require('./lib/mas_api');
const {
  RATES_TABLE_ID: SINGSTAT_RATES_TABLE_ID,
  UNIT_LABOUR_TABLE_ID: SINGSTAT_UNIT_LABOUR_TABLE_ID,
  CONSTRUCTION_GDP_TABLE_ID: SINGSTAT_CONSTRUCTION_GDP_TABLE_ID,
  fetchSingStatRequiredSeries,
  fetchUnitLabourCostConstructionSeries,
  fetchConstructionGdpSeries,
  isoDateToQuarterPeriod
} = require('./lib/singstat_tablebuilder');

const VERIFY_MODE = process.argv.includes('--verify_sources');
const IS_GITHUB_ACTIONS = process.env.GITHUB_ACTIONS === 'true';
const DATA_FILE = path.join(process.cwd(), 'data', 'macro_indicators.json');
const SINGSTAT_RATES_DATASET_REF = `tablebuilder.singstat.gov.sg/table/${SINGSTAT_RATES_TABLE_ID}`;
const SINGSTAT_UNIT_LABOUR_DATASET_REF = `tablebuilder.singstat.gov.sg/table/${SINGSTAT_UNIT_LABOUR_TABLE_ID}`;
const SINGSTAT_CONSTRUCTION_GDP_DATASET_REF = `tablebuilder.singstat.gov.sg/table/${SINGSTAT_CONSTRUCTION_GDP_TABLE_ID}`;

const DATASET_IDS = [
  'd_29f7b431ad79f61f19a731a6a86b0247',
  'd_ba3c493ad160125ce347d5572712f14f',
  'd_055b6549444dedb341c50805d9682a41',
  'd_e47c0f0674b46981c4994d5257de5be4',
  'd_4dca06508cd9d0a8076153443c17ea5f',
  'd_e9cc9d297b1cf8024cf99db4b12505cc',
  'd_af0415517a3a3a94b3b74039934ef976'
];

const REQUIRED_DATASETS = {
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


function inferFrequencyFromTimeFields(timeFields) {
  const monthlyCount = timeFields.filter((x) => x.periodType === 'M').length;
  const quarterlyCount = timeFields.filter((x) => x.periodType === 'Q').length;
  return quarterlyCount > monthlyCount ? 'Q' : 'M';
}

async function fetchDataset(datasetId) {
  const dataset = await fetchAllRecords(datasetId, dataGovApiKey, { verifyMode: VERIFY_MODE });
  const timeFields = detectTimeFields(dataset.fields);
  const seriesField = findSeriesFieldId(dataset.fields);
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

async function extractRatesFromSingStatTableBuilder() {
  const series = await fetchSingStatRequiredSeries();

  const latest10 = series.SGS_10Y?.latest;
  const latest2 = series.SGS_2Y?.latest;
  const latestSora = series.SORA?.latest;
  if (!latest10 || !latest2 || !latestSora) {
    throw new Error('SingStat payload did not include latest values for all required series');
  }

  let spread = null;
  let spreadDate = null;
  const spreadValues = [];
  const monthly10y = new Map(series.SGS_10Y.rows.map((row) => [row.date, row.value]));
  for (const row of series.SGS_2Y.rows) {
    const ten = monthly10y.get(row.date);
    if (ten == null) continue;
    spread = Number((ten - row.value).toFixed(4));
    spreadDate = row.date;
    spreadValues.push({ period: row.date.slice(0, 7), value: spread });
  }

  return {
    datasetRef: SINGSTAT_RATES_DATASET_REF,
    matchedLabels: {
      SORA: series.SORA.rows[0]?.series_name,
      SGS_2Y: series.SGS_2Y.rows[0]?.series_name,
      SGS_10Y: series.SGS_10Y.rows[0]?.series_name
    },
    sora_overnight: {
      freq: 'M',
      values: series.SORA.rows.map((row) => ({ date: row.date, value: row.value })),
      latest_period: latestSora.date,
      latest_value: latestSora.value,
      units: '%'
    },
    sgs_10y: { freq: 'M', latest_period: latest10.date, latest_value: latest10.value, units: '%' },
    sgs_2y: { freq: 'M', latest_period: latest2.date, latest_value: latest2.value, units: '%' },
    SGS_10Y_values: series.SGS_10Y.rows.map((row) => ({ period: row.date.slice(0, 7), value: row.value })),
    SGS_2Y_values: series.SGS_2Y.rows.map((row) => ({ period: row.date.slice(0, 7), value: row.value })),
    term_spread_values: spreadValues,
    term_spread_10y_2y: spread != null
      ? { freq: 'M', latest_period: spreadDate, latest_value: spread, units: 'pp' }
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

function labelOf(row, seriesField) {
  return String(row?.[seriesField] || '').trim();
}

function verifyAdjacentComponentBlock(sortedRecords, seriesField, anchorIndex, expectedLabels, datasetId, anchorLabel) {
  const totalRow = sortedRecords[anchorIndex];
  const componentRows = sortedRecords.slice(anchorIndex + 1, anchorIndex + 1 + expectedLabels.length);
  const componentLabels = componentRows.map((row) => labelOf(row, seriesField));
  if (componentRows.length !== expectedLabels.length) {
    throw new Error(`expected ${expectedLabels.length} rows below ${anchorLabel}, found ${componentRows.length}`);
  }
  const mismatch = expectedLabels.findIndex((expected, idx) => componentLabels[idx] !== expected);
  if (mismatch !== -1) {
    const blockLabels = [labelOf(totalRow, seriesField), ...componentLabels];
    const nearby = sortedRecords
      .slice(Math.max(0, anchorIndex - 5), anchorIndex + 6)
      .map((row) => labelOf(row, seriesField));
    throw new Error(
      [
        `component label mismatch for ${anchorLabel}`,
        `found block: ${blockLabels.join(' | ')}`,
        `nearby labels: ${nearby.join(' | ')}`
      ].join(' ; ')
    );
  }
  if (VERIFY_MODE) {
    console.log(`[verify-block] datasetId=${datasetId} total=${labelOf(totalRow, seriesField)} components=${componentLabels.join(' | ')}`);
  }
  return { totalRow, componentRows, componentLabels };
}

function latestPeriodAndSumCheck({ datasetId, timeFields, totalRow, componentRows, seriesField, absTol = 1, relTol = 1e-3 }) {
  const period = getLatestCommonPeriod([totalRow, ...componentRows], timeFields);
  if (!period) throw new Error('no common period with numeric values across total/components');
  const check = sumCheck(totalRow, componentRows, period, absTol, relTol);
  if (!check.pass) {
    console.warn(`[warn] sum-check failed dataset=${datasetId} period=${period} total=${check.total} sum=${check.sum_components} diff=${check.diff}`);
  }
  if (VERIFY_MODE) {
    console.log(
      `[verify-sum] datasetId=${datasetId} period=${period} total=${check.total} sum_components=${check.sum_components} diff=${check.diff} pass=${check.pass}`
    );
  }
  return { period, check };
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

function mergeMonthlyHistory(existingValues, fetchedValues) {
  const byPeriod = new Map();
  let updated = 0;
  let appended = 0;
  for (const item of Array.isArray(existingValues) ? existingValues : []) {
    if (!item?.period) continue;
    byPeriod.set(item.period, { period: item.period, prelim: Boolean(item.prelim), value: item.value });
  }
  for (const item of fetchedValues) {
    if (!item?.period) continue;
    if (byPeriod.has(item.period)) updated += 1;
    else appended += 1;
    byPeriod.set(item.period, { period: item.period, prelim: Boolean(item.prelim), value: item.value });
  }
  const merged = [...byPeriod.values()].sort((a, b) => a.period.localeCompare(b.period));
  return { merged, updated, appended };
}

function mergePeriodHistory(existingValues, fetchedValues) {
  const byPeriod = new Map();
  for (const item of Array.isArray(existingValues) ? existingValues : []) {
    if (!item?.period) continue;
    byPeriod.set(item.period, { period: item.period, value: item.value });
  }
  for (const item of Array.isArray(fetchedValues) ? fetchedValues : []) {
    if (!item?.period) continue;
    byPeriod.set(item.period, { period: item.period, value: item.value });
  }
  return [...byPeriod.values()].sort((a, b) => a.period.localeCompare(b.period));
}

async function buildMacroIndicators(verifyOnly = false, existingSeries = {}) {
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

  let singStatBundlePromise;
  const getSingStatBundle = async () => {
    if (!singStatBundlePromise) singStatBundlePromise = extractRatesFromSingStatTableBuilder();
    return singStatBundlePromise;
  };

  await tryIndicator({ key: 'sgs_10y', source: 'SingStat TableBuilder', dataset_ref: SINGSTAT_RATES_DATASET_REF, series_name: 'Government Securities - 10-Year Bond Yield' }, async () => {
    const rates = await getSingStatBundle();
    if (!verifyOnly) {
      series.sgs_10y = {
        ...rates.sgs_10y,
        values: rates.SGS_10Y_values
      };
    }
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetRef=${rates.datasetRef} required=${rates.matchedLabels.SGS_10Y} latest=${rates.sgs_10y.latest_period}=${rates.sgs_10y.latest_value}`);
    }
    return { latest_period: rates.sgs_10y.latest_period, latest_value: rates.sgs_10y.latest_value };
  });

  await tryIndicator({ key: 'sgs_2y', source: 'SingStat TableBuilder', dataset_ref: SINGSTAT_RATES_DATASET_REF, series_name: 'Government Securities - 2-Year Bond Yield' }, async () => {
    const rates = await getSingStatBundle();
    if (!verifyOnly) {
      series.sgs_2y = {
        ...rates.sgs_2y,
        values: rates.SGS_2Y_values
      };
    }
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetRef=${rates.datasetRef} required=${rates.matchedLabels.SGS_2Y} latest=${rates.sgs_2y.latest_period}=${rates.sgs_2y.latest_value}`);
    }
    return { latest_period: rates.sgs_2y.latest_period, latest_value: rates.sgs_2y.latest_value };
  });

  await tryIndicator({ key: 'term_spread_10y_2y', source: 'derived', dataset_ref: 'sgs_10y-sgs_2y' }, async () => {
    const rates = await getSingStatBundle();
    if (!rates.term_spread_10y_2y) throw new Error('no overlapping monthly observations for SGS 10Y and 2Y');
    if (!verifyOnly) {
      series.term_spread_10y_2y = {
        ...rates.term_spread_10y_2y,
        values: rates.term_spread_values
      };
    }
    return { latest_period: rates.term_spread_10y_2y.latest_period, latest_value: rates.term_spread_10y_2y.latest_value };
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
          const values = extractSeriesValues(match.row, dataset.timeFields);
          series[requirement.key] = { freq, latest_period: latest.latest_period, latest_value: latest.latest_value, units, values };
          return { latest_period: latest.latest_period, latest_value: latest.latest_value, series_name: requirement.target };
        }
      );
    }
  }

  const optionalDatasetSpecs = [
    {
      datasetId: 'd_29f7b431ad79f61f19a731a6a86b0247', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const targets = [
          {
            key: 'construction_material_steel_reinforcement_bars_16_32mm_high_tensile',
            label: 'Steel Reinforcement Bars (16-32mm High Tensile)'
          },
          {
            key: 'construction_material_cement_in_bulk_ordinary_portland_cement',
            label: 'Cement In Bulk (Ordinary Portland Cement)'
          },
          {
            key: 'construction_material_concreting_sand',
            label: 'Concreting Sand'
          },
          {
            key: 'construction_material_granite_20mm_aggregate',
            label: 'Granite (20mm Aggregate)'
          },
          {
            key: 'construction_material_ready_mixed_concrete',
            label: 'Ready Mixed Concrete'
          }
        ];
        return targets.map((target) => {
          const match = findSeriesRow(records, sf, target.label);
          if (!match.row) throw new Error(`${target.label}: ${match.error || 'series not found'}`);
          if (VERIFY_MODE) {
            console.log(`[verify-series] datasetId=${datasetId} required=${target.label} matched=${match.matchedSeriesName} (${match.matchType})`);
          }
          return {
            key: target.key,
            freq: inferFrequencyFromTimeFields(tf),
            units: 'index',
            latest: extractLatest(match.row, tf),
            row: match.row,
            metadata: { source_series_name: match.matchedSeriesName }
          };
        });
      }
    },
    {
      datasetId: 'd_ba3c493ad160125ce347d5572712f14f', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const targets = [
          { key: 'demand_construction_materials_cement', label: 'Cement' },
          { key: 'demand_construction_materials_steel_reinforcement_bars', label: 'Steel Reinforcement Bars' },
          { key: 'demand_construction_materials_granite', label: 'Granite' },
          { key: 'demand_construction_materials_ready_mixed_concrete', label: 'Ready-Mixed Concrete' }
        ];
        return targets.map((target) => {
          const match = findSeriesRow(records, sf, target.label);
          if (!match.row) throw new Error(`${target.label}: ${match.error || 'series not found'}`);
          if (VERIFY_MODE) {
            console.log(`[verify-series] datasetId=${datasetId} required=${target.label} matched=${match.matchedSeriesName} (${match.matchType})`);
          }
          return {
            key: target.key,
            freq: inferFrequencyFromTimeFields(tf),
            units: 'index',
            latest: extractLatest(match.row, tf),
            row: match.row,
            metadata: { datasetId, matched_series_label: match.matchedSeriesName }
          };
        });
      }
    },
    {
      datasetId: 'd_055b6549444dedb341c50805d9682a41', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const sorted = sortRecordsStable(records);
        const anchorLabel = 'Total Non-Landed Properties';
        const expected = ['Under Construction', 'Planned - Written Permission', 'Planned - Provisional Permission', 'Planned - Others'];
        const anchorIndex = sorted.findIndex((row) => labelOf(row, sf) === anchorLabel);
        if (anchorIndex === -1) throw new Error(`anchor not found: ${anchorLabel}`);
        const { totalRow, componentRows } = verifyAdjacentComponentBlock(sorted, sf, anchorIndex, expected, datasetId, anchorLabel);
        const { check } = latestPeriodAndSumCheck({ datasetId, timeFields: tf, totalRow, componentRows, seriesField: sf, absTol: 1, relTol: 1e-3 });
        const status = check.pass ? 'ok' : 'ok_with_warning';
        const keys = [
          'prp_pipeline_total_non_landed',
          'prp_pipeline_non_landed_under_construction',
          'prp_pipeline_non_landed_planned_written_permission',
          'prp_pipeline_non_landed_planned_provisional_permission',
          'prp_pipeline_non_landed_planned_others'
        ];
        const rows = [totalRow, ...componentRows];
        return rows.map((row, idx) => ({
          key: keys[idx],
          freq: inferFrequencyFromTimeFields(tf),
          units: 'units',
          latest: extractLatest(row, tf),
          row,
          metadata: { source_series_name: row[sf], check, status }
        }));
      }
    },
    {
      datasetId: 'd_e47c0f0674b46981c4994d5257de5be4', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const sorted = sortRecordsStable(records);
        const expected = ['Under Construction', 'Planned - Written Permission', 'Planned - Provisional Permission', 'Planned - Others'];
        const categories = [
          {
            total: 'Total Office Space',
            category: 'office',
            keys: ['commind_office_total', 'commind_office_under_construction', 'commind_office_planned_written_permission', 'commind_office_planned_provisional_permission', 'commind_office_planned_others']
          },
          {
            total: 'Total Business Park Space',
            category: 'business_park',
            keys: ['commind_business_park_total', 'commind_business_park_under_construction', 'commind_business_park_planned_written_permission', 'commind_business_park_planned_provisional_permission', 'commind_business_park_planned_others']
          },
          {
            total: 'Total Retail Space',
            category: 'retail',
            keys: ['commind_retail_total', 'commind_retail_under_construction', 'commind_retail_planned_written_permission', 'commind_retail_planned_provisional_permission', 'commind_retail_planned_others']
          }
        ];

        const out = [];
        for (const cat of categories) {
          try {
            const anchorIndex = sorted.findIndex((row) => labelOf(row, sf) === cat.total);
            if (anchorIndex === -1) throw new Error(`anchor not found: ${cat.total}`);
            const { totalRow, componentRows } = verifyAdjacentComponentBlock(sorted, sf, anchorIndex, expected, datasetId, cat.total);
            const { check } = latestPeriodAndSumCheck({ datasetId, timeFields: tf, totalRow, componentRows, seriesField: sf, absTol: 1, relTol: 1e-3 });
            const status = check.pass ? 'ok' : 'ok_with_warning';
            [totalRow, ...componentRows].forEach((row, idx) => {
              out.push({
                key: cat.keys[idx],
                freq: inferFrequencyFromTimeFields(tf),
                units: 'units',
                latest: extractLatest(row, tf),
                row,
                metadata: { source_series_name: row[sf], parent_total: cat.total, category: cat.category, check, status }
              });
            });
          } catch (err) {
            console.warn(`[warn] dataset=${datasetId} category=${cat.category} block extraction failed: ${summarizeError(err)}`);
          }
        }
        if (!out.length) throw new Error('all category blocks failed');
        return out;
      }
    },
    {
      datasetId: 'd_4dca06508cd9d0a8076153443c17ea5f', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const labels = ['Total', 'Under Construction', 'Written Permission', 'Provisional Permission', 'Others'];
        const rows = labels.map((label) => {
          const match = findSeriesRow(records, sf, label);
          if (!match.row || match.matchType !== 'exact') throw new Error(`required exact series missing: ${label}`);
          return match.row;
        });
        const totalRow = rows[0];
        const componentRows = rows.slice(1);
        const { check } = latestPeriodAndSumCheck({ datasetId, timeFields: tf, totalRow, componentRows, seriesField: sf, absTol: 1, relTol: 1e-3 });
        const status = check.pass ? 'ok' : 'ok_with_warning';
        const keys = ['industrial_pipeline_total', 'industrial_pipeline_under_construction', 'industrial_pipeline_written_permission', 'industrial_pipeline_provisional_permission', 'industrial_pipeline_others'];
        return rows.map((row, idx) => ({
          key: keys[idx],
          freq: inferFrequencyFromTimeFields(tf),
          units: 'sqm',
          latest: extractLatest(row, tf),
          row,
          metadata: { source_series_name: row[sf], check, status }
        }));
      }
    },
    {
      datasetId: 'd_e9cc9d297b1cf8024cf99db4b12505cc', source: 'data.gov.sg',
      build: (records, sf, tf, datasetId) => {
        const targets = [
          { key: 'private_vacant_private_sector_office_space_vacant', label: 'Private Sector Office Space Vacant' },
          { key: 'private_vacant_private_sector_business_park_space_vacant', label: 'Private Sector Business Park Space Vacant' },
          { key: 'private_vacant_private_sector_multiple_user_factory_space_vacant', label: 'Private Sector Multiple-User Factory Space Vacant' },
          { key: 'private_vacant_private_sector_retail_space_vacant', label: 'Private Sector Retail Space Vacant' }
        ];
        return targets.map((target) => {
          const match = findSeriesRow(records, sf, target.label);
          if (!match.row) throw new Error(`${target.label}: ${match.error || 'series not found'}`);
          if (VERIFY_MODE) {
            console.log(`[verify-series] datasetId=${datasetId} required=${target.label} matched=${match.matchedSeriesName} (${match.matchType})`);
          }
          return {
            key: target.key,
            freq: inferFrequencyFromTimeFields(tf),
            units: 'sqm',
            latest: extractLatest(match.row, tf),
            row: match.row,
            metadata: { source_series_name: match.matchedSeriesName }
          };
        });
      }
    },

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
      const entries = spec.build(records, seriesField, timeFields, spec.datasetId);
      if (!entries.length) throw new Error('no matching series');
      let latestSeen = null;
      for (const entry of entries) {
        if (!entry.latest) continue;
        latestSeen = entry.latest;
        if (!verifyOnly) {
          const mergedValues = mergePeriodHistory(existingSeries[entry.key]?.values, extractSeriesValues(entry.row, timeFields));
          series[entry.key] = {
            freq: entry.freq,
            latest_period: entry.latest.latest_period,
            latest_value: entry.latest.latest_value,
            units: entry.units,
            values: mergedValues,
            ...(entry.metadata || {})
          };
        }
      }
      if (!latestSeen) throw new Error('0 time columns detected');
      return { latest_period: latestSeen.latest_period, latest_value: latestSeen.latest_value };
    });
  }

  await tryIndicator({ key: 'construction_gdp', source: 'SingStat TableBuilder', dataset_ref: SINGSTAT_CONSTRUCTION_GDP_DATASET_REF, series_name: 'Construction' }, async () => {
    const bundle = await fetchConstructionGdpSeries();
    const construction = bundle.CONSTRUCTION_GDP_SA;
    if (!construction?.latest) throw new Error('SingStat parse 0 rows for construction GDP');
    if (!verifyOnly) {
      const latestQuarterPeriod = isoDateToQuarterPeriod(construction.latest.date);
      if (!latestQuarterPeriod) throw new Error(`Unable to convert construction GDP date ${construction.latest.date} to quarter period`);
      series.construction_gdp = {
        freq: 'Q',
        latest_period: latestQuarterPeriod,
        latest_value: construction.latest.value,
        units: 'S$ million',
        source_series_name: construction.rows[0]?.series_name || 'Construction',
        values: construction.rows.map((row) => ({ period: isoDateToQuarterPeriod(row.date), value: row.value })).filter((row) => row.period)
      };
    }
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetRef=${SINGSTAT_CONSTRUCTION_GDP_DATASET_REF} required=${construction.rows[0]?.series_name || 'Construction'} latest=${construction.latest.date}=${construction.latest.value}`);
    }
    return { latest_period: construction.latest.date, latest_value: construction.latest.value };
  });

  let singStatUnitLabourPromise;
  const getSingStatUnitLabourBundle = async () => {
    if (!singStatUnitLabourPromise) singStatUnitLabourPromise = fetchUnitLabourCostConstructionSeries();
    return singStatUnitLabourPromise;
  };

  await tryIndicator({ key: 'unit_labour_cost_construction', source: 'SingStat TableBuilder', dataset_ref: SINGSTAT_UNIT_LABOUR_DATASET_REF, series_name: 'Unit labour cost of construction' }, async () => {
    const bundle = await getSingStatUnitLabourBundle();
    const unit = bundle.UNIT_LABOUR_COST_CONSTRUCTION;
    if (!unit?.latest) throw new Error('SingStat parse 0 rows for unit labour cost of construction');
    if (!verifyOnly) {
      series.unit_labour_cost_construction = {
        freq: 'M',
        latest_period: unit.latest.date,
        latest_value: unit.latest.value,
        units: 'index',
        values: unit.rows.map((row) => ({ period: row.date.slice(0, 7), value: row.value }))
      };
    }
    return { latest_period: unit.latest.date, latest_value: unit.latest.value };
  });

  await tryIndicator({ key: 'sora_overnight', source: 'SingStat TableBuilder', dataset_ref: SINGSTAT_RATES_DATASET_REF, series_name: 'Singapore Overnight Rate Average' }, async () => {
    const rates = await getSingStatBundle();
    if (!rates.sora_overnight.values.length) throw new Error('SingStat parse 0 rows for SORA');
    const latest = rates.sora_overnight.values[rates.sora_overnight.values.length - 1];
    if (!verifyOnly) {
      series.sora_overnight = rates.sora_overnight;
    }
    if (VERIFY_MODE) {
      console.log(`[verify-series] datasetRef=${rates.datasetRef} required=${rates.matchedLabels.SORA} latest=${latest.date}=${latest.value}`);
    }
    return { latest_period: latest.date, latest_value: latest.value };
  });

  let masI6ApiPromise;
  const getMasI6Api = async () => {
    if (!masI6ApiPromise) masI6ApiPromise = fetchMasI6LoanLimits({ verifyMode: VERIFY_MODE });
    return masI6ApiPromise;
  };

  await tryIndicator({ key: 'loan_limits_granted_building_construction', source: 'MAS API I.6', dataset_ref: MAS_I6_API_URL }, async () => {
    const i6 = await getMasI6Api();
    const merged = mergeMonthlyHistory(existingSeries.loan_limits_granted_building_construction?.values, i6.grantedValues);
    const grantedLatest = merged.merged[merged.merged.length - 1];
    if (!grantedLatest) throw new Error('0 monthly values extracted');
    if (!verifyOnly) {
      series.loan_limits_granted_building_construction = {
        freq: 'M',
        latest_period: grantedLatest.period,
        latest_value: grantedLatest.value,
        units: 'S$ million',
        values: merged.merged
      };
    }
    if (VERIFY_MODE) {
      console.log(`[OK] loan_limits_granted_building_construction latest_period=${grantedLatest.period} latest_value=${grantedLatest.value} prelim=${Boolean(grantedLatest.prelim)}`);
      console.log(`[verify-mas-api-i6] granted counts extracted_rows=${i6.extractedRowCount} merged_updated=${merged.updated} merged_appended=${merged.appended}`);
    }
    return { latest_period: grantedLatest.period, latest_value: grantedLatest.value };
  });

  await tryIndicator({ key: 'loan_limits_utilised_building_construction', source: 'MAS API I.6', dataset_ref: MAS_I6_API_URL }, async () => {
    const i6 = await getMasI6Api();
    const merged = mergeMonthlyHistory(existingSeries.loan_limits_utilised_building_construction?.values, i6.utilisedValues);
    const utilisedLatest = merged.merged[merged.merged.length - 1];
    if (!utilisedLatest) throw new Error('0 monthly values extracted');
    if (!verifyOnly) {
      series.loan_limits_utilised_building_construction = {
        freq: 'M',
        latest_period: utilisedLatest.period,
        latest_value: utilisedLatest.value,
        units: '%',
        values: merged.merged
      };
    }
    if (VERIFY_MODE) {
      console.log(`[OK] loan_limits_utilised_building_construction latest_period=${utilisedLatest.period} latest_value=${utilisedLatest.value} prelim=${Boolean(utilisedLatest.prelim)}`);
      console.log(`[verify-mas-api-i6] utilised counts extracted_rows=${i6.extractedRowCount} merged_updated=${merged.updated} merged_appended=${merged.appended}`);
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
    const existing = JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
    const { updateRun } = await buildMacroIndicators(true, existing?.macro_indicators?.series || {});
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
  const { series: fetchedSeries, updateRun, results } = await buildMacroIndicators(false, existingSeries);

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
      {
        name: 'SingStat TableBuilder',
        method: 'json_api_parse',
        table_ids: [SINGSTAT_RATES_TABLE_ID, SINGSTAT_UNIT_LABOUR_TABLE_ID, SINGSTAT_CONSTRUCTION_GDP_TABLE_ID],
        table_urls: [
          `https://tablebuilder.singstat.gov.sg/table/${SINGSTAT_RATES_TABLE_ID}`,
          `https://tablebuilder.singstat.gov.sg/table/${SINGSTAT_UNIT_LABOUR_TABLE_ID}`,
          `https://tablebuilder.singstat.gov.sg/table/${SINGSTAT_CONSTRUCTION_GDP_TABLE_ID}`
        ]
      },
      { name: 'MAS I.6 JSON API', method: 'json_api_parse', url: MAS_I6_API_URL }
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
