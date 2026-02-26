#!/usr/bin/env node
const fs = require('fs/promises');
const path = require('path');
const {
  METRIC_SCHEMA,
  nowIso,
  parseRatiosTable,
  fetchWithRetry,
  readDeveloperCsv,
  emptyMetrics,
  buildCurrent,
  countParsedMetrics,
  normalizeTicker,
  sanitizeHtmlSnippet,
  buildRatiosUrl
} = require('./lib/developer_ratios');
const {
  BASE_WEIGHTS,
  NEGATIVE_LEVERAGE_SUPPORT,
  SCORE_COVERAGE_MIN,
  STATUS_BANDS,
  TREND_PENALTY_CAP,
  computeHealthScore
} = require('./lib/developer_health_score');

const ROOT = path.resolve(__dirname, '..');
const INPUT_CSV = path.join(ROOT, 'data', 'listed developer list.csv');
const OUTPUT_JSON = path.join(ROOT, 'data', 'processed', 'developer_ratios_history.json');
const DIAGNOSTICS_JSON = path.join(ROOT, 'data', 'processed', 'developer_health_diagnostics.json');
const CACHE_DIR = path.join(ROOT, 'data', 'cache', 'stockanalysis');
const DEBUG_DIR = path.join(ROOT, 'data', 'debug', 'stockanalysis');

async function processDeveloper(developer, { verbose = false } = {}) {
  await fs.mkdir(CACHE_DIR, { recursive: true });
  const tickerRaw = developer.ticker;
  const ticker = normalizeTicker(tickerRaw);
  const ratiosUrl = developer.stockanalysis_ratios_url || buildRatiosUrl(ticker);
  const debugReport = {
    tickerRaw,
    normalizedTicker: ticker,
    generatedRatiosUrl: ratiosUrl,
    fetch: null,
    dom: null,
    metricRows: [],
    output: null,
    fetchStatus: 'error',
    fetchError: null
  };

  const record = {
    ticker,
    name: developer.name,
    segment: developer.segment,
    stockanalysis_ratios_url: ratiosUrl,
    periods: [],
    metrics: emptyMetrics(),
    current: buildCurrent(emptyMetrics()),
    lastFetchedAt: nowIso(),
    fetchStatus: 'error',
    fetchError: null,
    healthScore: null,
    healthStatus: 'Pending data',
    scoreCoverage: 0,
    scoreNote: 'Insufficient ratio coverage',
    healthScoreComponents: null,
    lastScoredAt: null
  };

  const stageLog = (stage, details = {}) => {
    if (!verbose) return;
    console.log(`[stockanalysis:${ticker}] ${stage} ${JSON.stringify(details)}`);
  };

  stageLog('url-generation', {
    tickerRaw,
    normalizedTicker: ticker,
    generatedRatiosUrl: ratiosUrl
  });

  try {
    const fetchStartedAt = nowIso();
    stageLog('fetch-request-start', { at: fetchStartedAt, url: ratiosUrl });

    const fetchResult = await fetchWithRetry(ratiosUrl, { retries: 2, logger: console });
    const html = fetchResult.html;
    const htmlSnippet = sanitizeHtmlSnippet(html, 300);

    debugReport.fetch = {
      startedAt: fetchStartedAt,
      statusCode: fetchResult.status,
      contentType: fetchResult.contentType,
      finalUrl: fetchResult.finalUrl,
      htmlLength: html.length,
      htmlSnippet,
      expectedTextPresence: {
        ratiosAndMetrics: /Ratios and Metrics/i.test(html),
        fiscalYear: /Fiscal Year/i.test(html),
        marketCapitalization: /Market Capitalization/i.test(html)
      }
    };

    stageLog('fetch-response', debugReport.fetch);

    await fs.writeFile(path.join(CACHE_DIR, `${ticker}.html`), html, 'utf8');

    const parserDebug = {};
    const parsed = parseRatiosTable(html, {
      debug: parserDebug,
      log: (message) => stageLog('parse-warning', { message })
    });

    debugReport.dom = parserDebug.dom;
    debugReport.metricRows = parserDebug.metricRows || [];

    const parsedCount = countParsedMetrics(parsed.metrics);
    record.periods = parsed.periods;
    record.metrics = parsed.metrics;
    record.current = parsed.current;
    record.fetchStatus = parsedCount >= Math.floor(Object.keys(METRIC_SCHEMA).length * 0.8) ? 'ok' : 'partial';
    debugReport.fetchStatus = record.fetchStatus;
    await fs.writeFile(path.join(CACHE_DIR, `${ticker}.json`), JSON.stringify(parsed, null, 2), 'utf8');

    const missingMetrics = Object.keys(METRIC_SCHEMA).filter((key) => record.current[key] == null);
    debugReport.output = {
      periodsFound: parsed.periods.map((period) => period.label),
      metricsMatchedCount: parsedCount,
      missingMetrics
    };

    stageLog('dom-parse', debugReport.dom || {});
    stageLog('metric-extraction', { rows: debugReport.metricRows });
    stageLog('parse-summary', { fetchStatus: record.fetchStatus, ...debugReport.output });
  } catch (error) {
    record.fetchStatus = 'error';
    record.fetchError = error?.message || String(error);
    debugReport.fetchStatus = 'error';
    debugReport.fetchError = error?.message || String(error);
    stageLog('error', { message: error?.message || String(error), context: error?.context || null });
  }


  const scoring = computeHealthScore(record);
  record.healthScore = scoring.healthScore;
  record.healthStatus = scoring.healthStatus;
  record.scoreCoverage = scoring.scoreCoverage;
  record.scoreNote = scoring.scoreNote;
  record.healthScoreComponents = scoring.healthScoreComponents;
  record.lastScoredAt = scoring.lastScoredAt;

  if (process.env.DEBUG_STOCKANALYSIS === 'true') {
    await fs.mkdir(DEBUG_DIR, { recursive: true });
    if (debugReport.fetch?.htmlLength) {
      const html = await fs.readFile(path.join(CACHE_DIR, `${ticker}.html`), 'utf8');
      await fs.writeFile(path.join(DEBUG_DIR, `${ticker}.html`), html.slice(0, 2_000_000), 'utf8');
    }
    await fs.writeFile(path.join(DEBUG_DIR, `${ticker}.json`), JSON.stringify(debugReport, null, 2), 'utf8');
  }

  return { record, debugReport };
}


function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const idx = (sortedValues.length - 1) * p;
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  if (lower === upper) return sortedValues[lower];
  const weight = idx - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function topContributors(record, limit = 3) {
  const contributors = record?.healthScoreComponents?.weightedContributors || {};
  return Object.entries(contributors)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([metric, weightedRiskContribution]) => ({
      metric,
      weightedRiskContribution: Number(weightedRiskContribution.toFixed(3)),
      riskScore: record?.healthScoreComponents?.riskByMetric?.[metric] ?? null
    }));
}

function summarizeDeveloper(record) {
  return {
    ticker: record.ticker,
    name: record.name,
    segment: record.segment || null,
    healthScore: record.healthScore,
    healthStatus: record.healthStatus,
    scoreCoverage: record.scoreCoverage,
    topRiskContributors: topContributors(record, 3)
  };
}

function buildDiagnostics(developers = []) {
  const coverage = { gte0_8: 0, gte0_5_lt0_8: 0, lt0_5: 0 };
  const statusCounts = { Green: 0, Amber: 0, Red: 0, 'Pending data': 0 };

  for (const dev of developers) {
    const c = Number.isFinite(dev.scoreCoverage) ? dev.scoreCoverage : 0;
    if (c >= 0.8) coverage.gte0_8 += 1;
    else if (c >= SCORE_COVERAGE_MIN) coverage.gte0_5_lt0_8 += 1;
    else coverage.lt0_5 += 1;

    const status = dev.healthStatus || 'Pending data';
    statusCounts[status] = (statusCounts[status] || 0) + 1;
  }

  const scored = developers.filter((d) => Number.isFinite(d.healthScore));
  const sortedScores = scored.map((d) => d.healthScore).sort((a, b) => a - b);

  const distribution = sortedScores.length
    ? {
        min: sortedScores[0],
        p10: Number(percentile(sortedScores, 0.1).toFixed(2)),
        p25: Number(percentile(sortedScores, 0.25).toFixed(2)),
        median: Number(percentile(sortedScores, 0.5).toFixed(2)),
        p75: Number(percentile(sortedScores, 0.75).toFixed(2)),
        p90: Number(percentile(sortedScores, 0.9).toFixed(2)),
        max: sortedScores[sortedScores.length - 1]
      }
    : null;

  const lowest = [...scored].sort((a, b) => a.healthScore - b.healthScore).slice(0, 10).map(summarizeDeveloper);
  const highest = [...scored].sort((a, b) => b.healthScore - a.healthScore).slice(0, 10).map(summarizeDeveloper);

  const hasSegment = developers.some((d) => d.segment);
  const segmentBreakdown = hasSegment
    ? Object.values(developers.reduce((acc, dev) => {
        const key = dev.segment || 'Unspecified';
        if (!acc[key]) acc[key] = { segment: key, count: 0, scores: [] };
        acc[key].count += 1;
        if (Number.isFinite(dev.healthScore)) acc[key].scores.push(dev.healthScore);
        return acc;
      }, {})).map((entry) => ({
        segment: entry.segment,
        count: entry.count,
        medianHealthScore: entry.scores.length
          ? Number(percentile(entry.scores.sort((a, b) => a - b), 0.5).toFixed(2))
          : null
      }))
    : null;

  return {
    generatedAt: nowIso(),
    totalDevelopers: developers.length,
    coverageDistribution: coverage,
    healthScoreDistribution: distribution,
    statusCounts,
    lowestScores: lowest,
    highestScores: highest,
    segmentBreakdown
  };
}

async function main() {
  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.mkdir(CACHE_DIR, { recursive: true });

  const developers = await readDeveloperCsv(INPUT_CSV);
  const output = {
    updatedAt: nowIso(),
    source: 'stockanalysis',
    scoringModel: {
      formula: 'round(clamp((100 - weightedRisk) - trendPenalty, 0, 100))',
      weightedRisk: 'sum(metricWeight * metricRisk) / sum(availableMetricWeight)',
      weights: { ...BASE_WEIGHTS },
      coverageThreshold: SCORE_COVERAGE_MIN,
      trendPenaltyCap: TREND_PENALTY_CAP,
      negativeLeverageHandling: {
        targetMetrics: [...NEGATIVE_LEVERAGE_SUPPORT.targetMetrics],
        supportMetrics: [...NEGATIVE_LEVERAGE_SUPPORT.supportMetrics],
        supportBands: {
          strongMinHealthScore: NEGATIVE_LEVERAGE_SUPPORT.strongMinHealthScore,
          mixedMinHealthScore: NEGATIVE_LEVERAGE_SUPPORT.mixedMinHealthScore
        },
        riskFloors: {
          noSupport: NEGATIVE_LEVERAGE_SUPPORT.noSupportRiskFloor,
          mixedSupport: NEGATIVE_LEVERAGE_SUPPORT.mixedSupportRiskFloor,
          weakSupport: NEGATIVE_LEVERAGE_SUPPORT.weakSupportRiskFloor
        }
      },
      bands: {
        status: {
          green: STATUS_BANDS.green,
          amber: STATUS_BANDS.amber
        }
      },
      excludedMetrics: Object.entries(BASE_WEIGHTS)
        .filter(([, weight]) => !Number.isFinite(weight) || weight <= 0)
        .map(([metricKey]) => metricKey)
    },
    developers: []
  };

  const statusCounts = { ok: 0, partial: 0, error: 0 };

  for (const developer of developers) {
    const verboseDebug = process.env.DEBUG_STOCKANALYSIS === 'true';
    const { record } = await processDeveloper(developer, { verbose: verboseDebug });
    output.developers.push(record);
    statusCounts[record.fetchStatus] = (statusCounts[record.fetchStatus] || 0) + 1;
  }

  output.updatedAt = nowIso();
  await fs.writeFile(OUTPUT_JSON, JSON.stringify(output, null, 2), 'utf8');

  const diagnostics = buildDiagnostics(output.developers);
  await fs.writeFile(DIAGNOSTICS_JSON, JSON.stringify(diagnostics, null, 2), 'utf8');

  console.log(`[stockanalysis:write-output] ${JSON.stringify({
    filePathWritten: OUTPUT_JSON,
    diagnosticsPathWritten: DIAGNOSTICS_JSON,
    developersProcessed: output.developers.length,
    fetchStatusCounts: statusCounts,
    coverageDistribution: diagnostics.coverageDistribution,
    healthScoreDistribution: diagnostics.healthScoreDistribution,
    statusCounts: diagnostics.statusCounts
  })}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = {
  processDeveloper,
  main,
  buildDiagnostics
};
