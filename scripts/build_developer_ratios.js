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

const ROOT = path.resolve(__dirname, '..');
const INPUT_CSV = path.join(ROOT, 'data', 'listed developer list.csv');
const OUTPUT_JSON = path.join(ROOT, 'data', 'processed', 'developer_ratios_history.json');
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
    fetchError: null
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

async function main() {
  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.mkdir(CACHE_DIR, { recursive: true });

  const developers = await readDeveloperCsv(INPUT_CSV);
  const output = {
    updatedAt: nowIso(),
    source: 'stockanalysis',
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

  console.log(`[stockanalysis:write-output] ${JSON.stringify({
    filePathWritten: OUTPUT_JSON,
    developersProcessed: output.developers.length,
    fetchStatusCounts: statusCounts
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
  main
};
