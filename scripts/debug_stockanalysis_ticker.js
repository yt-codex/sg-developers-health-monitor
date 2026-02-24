#!/usr/bin/env node
const { readDeveloperCsv, normalizeTicker } = require('./lib/developer_ratios');
const { processDeveloper } = require('./build_developer_ratios');

async function main() {
  const inputTicker = process.argv[2];
  if (!inputTicker) {
    throw new Error('Usage: npm run debug:stockanalysis -- <ticker>');
  }

  process.env.DEBUG_STOCKANALYSIS = 'true';
  const ticker = normalizeTicker(inputTicker);
  const developers = await readDeveloperCsv('data/listed developer list.csv');
  const match = developers.find((item) => normalizeTicker(item.ticker) === ticker);
  const target = match || { ticker, name: ticker, segment: '', stockanalysis_ratios_url: '' };

  const { record, debugReport } = await processDeveloper(target, { verbose: true });
  const missingMetrics = Object.keys(record.current).filter((key) => record.current[key] == null);
  console.log('[stockanalysis:compact-summary]', JSON.stringify({
    ticker: record.ticker,
    periodsFound: record.periods.map((p) => p.label),
    metricsMatchedCount: Object.values(record.current).filter((v) => v != null).length,
    missingMetrics,
    fetchStatus: record.fetchStatus,
    debugJson: `data/debug/stockanalysis/${record.ticker}.json`,
    debugHtml: `data/debug/stockanalysis/${record.ticker}.html`
  }));
  if (debugReport.fetchError) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
