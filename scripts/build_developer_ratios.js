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
  countParsedMetrics
} = require('./lib/developer_ratios');

const ROOT = path.resolve(__dirname, '..');
const INPUT_CSV = path.join(ROOT, 'data', 'listed developer list.csv');
const OUTPUT_JSON = path.join(ROOT, 'data', 'processed', 'developer_ratios_history.json');
const CACHE_DIR = path.join(ROOT, 'data', 'cache', 'stockanalysis');

async function main() {
  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.mkdir(CACHE_DIR, { recursive: true });

  const developers = await readDeveloperCsv(INPUT_CSV);
  const output = {
    updatedAt: nowIso(),
    source: 'stockanalysis',
    developers: []
  };

  for (const developer of developers) {
    const record = {
      ticker: developer.ticker,
      name: developer.name,
      segment: developer.segment,
      stockanalysis_ratios_url: developer.stockanalysis_ratios_url,
      periods: [],
      metrics: emptyMetrics(),
      current: buildCurrent(emptyMetrics()),
      lastFetchedAt: nowIso(),
      fetchStatus: 'error',
      fetchError: null
    };

    try {
      if (!developer.stockanalysis_ratios_url) throw new Error('Missing stockanalysis_ratios_url');
      const html = await fetchWithRetry(developer.stockanalysis_ratios_url, { retries: 2, logger: console });
      await fs.writeFile(path.join(CACHE_DIR, `${developer.ticker}.html`), html, 'utf8');

      const parsed = parseRatiosTable(html, (message) => {
        console.warn(`[parse-warning] ticker=${developer.ticker} url=${developer.stockanalysis_ratios_url} ${message}`);
      });

      record.periods = parsed.periods;
      record.metrics = parsed.metrics;
      record.current = parsed.current;
      const parsedCount = countParsedMetrics(parsed.metrics);
      record.fetchStatus = parsedCount >= Math.floor(Object.keys(METRIC_SCHEMA).length * 0.8) ? 'ok' : 'partial';
      await fs.writeFile(path.join(CACHE_DIR, `${developer.ticker}.json`), JSON.stringify(parsed, null, 2), 'utf8');
      console.log(`[${developer.ticker}] ${record.fetchStatus} currentMetrics=${parsedCount}`);
    } catch (error) {
      record.fetchStatus = 'error';
      record.fetchError = error.message;
      console.error(`[${developer.ticker}] error url=${developer.stockanalysis_ratios_url} ${error.message}`);
    }

    output.developers.push(record);
  }

  output.updatedAt = nowIso();
  await fs.writeFile(OUTPUT_JSON, JSON.stringify(output, null, 2), 'utf8');
  console.log(`Wrote ${OUTPUT_JSON}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
