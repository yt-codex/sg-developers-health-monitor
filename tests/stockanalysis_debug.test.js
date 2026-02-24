const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  normalizeTicker,
  parseNumeric,
  formatMarketCapCompact,
  findMetricKeyByLabel,
  parseRatiosTable,
  fetchWithRetry
} = require('../scripts/lib/developer_ratios');
const { processDeveloper } = require('../scripts/build_developer_ratios');

const fixtureHtml = fs.readFileSync(path.join(__dirname, 'fixtures', 'stockanalysis_ratios_9CI_sample.html'), 'utf8');

test('utility functions parse and normalize correctly', () => {
  assert.equal(normalizeTicker(' 9ci '), '9CI');
  assert.equal(parseNumeric('15,562'), 15562);
  assert.equal(parseNumeric('-19.00%'), -19);
  assert.equal(parseNumeric('-'), null);
  assert.equal(parseNumeric('N/A'), null);
  assert.equal(formatMarketCapCompact(15562), 'S$15.6B');
});

test('alias matching maps labels to canonical keys', () => {
  assert.equal(findMetricKeyByLabel('Debt / Equity Ratio'), 'debtToEquity');
  assert.equal(findMetricKeyByLabel('Net Debt / EBITDA'), 'netDebtToEbitda');
  assert.equal(findMetricKeyByLabel('Return on Invested Capital (ROIC)'), 'roic');
});

test('fixture parser parses periods and key rows while allowing missing metrics', () => {
  const parsed = parseRatiosTable(fixtureHtml);

  assert.deepEqual(parsed.periods.map((p) => p.label).slice(0, 3), ['Current', 'FY 2025', 'FY 2024']);
  assert.equal(parsed.periods[0].periodEnding, '2026-02-23');
  assert.equal(parsed.metrics.marketCap.values.Current, 15562);
  assert.equal(parsed.metrics.debtToEquity.values.Current, 0.64);
  assert.equal(parsed.metrics.roe.values.Current, 12.3);
  assert.equal(parsed.metrics.payoutRatio.values.Current, null);
  assert.doesNotThrow(() => parseRatiosTable(fixtureHtml));
});


test('parser classifies blocked/interstitial html distinctly', () => {
  const blockedHtml = '<html><head><title>Attention Required!</title></head><body><h1>Checking your browser before accessing stockanalysis.com</h1></body></html>';
  assert.throws(
    () => parseRatiosTable(blockedHtml),
    (error) => error.code === 'BLOCKED_INTERSTITIAL_CONTENT' && /blocked\/interstitial content detected/i.test(error.message)
  );
});

test('fetchWithRetry rotates request profile and keeps detailed context', async (t) => {
  const originalFetch = global.fetch;
  let callCount = 0;
  global.fetch = async (_url, options) => {
    callCount += 1;
    if (callCount === 1) {
      assert.match(options.headers['user-agent'], /Chrome/);
      return {
        ok: false,
        status: 403,
        url: 'https://stockanalysis.com/blocked',
        headers: { get: () => 'text/html' },
        text: async () => '<html>blocked</html>'
      };
    }
    assert.match(options.headers['user-agent'], /Safari/);
    return {
      ok: true,
      status: 200,
      url: 'https://stockanalysis.com/quote/sgx/9CI/financials/ratios/',
      headers: { get: () => 'text/html; charset=utf-8' },
      text: async () => fixtureHtml
    };
  };

  t.after(() => {
    global.fetch = originalFetch;
  });

  const result = await fetchWithRetry('https://example.test', { retries: 0, logger: { warn: () => {} } });
  assert.equal(result.status, 200);
  assert.equal(result.profile, 'fallback');
});

test('processDeveloper preserves original fetch error context', async (t) => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error('socket hang up');
  };

  t.after(() => {
    global.fetch = originalFetch;
  });

  const { record } = await processDeveloper({
    ticker: '9CI',
    name: 'CapitaLand Investment',
    segment: 'Mainboard',
    stockanalysis_ratios_url: 'https://example.test'
  }, { verbose: false });

  assert.equal(record.fetchStatus, 'error');
  assert.match(record.fetchError, /network\/request failure/);
  assert.match(record.fetchError, /socket hang up/);
  assert.match(record.fetchError, /context=/);
});

test('mocked fetch smoke test produces normalized output shape', async (t) => {
  const originalFetch = global.fetch;
  global.fetch = async () => ({
    ok: true,
    status: 200,
    url: 'https://stockanalysis.com/quote/sgx/9CI/financials/ratios/',
    headers: { get: () => 'text/html; charset=utf-8' },
    text: async () => fixtureHtml
  });
  t.after(() => {
    global.fetch = originalFetch;
  });

  const result = await fetchWithRetry('https://example.test');
  assert.equal(result.status, 200);
  assert.ok(result.html.includes('Market Capitalization'));

  process.env.DEBUG_STOCKANALYSIS = 'true';
  const { record } = await processDeveloper({
    ticker: '9CI',
    name: 'CapitaLand Investment',
    segment: 'Mainboard',
    stockanalysis_ratios_url: 'https://example.test'
  }, { verbose: false });

  assert.equal(record.ticker, '9CI');
  assert.equal(typeof record.current, 'object');
  assert.equal(Array.isArray(record.periods), true);
});
