const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { parseRatiosTable } = require('../scripts/lib/developer_ratios');

test('parseRatiosTable parses current and FY values with aliases', () => {
  const html = fs.readFileSync(path.join(__dirname, 'fixtures', 'stockanalysis_ratios_9CI_sample.html'), 'utf8');
  const parsed = parseRatiosTable(html);

  assert.equal(parsed.periods[0].label, 'Current');
  assert.equal(parsed.periods[1].label, 'FY 2025');
  assert.equal(parsed.periods[0].periodEnding, '2026-02-23');

  assert.equal(parsed.metrics.marketCap.values.Current, 15562);
  assert.equal(parsed.metrics.marketCap.values['FY 2025'], 13516);
  assert.equal(parsed.metrics.roe.values.Current, 12.3);
  assert.equal(parsed.metrics.payoutRatio.values.Current, null);
  assert.equal(parsed.current.marketCap, 15562);
  assert.equal(parsed.current.debtToEquity, 0.64);
});
