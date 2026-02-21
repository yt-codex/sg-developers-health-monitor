const fs = require('fs');
const path = require('path');
const test = require('node:test');
const assert = require('node:assert/strict');
const {
  parseTableBuilderPivotJson,
  matchRequiredSeries,
  normalizeLabel
} = require('../scripts/lib/singstat_tablebuilder');

test('parses TableBuilder pivot JSON fixture into tidy rows and matches required labels strictly', () => {
  const fixturePath = path.join(__dirname, 'fixtures', 'singstat_tablebuilder_pivot_sample.json');
  const payload = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

  const tidy = parseTableBuilderPivotJson(payload);
  assert.equal(tidy.length, 14);
  assert.deepEqual(tidy[0], { date: '2025-12-01', series_name: 'Government Securities - 10-Year Bond Yield', value: 2.95 });

  const selected = matchRequiredSeries(tidy, [
    { key: 'SORA', label: 'Singapore Overnight Rate Average', normalized: normalizeLabel('Singapore Overnight Rate Average') },
    { key: 'SGS_2Y', label: 'Government Securities - 2-Year Bond Yield', normalized: normalizeLabel('Government Securities - 2-Year Bond Yield') },
    { key: 'SGS_10Y', label: 'Government Securities - 10-Year Bond Yield', normalized: normalizeLabel('Government Securities - 10-Year Bond Yield') }
  ]);

  assert.equal(selected.SGS_2Y.length, 3);
  assert.equal(selected.SGS_10Y.length, 3);
  assert.equal(selected.SORA.length, 2);

  const labels = new Set([
    selected.SGS_2Y[0].series_name,
    selected.SGS_10Y[0].series_name,
    selected.SORA[0].series_name
  ]);
  assert.ok(!labels.has('Government Securities - 5-Year Bond Yield'));

  const unitMatch = matchRequiredSeries(tidy, [
    {
      key: 'UNIT_LABOUR_COST_CONSTRUCTION',
      label: 'Unit labour cost of construction',
      pattern: /\bunit\b.*\blabou?r\b.*\bcost\b.*\bconstruction\b/
    }
  ]);
  assert.equal(unitMatch.UNIT_LABOUR_COST_CONSTRUCTION.length, 3);
});
