const test = require('node:test');
const assert = require('node:assert/strict');
const { toTidyRowsFromWide, matchRequiredSeries } = require('../scripts/lib/singstat_tablebuilder');

test('converts wide table layout to tidy long rows and matches exact required series', () => {
  const wide = [
    {
      'Data Series': 'Government Securities - 2-Year Bond Yield',
      '2025 Dec': '2.88',
      '2026 Jan': '2.91'
    },
    {
      'Data Series': 'Government Securities - 10-Year Bond Yield',
      '2025 Dec': '2.95',
      '2026 Jan': '2.98'
    },
    {
      'Data Series': 'Singapore Overnight Rate Average',
      '2025 Dec': '2.77',
      '2026 Jan': '2.79'
    },
    {
      'Data Series': 'Government Securities - 5-Year Bond Yield',
      '2025 Dec': '2.50',
      '2026 Jan': '2.55'
    }
  ];

  const tidy = toTidyRowsFromWide(wide);
  assert.equal(tidy.length, 8);
  assert.deepEqual(tidy[0], { date: '2025-12-01', series_name: 'Government Securities - 10-Year Bond Yield', value: 2.95 });

  const selected = matchRequiredSeries(tidy);
  assert.equal(selected.SGS_2Y.length, 2);
  assert.equal(selected.SGS_10Y.length, 2);
  assert.equal(selected.SORA.length, 2);

  const labels = new Set([
    selected.SGS_2Y[0].series_name,
    selected.SGS_10Y[0].series_name,
    selected.SORA[0].series_name
  ]);
  assert.ok(!labels.has('Government Securities - 5-Year Bond Yield'));
});
