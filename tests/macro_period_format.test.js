const test = require('node:test');
const assert = require('node:assert/strict');

const {
  parseYearMonth,
  toQuarterLabel,
  formatLastPointLabel,
  inferFrequencyFromSeriesValues,
  resolveSeriesFrequency
} = require('../assets/js/macro.js');

test('monthly stays monthly when metadata indicates monthly', () => {
  const result = formatLastPointLabel(
    'monthly_series',
    '2025Oct',
    { freq: 'monthly' },
    [{ period: '2025Aug' }, { period: '2025Sep' }, { period: '2025Oct' }]
  );
  assert.equal(result, '2025 Oct');
});

test('quarterly converts to quarter label when metadata indicates quarterly', () => {
  const result = formatLastPointLabel(
    'unit_labour_cost_construction',
    '2025 Oct',
    { freq: 'Q' },
    [{ period: '2025 Apr' }, { period: '2025 Jul' }, { period: '2025 Oct' }]
  );
  assert.equal(result, '2025 Q4');
});

test('inference distinguishes monthly vs quarterly from month gaps', () => {
  const monthly = inferFrequencyFromSeriesValues([
    { period: '2025 Jun' },
    { period: '2025 Jul' },
    { period: '2025 Aug' },
    { period: '2025 Sep' },
    { period: '2025 Oct' }
  ]);
  assert.equal(monthly, 'M');

  const quarterly = inferFrequencyFromSeriesValues([
    { period: '2024 Jul' },
    { period: '2024 Oct' },
    { period: '2025 Jan' },
    { period: '2025 Apr' },
    { period: '2025 Jul' },
    { period: '2025 Oct' }
  ]);
  assert.equal(quarterly, 'Q');
});

test('strict period parsing and quarter mapping helpers', () => {
  assert.deepEqual(parseYearMonth('2025 Oct'), { year: 2025, month: 10 });
  assert.deepEqual(parseYearMonth('2025Oct'), { year: 2025, month: 10 });
  assert.equal(parseYearMonth('2025 October'), null);
  assert.equal(toQuarterLabel({ year: 2025, month: 10 }), '2025 Q4');
});

test('fallback whitelist only marks configured IDs as quarterly when inference unavailable', () => {
  assert.equal(resolveSeriesFrequency('unit_labour_cost_construction', {}, [{ period: '2025 Oct' }]), 'Q');
  assert.equal(resolveSeriesFrequency('some_other_series', {}, [{ period: '2025 Oct' }]), 'M');
});
