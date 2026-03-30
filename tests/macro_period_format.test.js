const test = require('node:test');
const assert = require('node:assert/strict');

const {
  parseYearMonth,
  toQuarterLabel,
  formatLastPointLabel,
  inferFrequencyFromSeriesValues,
  resolveSeriesFrequency,
  filterSparklineSeries
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
  assert.deepEqual(parseYearMonth('2025-10'), { year: 2025, month: 10 });
  assert.deepEqual(parseYearMonth('2025 Q4'), { year: 2025, month: 12 });
  assert.deepEqual(parseYearMonth('2025Q4'), { year: 2025, month: 12 });
  assert.equal(parseYearMonth('2025 October'), null);
  assert.equal(toQuarterLabel({ year: 2025, month: 10 }), '2025 Q4');
});

test('formatLastPointLabel preserves quarterly format for quarter tokens from latest data', () => {
  const result = formatLastPointLabel(
    'unit_labour_cost_construction',
    '2025 Q4',
    { freq: 'Q' },
    [{ period: '2025 Q4' }]
  );
  assert.equal(result, '2025 Q4');
});

test('fallback whitelist only marks configured IDs as quarterly when inference unavailable', () => {
  assert.equal(resolveSeriesFrequency('unit_labour_cost_construction', {}, [{ period: '2025 Oct' }]), 'Q');
  assert.equal(resolveSeriesFrequency('some_other_series', {}, [{ period: '2025 Oct' }]), 'M');
});

test('construction GDP sparkline drops 2021 Q2 and earlier before applying moving window', () => {
  const filtered = filterSparklineSeries('construction_gdp', [
    { rawDate: '2021Q1', date: '2021-03-01', value: -1 },
    { rawDate: '2021Q2', date: '2021-06-01', value: -2 },
    { rawDate: '2021Q3', date: '2021-09-01', value: 3 },
    { rawDate: '2021Q4', date: '2021-12-01', value: 4 },
    { rawDate: '2022Q1', date: '2022-03-01', value: 5 }
  ]);

  assert.deepEqual(
    filtered.map((point) => point.rawDate),
    ['2021Q3', '2021Q4', '2022Q1']
  );

  const untouched = filterSparklineSeries('some_other_series', [
    { rawDate: '2021Q1', date: '2021-03-01', value: 1 },
    { rawDate: '2021Q2', date: '2021-06-01', value: 2 }
  ]);
  assert.equal(untouched.length, 2);
});
