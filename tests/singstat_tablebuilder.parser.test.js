const fs = require('fs');
const path = require('path');
const test = require('node:test');
const assert = require('node:assert/strict');
const {
  parseTableBuilderPivotJson,
  matchRequiredSeries,
  normalizeLabel,
  parseMonthLabel,
  isoDateToQuarterPeriod
} = require('../scripts/lib/singstat_tablebuilder');


test('parseMonthLabel supports monthly, quarterly, and yearly period labels', () => {
  assert.equal(parseMonthLabel('2025 Dec'), '2025-12-01');
  assert.equal(parseMonthLabel('2025 Q4'), '2025-10-01');
  assert.equal(parseMonthLabel('Q1 2026'), '2026-01-01');
  assert.equal(parseMonthLabel('2025 3Q'), '2025-07-01');
  assert.equal(parseMonthLabel('2024'), '2024-01-01');
  assert.equal(parseMonthLabel('invalid'), null);
});


test('isoDateToQuarterPeriod converts monthly ISO dates to YYYYQn', () => {
  assert.equal(isoDateToQuarterPeriod('2025-10-01'), '2025Q4');
  assert.equal(isoDateToQuarterPeriod('2026-01-01'), '2026Q1');
  assert.equal(isoDateToQuarterPeriod('invalid'), null);
});

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

test('parses nested period cell payloads where values are objects instead of direct month columns', () => {
  const payload = {
    Data: [
      {
        rowText: 'Government Securities - 2-Year Bond Yield',
        cells: [
          { period: '2025 Dec', value: '2.88' },
          { period: '2026 Jan', value: '2.91' }
        ]
      },
      {
        rowText: 'Government Securities - 10-Year Bond Yield',
        cells: [
          { period: '2025 Dec', value: '2.95' },
          { period: '2026 Jan', value: '2.98' }
        ]
      },
      {
        rowText: 'Singapore Overnight Rate Average',
        cells: [
          { period: '2025 Dec', value: '2.77' },
          { period: '2026 Jan', value: '2.79' }
        ]
      }
    ],
    metaData: { columns: [{ label: '2026 Jan' }, { label: '2025 Dec' }] }
  };

  const tidy = parseTableBuilderPivotJson(payload);
  assert.equal(tidy.length, 6);

  const selected = matchRequiredSeries(tidy, [
    { key: 'SORA', label: 'Singapore Overnight Rate Average', normalized: normalizeLabel('Singapore Overnight Rate Average') },
    { key: 'SGS_2Y', label: 'Government Securities - 2-Year Bond Yield', normalized: normalizeLabel('Government Securities - 2-Year Bond Yield') },
    { key: 'SGS_10Y', label: 'Government Securities - 10-Year Bond Yield', normalized: normalizeLabel('Government Securities - 10-Year Bond Yield') }
  ]);

  assert.equal(selected.SGS_2Y.length, 2);
  assert.equal(selected.SGS_10Y.length, 2);
  assert.equal(selected.SORA.length, 2);
});


test('parses quarterly period columns when SingStat table does not expose YYYY Mon columns', () => {
  const payload = {
    Data: [
      {
        rowText: 'Unit labour cost of construction',
        '2024 Q4': '5501.7',
        '2025 Q1': '5532.1'
      }
    ]
  };

  const tidy = parseTableBuilderPivotJson(payload);
  assert.deepEqual(tidy, [
    { date: '2024-10-01', series_name: 'Unit labour cost of construction', value: 5501.7 },
    { date: '2025-01-01', series_name: 'Unit labour cost of construction', value: 5532.1 }
  ]);
});
