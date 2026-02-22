const test = require('node:test');
const assert = require('node:assert/strict');

const {
  computeSectorPerformanceSignal,
  computeLabourCostSignal,
  computeInterestRateSignal
} = require('../scripts/lib/macro_stress_signals');

test('sector stress requires two consecutive negative quarters', () => {
  const stress = computeSectorPerformanceSignal([
    { period: '2025-Q1', value: -0.2 },
    { period: '2025-Q2', value: -1.1 }
  ]);
  assert.equal(stress.status, 'Stress');

  const normal = computeSectorPerformanceSignal([
    { period: '2025-Q1', value: -0.2 },
    { period: '2025-Q2', value: 0.1 }
  ]);
  assert.equal(normal.status, 'Normal');
});

test('ULC YoY stress requires >=8% for two consecutive quarters', () => {
  const stress = computeLabourCostSignal([
    { period: '2024-Q1', value: 100 },
    { period: '2024-Q2', value: 100 },
    { period: '2025-Q1', value: 109 },
    { period: '2025-Q2', value: 108 }
  ]);
  assert.equal(stress.status, 'Stress');
  assert.equal(stress.details.yoy_latest.period, '2025-Q2');
  assert.ok(stress.details.yoy_latest.value >= 0.08);
  assert.ok(stress.details.yoy_prev.value >= 0.08);

  const missingLag = computeLabourCostSignal([
    { period: '2024-Q2', value: 100 },
    { period: '2025-Q1', value: 109 },
    { period: '2025-Q2', value: 108 }
  ]);
  assert.equal(missingLag.status, 'Normal');
  assert.match(missingLag.note, /Missing required quarter/);
});

test('interest rate stress uses p80 and 6m change with t-6 missing fallback', () => {
  const series = [];
  for (let i = 1; i <= 70; i += 1) {
    const year = 2020 + Math.floor((i - 1) / 12);
    const month = String(((i - 1) % 12) + 1).padStart(2, '0');
    const value = i <= 64 ? 0.8 + i * 0.01 : 2.0 + (i - 64) * 0.2;
    series.push({ period: `${year}-${month}`, value });
  }
  const stress = computeInterestRateSignal(series);
  assert.equal(stress.status, 'Stress');
  assert.ok(stress.details.current.value > stress.details.p80_5y);
  assert.ok(stress.details.chg_6m_pp >= 0.75);

  const missingT6 = computeInterestRateSignal([
    { period: '2025-01', value: 1.0 },
    { period: '2025-03', value: 1.2 },
    { period: '2025-04', value: 1.3 },
    { period: '2025-05', value: 1.4 },
    { period: '2025-06', value: 1.5 },
    { period: '2025-08', value: 2.0 }
  ]);
  assert.equal(missingT6.status, 'Normal');
  assert.match(missingT6.note, /Missing t-6 month/);
});
