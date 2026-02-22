const test = require('node:test');
const assert = require('node:assert/strict');

const {
  computeSectorPerformanceSignal,
  computeLabourCostSignal,
  computeInterestRateSignal,
  computeMaterialsPriceSignal
} = require('../scripts/lib/macro_stress_signals');
const {
  MATERIALS_PRICE_YOY_THRESHOLD_PCT,
  MATERIALS_PRICE_SERIES_IDS
} = require('../scripts/lib/macro_stress_constants');

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

function buildMonthlySeries({ base = 100, yoyPrev = 0, yoyLatest = 0 }) {
  return [
    { period: '2024-11', value: 100 },
    { period: '2024-12', value: 100 },
    { period: '2025-11', value: base * (1 + yoyPrev) },
    { period: '2025-12', value: base * (1 + yoyLatest) }
  ];
}

test('materials price watch: one series triggers => Watch', () => {
  const targetSeries = MATERIALS_PRICE_SERIES_IDS[0];
  const seriesById = Object.fromEntries(
    MATERIALS_PRICE_SERIES_IDS.map((seriesId) => [
      seriesId,
      {
        values: seriesId === targetSeries
          ? buildMonthlySeries({ yoyPrev: 0.09, yoyLatest: 0.11 })
          : buildMonthlySeries({ yoyPrev: 0.01, yoyLatest: 0.02 })
      }
    ])
  );

  const signal = computeMaterialsPriceSignal(seriesById);
  assert.equal(signal.status, 'Watch');
  assert.equal(signal.details.triggered_by, targetSeries);
  assert.ok(signal.details.yoy_latest.value >= MATERIALS_PRICE_YOY_THRESHOLD_PCT);
  assert.ok(signal.details.yoy_prev.value >= MATERIALS_PRICE_YOY_THRESHOLD_PCT);
});

test('materials price watch: different series crossing in only one month each => Normal', () => {
  const [a, b, ...rest] = MATERIALS_PRICE_SERIES_IDS;
  const seriesById = {
    [a]: { values: buildMonthlySeries({ yoyPrev: 0.09, yoyLatest: 0.03 }) },
    [b]: { values: buildMonthlySeries({ yoyPrev: 0.02, yoyLatest: 0.1 }) }
  };
  for (const seriesId of rest) {
    seriesById[seriesId] = { values: buildMonthlySeries({ yoyPrev: 0.01, yoyLatest: 0.02 }) };
  }

  const signal = computeMaterialsPriceSignal(seriesById);
  assert.equal(signal.status, 'Normal');
  assert.equal(signal.details.triggered_by, null);
});

test('materials price watch: missing t-12/t-13 for all series => Normal with note', () => {
  const seriesById = Object.fromEntries(
    MATERIALS_PRICE_SERIES_IDS.map((seriesId) => [
      seriesId,
      {
        values: [
          { period: '2025-11', value: 102 },
          { period: '2025-12', value: 104 }
        ]
      }
    ])
  );

  const signal = computeMaterialsPriceSignal(seriesById);
  assert.equal(signal.status, 'Normal');
  assert.equal(signal.details.triggered_by, null);
  assert.match(signal.note, /missing required t-12\/t-13 lags/i);
});
