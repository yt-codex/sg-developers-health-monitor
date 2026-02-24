const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const { parseRatiosTable } = require('../scripts/lib/developer_ratios');
const { computeHealthScore, computeTrendPenalty } = require('../scripts/lib/developer_health_score');

function metric(values = {}) {
  return { values };
}

function buildRecord(metricValues = {}) {
  return {
    metrics: {
      netDebtToEbitda: metric(metricValues.netDebtToEbitda || {}),
      debtToEquity: metric(metricValues.debtToEquity || {}),
      netDebtToEquity: metric(metricValues.netDebtToEquity || {}),
      debtToEbitda: metric(metricValues.debtToEbitda || {}),
      quickRatio: metric(metricValues.quickRatio || {}),
      currentRatio: metric(metricValues.currentRatio || {}),
      roic: metric(metricValues.roic || {}),
      roe: metric(metricValues.roe || {}),
      payoutRatio: metric(metricValues.payoutRatio || {}),
      assetTurnover: metric(metricValues.assetTurnover || {})
    }
  };
}

test('full metrics case computes score and status', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6 },
    debtToEquity: { Current: 0.8 },
    netDebtToEquity: { Current: 0.5 },
    debtToEbitda: { Current: 7 },
    quickRatio: { Current: 1.1 },
    currentRatio: { Current: 1.7 },
    roic: { Current: 7 },
    roe: { Current: 9 },
    payoutRatio: { Current: 60 },
    assetTurnover: { Current: 0.12 }
  });

  const result = computeHealthScore(record);
  assert.equal(typeof result.healthScore, 'number');
  assert.match(result.healthStatus, /Green|Amber|Red/);
  assert.equal(result.healthScoreComponents.missingMetrics.length, 0);
});

test('one missing metric renormalizes and still computes', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6 },
    debtToEquity: { Current: 0.8 },
    netDebtToEquity: { Current: 0.5 },
    debtToEbitda: { Current: 7 },
    quickRatio: { Current: 1.1 },
    currentRatio: { Current: 1.7 },
    roic: { Current: 7 },
    roe: { Current: 9 },
    assetTurnover: { Current: 0.12 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage < 1);
  assert.ok(result.scoreCoverage >= 0.5);
  assert.ok(result.healthScoreComponents.missingMetrics.includes('payoutRatio'));
});

test('several missing metrics but coverage >= 0.5 still computes', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6 },
    debtToEquity: { Current: 0.8 },
    netDebtToEquity: { Current: 0.5 },
    quickRatio: { Current: 1.1 },
    currentRatio: { Current: 1.7 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage >= 0.5);
});

test('coverage below threshold returns Pending data', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6 },
    debtToEquity: { Current: 0.8 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScore, null);
  assert.equal(result.healthStatus, 'Pending data');
  assert.equal(result.scoreNote, 'Insufficient ratio coverage');
});

test('trend penalty applies when leverage worsens', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6, 'FY 2025': 10, 'FY 2024': 8 },
    debtToEquity: { Current: 0.8, 'FY 2025': 1.2, 'FY 2024': 0.8 },
    netDebtToEquity: { Current: 0.5 },
    debtToEbitda: { Current: 7 },
    quickRatio: { Current: 1.1 },
    currentRatio: { Current: 1.7 },
    roic: { Current: 7 },
    roe: { Current: 9 },
    payoutRatio: { Current: 60 },
    assetTurnover: { Current: 0.12 }
  });

  const result = computeHealthScore(record);
  assert.ok(result.healthScoreComponents.trendPenalty >= 7);
});

test('trend penalty not applied when FY history missing', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 6 },
    debtToEquity: { Current: 0.8 },
    netDebtToEquity: { Current: 0.5 },
    debtToEbitda: { Current: 7 },
    quickRatio: { Current: 1.1 },
    currentRatio: { Current: 1.7 },
    roic: { Current: 7 },
    roe: { Current: 9 },
    payoutRatio: { Current: 60 },
    assetTurnover: { Current: 0.12 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScoreComponents.trendPenalty, 0);
});

test('trend penalty is capped at 15', () => {
  const metrics = {
    netDebtToEbitda: metric({ 'FY 2025': 20, 'FY 2024': 10, 'FY 2023': 5 }),
    debtToEquity: metric({ 'FY 2025': 2, 'FY 2024': 1, 'FY 2023': 0.5 }),
    quickRatio: metric({ 'FY 2025': 0.5, 'FY 2024': 0.8, 'FY 2023': 1.1 }),
    currentRatio: metric({ 'FY 2025': 1.0, 'FY 2024': 1.4, 'FY 2023': 1.8 }),
    roic: metric({ 'FY 2025': 2, 'FY 2024': 5, 'FY 2023': 9 }),
    roe: metric({ 'FY 2025': 1, 'FY 2024': 5, 'FY 2023': 11 }),
    payoutRatio: metric({ 'FY 2025': 220, 'FY 2024': 110, 'FY 2023': 70 })
  };

  const trend = computeTrendPenalty(metrics);
  assert.equal(trend.trendPenalty, 15);
});

test("'-' parsed as null from fixture does not crash score and keeps computing", () => {
  const html = fs.readFileSync(path.join(__dirname, 'fixtures', 'stockanalysis_ratios_9CI_sample.html'), 'utf8');
  const parsed = parseRatiosTable(html);
  const result = computeHealthScore({ metrics: parsed.metrics });

  assert.equal(parsed.metrics.payoutRatio.values.Current, null);
  assert.ok(result.healthScore === null || Number.isFinite(result.healthScore));
  assert.ok(result.healthScoreComponents.missingMetrics.includes('payoutRatio'));
});
