const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const { parseRatiosTable } = require('../scripts/lib/developer_ratios');
const {
  BASE_WEIGHTS,
  NEGATIVE_LEVERAGE_SUPPORT,
  SCORE_COVERAGE_MIN,
  STATUS_BANDS,
  computeHealthScore,
  computeTrendPenalty,
  riskForMetric,
  statusFromScore
} = require('../scripts/lib/developer_health_score');

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

test('weights sum to 1.00', () => {
  const totalWeight = Object.values(BASE_WEIGHTS).reduce((sum, value) => sum + value, 0);
  assert.equal(Number(totalWeight.toFixed(2)), 1.0);
});

test('one missing metric renormalizes and still computes', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    quickRatio: { Current: 0.7 },
    roic: { Current: 6 },
    roe: { Current: 8 },
    assetTurnover: { Current: 0.08 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage < 1);
  assert.ok(result.scoreCoverage >= 0.5);
  assert.ok(result.healthScoreComponents.missingMetrics.includes('currentRatio'));
});

test('coverage below threshold returns Pending data', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 9 },
    debtToEquity: { Current: 1.3 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScore, null);
  assert.equal(result.healthStatus, 'Pending data');
  assert.equal(result.scoreNote, 'Insufficient ratio coverage');
  assert.ok(result.scoreCoverage < SCORE_COVERAGE_MIN);
});

test('trend penalty is capped at 8', () => {
  const metrics = {
    netDebtToEbitda: metric({ 'FY 2025': 20, 'FY 2024': 12, 'FY 2023': 6 }),
    debtToEquity: metric({ 'FY 2025': 2.8, 'FY 2024': 1.8, 'FY 2023': 0.9 }),
    quickRatio: metric({ 'FY 2025': 0.5, 'FY 2024': 0.7, 'FY 2023': 1.0 }),
    currentRatio: metric({ 'FY 2025': 1.0, 'FY 2024': 1.3, 'FY 2023': 1.8 }),
    roic: metric({ 'FY 2025': 2, 'FY 2024': 5, 'FY 2023': 8 }),
    roe: metric({ 'FY 2025': 1, 'FY 2024': 4, 'FY 2023': 9 }),
    payoutRatio: metric({ 'FY 2025': 260, 'FY 2024': 180, 'FY 2023': 90 })
  };

  const trend = computeTrendPenalty(metrics);
  assert.equal(trend.trendPenalty, 8);
});

test('debtToEbitda is excluded from score even when present', () => {
  const base = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    quickRatio: { Current: 0.7 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 6 },
    roe: { Current: 8 },
    payoutRatio: { Current: 90 },
    assetTurnover: { Current: 0.08 }
  });

  const withDebtToEbitda = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    debtToEbitda: { Current: 999 },
    quickRatio: { Current: 0.7 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 6 },
    roe: { Current: 8 },
    payoutRatio: { Current: 90 },
    assetTurnover: { Current: 0.08 }
  });

  const without = computeHealthScore(base);
  const withMetric = computeHealthScore(withDebtToEbitda);

  assert.equal(without.healthScore, withMetric.healthScore);
  assert.equal(withMetric.scoreCoverage, without.scoreCoverage);
  assert.ok(withMetric.healthScoreComponents.excludedMetrics.includes('debtToEbitda'));
  assert.equal(withMetric.healthScoreComponents.riskByMetric.debtToEbitda, undefined);
});

test('zero-weight metrics do not affect availableWeight or coverage', () => {
  const onlyZeroWeight = buildRecord({
    debtToEbitda: { Current: 10 }
  });

  const result = computeHealthScore(onlyZeroWeight);
  assert.equal(result.scoreCoverage, 0);
  assert.equal(result.healthScore, null);
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('debtToEbitda'));
  assert.equal(result.healthScoreComponents.usedMetrics.length, 0);
});

test('trend penalty excludes debtToEbitda', () => {
  const trend = computeTrendPenalty({
    debtToEbitda: metric({ 'FY 2025': 40, 'FY 2024': 20, 'FY 2023': 5 })
  });

  assert.equal(trend.trendPenalty, 0);
  assert.equal(trend.trendBreakdown.debtToEbitda, undefined);
});

test('payout/roe interaction penalty requires both metrics in included coverage set', () => {
  const metrics = {
    payoutRatio: metric({ 'FY 2025': 260, 'FY 2024': 180, 'FY 2023': 90 }),
    roe: metric({ 'FY 2025': 1, 'FY 2024': 4, 'FY 2023': 9 })
  };

  const eligible = computeTrendPenalty(metrics, { includedMetrics: ['payoutRatio', 'roe'] });
  const missingPayout = computeTrendPenalty(metrics, { includedMetrics: ['roe'] });

  assert.equal(eligible.trendBreakdown.payoutRoeInteraction.eligible, true);
  assert.equal(eligible.trendBreakdown.payoutRoeInteraction.appliedPenalty, 1);
  assert.equal(missingPayout.trendBreakdown.payoutRoeInteraction.eligible, false);
  assert.equal(missingPayout.trendBreakdown.payoutRoeInteraction.appliedPenalty, 0);
});

test('missing payout current value does not trigger payout/roe interaction in final score', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    quickRatio: { Current: 0.7 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 6, 'FY 2025': 6, 'FY 2024': 7 },
    roe: { Current: 8, 'FY 2025': 8, 'FY 2024': 9 },
    payoutRatio: { 'FY 2025': 260, 'FY 2024': 180, 'FY 2023': 90 },
    assetTurnover: { Current: 0.08 }
  });

  const result = computeHealthScore(record);
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('payoutRatio'));
  assert.equal(result.healthScoreComponents.trendBreakdown.payoutRoeInteraction.eligible, false);
  assert.equal(result.healthScoreComponents.trendBreakdown.payoutRoeInteraction.appliedPenalty, 0);
});

test('negative leverage is not auto-low-risk when support metrics are weak', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: -2.0 },
    debtToEquity: { Current: 1.1 },
    netDebtToEquity: { Current: -0.2 },
    quickRatio: { Current: 0.2 },
    currentRatio: { Current: 0.9 },
    roic: { Current: 0.5 },
    roe: { Current: 0.8 },
    assetTurnover: { Current: 0.08 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEbitda, NEGATIVE_LEVERAGE_SUPPORT.weakSupportRiskFloor);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEquity, NEGATIVE_LEVERAGE_SUPPORT.weakSupportRiskFloor);
  assert.equal(result.healthScoreComponents.metricAdjustments.netDebtToEbitda.supportBand, 'weak');
  assert.equal(result.healthScoreComponents.metricAdjustments.netDebtToEbitda.applied, true);
});

test('negative leverage stays low-risk when support metrics are strong', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: -1.4 },
    debtToEquity: { Current: 0.6 },
    netDebtToEquity: { Current: -0.1 },
    quickRatio: { Current: 1.2 },
    currentRatio: { Current: 2.3 },
    roic: { Current: 14 },
    roe: { Current: 16 },
    payoutRatio: { Current: 80 },
    assetTurnover: { Current: 0.2 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEbitda, 0);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEquity, 0);
  assert.equal(result.healthScoreComponents.metricAdjustments.netDebtToEbitda.supportBand, 'strong');
  assert.equal(result.healthScoreComponents.metricAdjustments.netDebtToEbitda.applied, false);
});

test('negative leverage uses neutral floor when support metrics are unavailable', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: -0.5 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: -0.1 }
  });

  const result = computeHealthScore(record);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEbitda, NEGATIVE_LEVERAGE_SUPPORT.noSupportRiskFloor);
  assert.equal(result.healthScoreComponents.riskByMetric.netDebtToEquity, NEGATIVE_LEVERAGE_SUPPORT.noSupportRiskFloor);
  assert.equal(result.healthScoreComponents.metricAdjustments.netDebtToEbitda.supportBand, 'no_support');
});

test('partial metric sets can still compute when positive-weight coverage is sufficient', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 9 },
    debtToEquity: { Current: 1.4 },
    netDebtToEquity: { Current: 1.1 },
    currentRatio: { Current: 1.4 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage >= 0.5);
});

test('simplified model excludes quick ratio, payout ratio, and asset turnover', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 9 },
    debtToEquity: { Current: 1.4 },
    netDebtToEquity: { Current: 1.1 },
    quickRatio: { Current: 0.8 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 7 },
    roe: { Current: 9 },
    payoutRatio: { Current: 90 },
    assetTurnover: { Current: 0.08 }
  });

  const result = computeHealthScore(record);
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('quickRatio'));
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('payoutRatio'));
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('assetTurnover'));
});

test('high leverage mixed profile no longer trivially collapses to 0', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 14, 'FY 2025': 14, 'FY 2024': 13 },
    debtToEquity: { Current: 2.0, 'FY 2025': 2.0, 'FY 2024': 1.9 },
    netDebtToEquity: { Current: 1.4 },
    debtToEbitda: { Current: 16 },
    quickRatio: { Current: 0.65 },
    currentRatio: { Current: 1.35 },
    roic: { Current: 6 },
    roe: { Current: 8 },
    payoutRatio: { Current: 110 },
    assetTurnover: { Current: 0.10 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.healthScore > 0);
});

test('risk boundaries obey v2 threshold edges', () => {
  assert.equal(riskForMetric('debtToEquity', 0.8), 0);
  assert.equal(riskForMetric('debtToEquity', 2.5), 100);

  assert.equal(riskForMetric('netDebtToEquity', 0.4), 0);
  assert.equal(riskForMetric('netDebtToEquity', 1.8), 100);

  assert.equal(riskForMetric('netDebtToEbitda', 6), 0);
  assert.equal(riskForMetric('netDebtToEbitda', 18), 100);

  assert.equal(riskForMetric('debtToEbitda', 8), 0);
  assert.equal(riskForMetric('debtToEbitda', 22), 100);

  assert.equal(riskForMetric('quickRatio', 1.0), 0);
  assert.equal(riskForMetric('quickRatio', 0.3), 100);

  assert.equal(riskForMetric('currentRatio', 2.0), 0);
  assert.equal(riskForMetric('currentRatio', 1.0), 100);

  assert.equal(riskForMetric('roic', 10), 0);
  assert.equal(riskForMetric('roic', 1), 100);

  assert.equal(riskForMetric('roe', 12), 0);
  assert.equal(riskForMetric('roe', 2), 100);

  assert.equal(riskForMetric('assetTurnover', 0.18), 0);
  assert.equal(riskForMetric('assetTurnover', 0.04), 100);

  assert.equal(riskForMetric('payoutRatio', 100), 0);
  assert.equal(riskForMetric('payoutRatio', 250), 100);
  assert.equal(riskForMetric('payoutRatio', -1), null);
});

test('status bands are consistent with scoring constants', () => {
  assert.equal(statusFromScore(STATUS_BANDS.green), 'Green');
  assert.equal(statusFromScore(STATUS_BANDS.green - 1), 'Amber');
  assert.equal(statusFromScore(STATUS_BANDS.amber), 'Amber');
  assert.equal(statusFromScore(STATUS_BANDS.amber - 1), 'Red');
});

test("'-' parsed as null from fixture does not crash score and keeps computing", () => {
  const html = fs.readFileSync(path.join(__dirname, 'fixtures', 'stockanalysis_ratios_9CI_sample.html'), 'utf8');
  const parsed = parseRatiosTable(html);
  const result = computeHealthScore({ metrics: parsed.metrics });

  assert.equal(parsed.metrics.payoutRatio.values.Current, null);
  assert.ok(result.healthScore === null || Number.isFinite(result.healthScore));
  assert.ok(result.healthScoreComponents.excludedMetrics.includes('payoutRatio'));
});
