const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const { parseRatiosTable } = require('../scripts/lib/developer_ratios');
const {
  BASE_WEIGHTS,
  NEGATIVE_LEVERAGE_SUPPORT,
  POLICY_PILLAR_WEIGHTS,
  SCORE_COVERAGE_MIN,
  STATUS_BANDS,
  TREND_MIN_WORSENING_METRICS,
  TREND_PENALTY_CAP,
  TREND_PENALTY_MULTIPLIER,
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

test('legacy metric weights are zero in pillar model', () => {
  const totalWeight = Object.values(BASE_WEIGHTS).reduce((sum, value) => sum + value, 0);
  assert.equal(Number(totalWeight.toFixed(2)), 0);
});

test('pillar weights sum to 1.00', () => {
  const totalWeight = Object.values(POLICY_PILLAR_WEIGHTS).reduce((sum, value) => sum + value, 0);
  assert.equal(Number(totalWeight.toFixed(2)), 1.0);
});

test('missing one key metric still computes when pillar coverage >= threshold', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    roic: { Current: 6 },
    roe: { Current: 8 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage < 1);
  assert.ok(result.scoreCoverage >= SCORE_COVERAGE_MIN);
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

test('trend penalty is scaled and capped', () => {
  const metrics = {
    netDebtToEbitda: metric({ 'FY 2025': 20, 'FY 2024': 12, 'FY 2023': 6 }),
    debtToEquity: metric({ 'FY 2025': 2.8, 'FY 2024': 1.8, 'FY 2023': 0.9 }),
    currentRatio: metric({ 'FY 2025': 1.0, 'FY 2024': 1.3, 'FY 2023': 1.8 }),
    roic: metric({ 'FY 2025': 2, 'FY 2024': 5, 'FY 2023': 8 }),
    roe: metric({ 'FY 2025': 1, 'FY 2024': 4, 'FY 2023': 9 })
  };

  const trend = computeTrendPenalty(metrics);
  const expectedRaw = 3 + 2.5 + 1.5 + 2.5 + 1.5;
  assert.equal(Number(trend.rawTrendPenalty.toFixed(2)), Number(expectedRaw.toFixed(2)));
  assert.equal(trend.worseningMetricCount, 5);
  assert.equal(
    Number(trend.trendPenalty.toFixed(2)),
    Number(Math.min(TREND_PENALTY_CAP, expectedRaw * TREND_PENALTY_MULTIPLIER).toFixed(2))
  );
});

test('trend penalty is zero when fewer than minimum metrics worsen', () => {
  const metrics = {
    netDebtToEbitda: metric({ 'FY 2025': 8, 'FY 2024': 8, 'FY 2023': 7 }),
    debtToEquity: metric({ 'FY 2025': 1.1, 'FY 2024': 1.1, 'FY 2023': 1.0 }),
    currentRatio: metric({ 'FY 2025': 1.8, 'FY 2024': 1.8, 'FY 2023': 1.9 }),
    roic: metric({ 'FY 2025': 5.5, 'FY 2024': 5.5, 'FY 2023': 6.0 }),
    roe: metric({ 'FY 2025': 8, 'FY 2024': 8, 'FY 2023': 9 })
  };

  const trend = computeTrendPenalty(metrics);
  assert.ok(trend.worseningMetricCount < TREND_MIN_WORSENING_METRICS);
  assert.equal(trend.trendPenalty, 0);
});

test('debtToEbitda is excluded from score even when present', () => {
  const base = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 6 },
    roe: { Current: 8 }
  });

  const withDebtToEbitda = buildRecord({
    netDebtToEbitda: { Current: 8 },
    debtToEquity: { Current: 1.2 },
    netDebtToEquity: { Current: 0.9 },
    debtToEbitda: { Current: 999 },
    currentRatio: { Current: 1.5 },
    roic: { Current: 6 },
    roe: { Current: 8 }
  });

  const without = computeHealthScore(base);
  const withMetric = computeHealthScore(withDebtToEbitda);

  assert.equal(without.healthScore, withMetric.healthScore);
  assert.equal(withMetric.scoreCoverage, without.scoreCoverage);
  assert.ok(withMetric.healthScoreComponents.excludedMetrics.includes('debtToEbitda'));
  assert.equal(withMetric.healthScoreComponents.riskByMetric.debtToEbitda, undefined);
});

test('trend rules ignore debtToEbitda', () => {
  const trend = computeTrendPenalty({
    debtToEbitda: metric({ 'FY 2025': 40, 'FY 2024': 20, 'FY 2023': 5 })
  });

  assert.equal(trend.trendPenalty, 0);
  assert.equal(trend.trendBreakdown.debtToEbitda, undefined);
});

test('negative leverage is not auto-low-risk when support metrics are weak', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: -2.0 },
    debtToEquity: { Current: 1.1 },
    netDebtToEquity: { Current: -0.2 },
    currentRatio: { Current: 0.9 },
    roic: { Current: 0.5 },
    roe: { Current: 0.8 }
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
    currentRatio: { Current: 2.3 },
    roic: { Current: 14 },
    roe: { Current: 16 }
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

test('partial metric sets can still compute when pillar coverage is sufficient', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 9 },
    debtToEquity: { Current: 1.4 },
    netDebtToEquity: { Current: 1.1 },
    currentRatio: { Current: 1.4 }
  });

  const result = computeHealthScore(record);
  assert.notEqual(result.healthScore, null);
  assert.ok(result.scoreCoverage >= SCORE_COVERAGE_MIN);
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

test('pillar contributors exist and sum to static risk score', () => {
  const record = buildRecord({
    netDebtToEbitda: { Current: 10 },
    debtToEquity: { Current: 1.3 },
    netDebtToEquity: { Current: 0.9 },
    currentRatio: { Current: 1.6 },
    roic: { Current: 4 },
    roe: { Current: 6 }
  });

  const result = computeHealthScore(record);
  const contributors = result.healthScoreComponents.pillarContributors || {};
  assert.ok(Object.keys(contributors).length >= 2);
  const deductionSum = Object.values(contributors).reduce((sum, detail) => (
    sum + (Number.isFinite(detail.weightedRiskContribution) ? detail.weightedRiskContribution : 0)
  ), 0);
  assert.equal(Number(deductionSum.toFixed(6)), Number(result.healthScoreComponents.staticRiskScore.toFixed(6)));
});

test('risk boundaries obey threshold edges', () => {
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

  assert.equal(riskForMetric('roic', 5), 0);
  assert.equal(riskForMetric('roic', 1), 100);

  assert.equal(riskForMetric('roe', 10), 0);
  assert.equal(riskForMetric('roe', 0), 100);

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
