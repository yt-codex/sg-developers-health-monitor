const { nowIso } = require('./developer_ratios');

const SCORE_METRICS = [
  'netDebtToEbitda',
  'debtToEquity',
  'netDebtToEquity',
  'debtToEbitda',
  'quickRatio',
  'currentRatio',
  'roic',
  'roe',
  'payoutRatio',
  'assetTurnover'
];

const BASE_WEIGHTS = {
  debtToEquity: 0.15,
  netDebtToEbitda: 0.20,
  debtToEbitda: 0.08,
  netDebtToEquity: 0.12,
  quickRatio: 0.15,
  currentRatio: 0.10,
  roic: 0.10,
  roe: 0.05,
  assetTurnover: 0.03,
  payoutRatio: 0.02
};

function clamp(value, min = 0, max = 100) {
  return Math.min(max, Math.max(min, value));
}

function scaleRiskHigherWorse(value, lowGood, highBad) {
  if (!Number.isFinite(value)) return null;
  if (value <= lowGood) return 0;
  if (value >= highBad) return 100;
  return clamp(((value - lowGood) / (highBad - lowGood)) * 100);
}

function scaleRiskLowerWorse(value, highGood, lowBad) {
  if (!Number.isFinite(value)) return null;
  if (value >= highGood) return 0;
  if (value <= lowBad) return 100;
  return clamp(((highGood - value) / (highGood - lowBad)) * 100);
}

function riskForMetric(metricKey, value) {
  if (!Number.isFinite(value)) return null;
  switch (metricKey) {
    case 'debtToEquity':
      return scaleRiskHigherWorse(value, 0.5, 1.5);
    case 'netDebtToEbitda':
      return scaleRiskHigherWorse(value, 4, 12);
    case 'debtToEbitda':
      return scaleRiskHigherWorse(value, 5, 14);
    case 'quickRatio':
      return scaleRiskLowerWorse(value, 1.2, 0.6);
    case 'currentRatio':
      return scaleRiskLowerWorse(value, 1.8, 1.0);
    case 'roic':
      return scaleRiskLowerWorse(value, 8, 2);
    case 'roe':
      return scaleRiskLowerWorse(value, 10, 3);
    case 'assetTurnover':
      return scaleRiskLowerWorse(value, 0.15, 0.05);
    case 'payoutRatio':
      return value < 0 ? null : scaleRiskHigherWorse(value, 80, 200);
    case 'netDebtToEquity':
      return scaleRiskHigherWorse(value, 0.3, 1.2);
    default:
      return null;
  }
}

function getCurrentValue(metricObj) {
  return metricObj?.values?.Current ?? null;
}

function extractFySeries(metricObj) {
  const values = metricObj?.values || {};
  return Object.entries(values)
    .map(([label, value]) => {
      const match = /^FY\s+(\d{4})$/i.exec(label);
      if (!match || !Number.isFinite(value)) return null;
      return { label: `FY ${match[1]}`, year: Number(match[1]), value };
    })
    .filter(Boolean)
    .sort((a, b) => b.year - a.year);
}

function evaluateTrendForMetric(series, direction) {
  if (!Array.isArray(series) || series.length < 2) return { latestWorsened: false, consecutiveWorsened: false };

  const latest = series[0];
  const prev = series[1];
  const latestWorsened = direction === 'higherWorse' ? latest.value > prev.value : latest.value < prev.value;

  if (!latestWorsened || series.length < 3) {
    return { latestWorsened, consecutiveWorsened: false };
  }

  const prev2 = series[2];
  const secondWorsened = direction === 'higherWorse' ? prev.value > prev2.value : prev.value < prev2.value;

  return { latestWorsened, consecutiveWorsened: secondWorsened };
}

function computeTrendPenalty(metrics = {}) {
  const trendBreakdown = {};
  let trendPenalty = 0;

  const rules = [
    { key: 'netDebtToEbitda', direction: 'higherWorse', base: 4, consecutive: 6 },
    { key: 'debtToEquity', direction: 'higherWorse', base: 3, consecutive: 5 },
    { key: 'quickRatio', direction: 'lowerWorse', base: 3, consecutive: 5 },
    { key: 'currentRatio', direction: 'lowerWorse', base: 2, consecutive: 3 },
    { key: 'roic', direction: 'lowerWorse', base: 3, consecutive: 5 },
    { key: 'roe', direction: 'lowerWorse', base: 2, consecutive: 3 }
  ];

  for (const rule of rules) {
    const series = extractFySeries(metrics[rule.key]);
    const trend = evaluateTrendForMetric(series, rule.direction);
    const appliedPenalty = trend.latestWorsened ? (trend.consecutiveWorsened ? rule.consecutive : rule.base) : 0;
    trendBreakdown[rule.key] = {
      comparedYears: series.slice(0, 3).map((entry) => entry.label),
      latestWorsened: trend.latestWorsened,
      consecutiveWorsened: trend.consecutiveWorsened,
      appliedPenalty
    };
    trendPenalty += appliedPenalty;
  }

  const payoutTrend = evaluateTrendForMetric(extractFySeries(metrics.payoutRatio), 'higherWorse');
  const roeTrend = evaluateTrendForMetric(extractFySeries(metrics.roe), 'lowerWorse');
  const interactionPenalty = payoutTrend.latestWorsened && roeTrend.latestWorsened ? 2 : 0;

  trendBreakdown.payoutRoeInteraction = {
    payoutLatestWorsened: payoutTrend.latestWorsened,
    roeLatestWorsened: roeTrend.latestWorsened,
    appliedPenalty: interactionPenalty
  };

  trendPenalty += interactionPenalty;
  trendPenalty = Math.min(15, trendPenalty);

  return { trendPenalty, trendBreakdown };
}

function statusFromScore(score) {
  if (score == null) return 'Pending data';
  if (score >= 70) return 'Green';
  if (score >= 40) return 'Amber';
  return 'Red';
}

function computeHealthScore(record = {}) {
  const metrics = record.metrics || {};
  const riskByMetric = {};
  const weightedContributors = {};
  const usedMetrics = [];
  const missingMetrics = [];

  let weightedRiskSum = 0;
  let availableWeight = 0;

  for (const metricKey of SCORE_METRICS) {
    const currentValue = getCurrentValue(metrics[metricKey]);
    const risk = riskForMetric(metricKey, currentValue);
    if (risk == null) {
      missingMetrics.push(metricKey);
      continue;
    }

    const metricWeight = BASE_WEIGHTS[metricKey] || 0;
    usedMetrics.push(metricKey);
    riskByMetric[metricKey] = risk;
    availableWeight += metricWeight;
    weightedRiskSum += risk * metricWeight;
  }

  if (availableWeight === 0) {
    return {
      healthScore: null,
      healthStatus: 'Pending data',
      scoreCoverage: 0,
      scoreNote: 'Insufficient ratio coverage',
      healthScoreComponents: {
        staticRiskScore: null,
        staticHealthScore: null,
        trendPenalty: null,
        finalHealthScore: null,
        scoreCoverage: 0,
        missingMetrics,
        usedMetrics,
        riskByMetric,
        weightedContributors,
        trendBreakdown: {}
      },
      lastScoredAt: nowIso()
    };
  }

  const staticRiskScore = weightedRiskSum / availableWeight;
  const staticHealthScore = 100 - staticRiskScore;

  for (const metricKey of usedMetrics) {
    const metricWeight = BASE_WEIGHTS[metricKey] || 0;
    weightedContributors[metricKey] = (metricWeight * riskByMetric[metricKey]) / availableWeight;
  }

  const { trendPenalty, trendBreakdown } = computeTrendPenalty(metrics);
  const provisionalFinal = clamp(staticHealthScore - trendPenalty, 0, 100);
  const roundedFinal = Math.round(provisionalFinal);

  if (availableWeight < 0.5) {
    return {
      healthScore: null,
      healthStatus: 'Pending data',
      scoreCoverage: availableWeight,
      scoreNote: 'Insufficient ratio coverage',
      healthScoreComponents: {
        staticRiskScore,
        staticHealthScore,
        trendPenalty,
        finalHealthScore: roundedFinal,
        scoreCoverage: availableWeight,
        missingMetrics,
        usedMetrics,
        riskByMetric,
        weightedContributors,
        trendBreakdown
      },
      lastScoredAt: nowIso()
    };
  }

  return {
    healthScore: roundedFinal,
    healthStatus: statusFromScore(roundedFinal),
    scoreCoverage: availableWeight,
    scoreNote: null,
    healthScoreComponents: {
      staticRiskScore,
      staticHealthScore,
      trendPenalty,
      finalHealthScore: roundedFinal,
      scoreCoverage: availableWeight,
      missingMetrics,
      usedMetrics,
      riskByMetric,
      weightedContributors,
      trendBreakdown
    },
    lastScoredAt: nowIso()
  };
}

module.exports = {
  BASE_WEIGHTS,
  SCORE_METRICS,
  clamp,
  scaleRiskHigherWorse,
  scaleRiskLowerWorse,
  riskForMetric,
  computeTrendPenalty,
  computeHealthScore,
  statusFromScore
};
