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
  netDebtToEbitda: 0.20,
  debtToEquity: 0.15,
  netDebtToEquity: 0.10,
  debtToEbitda: 0,
  currentRatio: 0.20,
  quickRatio: 0,
  roic: 0.20,
  roe: 0.15,
  assetTurnover: 0,
  payoutRatio: 0
};

const STATUS_BANDS = {
  green: 70,
  amber: 40
};

const SCORE_COVERAGE_MIN = 0.5;
const TREND_PENALTY_CAP = 8;

const RISK_THRESHOLDS = {
  debtToEquity: { direction: 'higherWorse', good: 0.8, bad: 2.5 },
  netDebtToEquity: { direction: 'higherWorse', good: 0.4, bad: 1.8 },
  netDebtToEbitda: { direction: 'higherWorse', good: 6, bad: 18 },
  debtToEbitda: { direction: 'higherWorse', good: 8, bad: 22 },
  quickRatio: { direction: 'lowerWorse', good: 1.0, bad: 0.3 },
  currentRatio: { direction: 'lowerWorse', good: 2.0, bad: 1.0 },
  roic: { direction: 'lowerWorse', good: 10, bad: 1 },
  roe: { direction: 'lowerWorse', good: 12, bad: 2 },
  assetTurnover: { direction: 'lowerWorse', good: 0.18, bad: 0.04 },
  payoutRatio: { direction: 'higherWorse', good: 100, bad: 250 }
};

const TREND_RULES = [
  { key: 'netDebtToEbitda', direction: 'higherWorse', base: 2, consecutive: 3 },
  { key: 'debtToEquity', direction: 'higherWorse', base: 1.5, consecutive: 2.5 },
  { key: 'currentRatio', direction: 'lowerWorse', base: 1, consecutive: 1.5 },
  { key: 'roic', direction: 'lowerWorse', base: 1.5, consecutive: 2.5 },
  { key: 'roe', direction: 'lowerWorse', base: 1, consecutive: 1.5 }
];

const NEGATIVE_LEVERAGE_SUPPORT = {
  targetMetrics: ['netDebtToEbitda', 'netDebtToEquity'],
  supportMetrics: ['currentRatio', 'roic', 'roe'],
  strongMinHealthScore: 70,
  mixedMinHealthScore: 40,
  noSupportRiskFloor: 50,
  mixedSupportRiskFloor: 25,
  weakSupportRiskFloor: 55
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

  if (metricKey === 'payoutRatio' && value < 0) return null;

  const threshold = RISK_THRESHOLDS[metricKey];
  if (!threshold) return null;

  return threshold.direction === 'higherWorse'
    ? scaleRiskHigherWorse(value, threshold.good, threshold.bad)
    : scaleRiskLowerWorse(value, threshold.good, threshold.bad);
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

function isMetricIncluded(metricKey, includedMetricsSet) {
  return !includedMetricsSet || includedMetricsSet.has(metricKey);
}

function average(values = []) {
  if (!values.length) return null;
  const sum = values.reduce((acc, value) => acc + value, 0);
  return sum / values.length;
}

function computeSupportHealthScore(metrics = {}, riskByMetric = {}) {
  const supportMetricsUsed = [];
  const supportHealthValues = [];

  for (const metricKey of NEGATIVE_LEVERAGE_SUPPORT.supportMetrics) {
    let risk = riskByMetric[metricKey];
    if (!Number.isFinite(risk)) {
      const currentValue = getCurrentValue(metrics[metricKey]);
      risk = riskForMetric(metricKey, currentValue);
    }
    if (!Number.isFinite(risk)) continue;
    supportMetricsUsed.push(metricKey);
    supportHealthValues.push(100 - risk);
  }

  return {
    supportMetricsUsed,
    supportHealthScore: average(supportHealthValues)
  };
}

function adjustNegativeLeverageRisks(metrics = {}, riskByMetric = {}, usedMetrics = []) {
  const usedMetricsSet = new Set(usedMetrics);
  const { supportMetricsUsed, supportHealthScore } = computeSupportHealthScore(metrics, riskByMetric);
  const metricAdjustments = {};

  for (const metricKey of NEGATIVE_LEVERAGE_SUPPORT.targetMetrics) {
    if (!usedMetricsSet.has(metricKey)) continue;
    const currentValue = getCurrentValue(metrics[metricKey]);
    if (!Number.isFinite(currentValue) || currentValue >= 0) continue;

    const originalRisk = riskByMetric[metricKey];
    let riskFloor;
    let supportBand;

    if (!Number.isFinite(supportHealthScore)) {
      riskFloor = NEGATIVE_LEVERAGE_SUPPORT.noSupportRiskFloor;
      supportBand = 'no_support';
    } else if (supportHealthScore >= NEGATIVE_LEVERAGE_SUPPORT.strongMinHealthScore) {
      riskFloor = 0;
      supportBand = 'strong';
    } else if (supportHealthScore >= NEGATIVE_LEVERAGE_SUPPORT.mixedMinHealthScore) {
      riskFloor = NEGATIVE_LEVERAGE_SUPPORT.mixedSupportRiskFloor;
      supportBand = 'mixed';
    } else {
      riskFloor = NEGATIVE_LEVERAGE_SUPPORT.weakSupportRiskFloor;
      supportBand = 'weak';
    }

    const adjustedRisk = Math.max(originalRisk, riskFloor);
    riskByMetric[metricKey] = adjustedRisk;
    metricAdjustments[metricKey] = {
      rule: 'negativeLeverageSupportGate',
      currentValue,
      originalRisk,
      adjustedRisk,
      supportBand,
      supportHealthScore,
      supportMetricsUsed,
      applied: adjustedRisk !== originalRisk
    };
  }

  return metricAdjustments;
}

function computeTrendPenalty(metrics = {}, options = {}) {
  const includedMetricsSet = Array.isArray(options.includedMetrics)
    ? new Set(options.includedMetrics)
    : null;
  const trendBreakdown = {};
  let trendPenalty = 0;

  for (const rule of TREND_RULES) {
    if (!isMetricIncluded(rule.key, includedMetricsSet)) {
      trendBreakdown[rule.key] = {
        comparedYears: [],
        latestWorsened: false,
        consecutiveWorsened: false,
        appliedPenalty: 0,
        skipped: true
      };
      continue;
    }

    const series = extractFySeries(metrics[rule.key]);
    const trend = evaluateTrendForMetric(series, rule.direction);
    const appliedPenalty = trend.latestWorsened ? (trend.consecutiveWorsened ? rule.consecutive : rule.base) : 0;
    trendBreakdown[rule.key] = {
        comparedYears: series.slice(0, 3).map((entry) => entry.label),
        latestWorsened: trend.latestWorsened,
        consecutiveWorsened: trend.consecutiveWorsened,
        appliedPenalty,
        skipped: false
      };
      trendPenalty += appliedPenalty;
  }

  const payoutAndRoeEligible = isMetricIncluded('payoutRatio', includedMetricsSet)
    && isMetricIncluded('roe', includedMetricsSet);
  const payoutTrend = payoutAndRoeEligible
    ? evaluateTrendForMetric(extractFySeries(metrics.payoutRatio), 'higherWorse')
    : { latestWorsened: false, consecutiveWorsened: false };
  const roeTrend = payoutAndRoeEligible
    ? evaluateTrendForMetric(extractFySeries(metrics.roe), 'lowerWorse')
    : { latestWorsened: false, consecutiveWorsened: false };
  const interactionPenalty = payoutAndRoeEligible && payoutTrend.latestWorsened && roeTrend.latestWorsened ? 1 : 0;

  trendBreakdown.payoutRoeInteraction = {
    eligible: payoutAndRoeEligible,
    payoutLatestWorsened: payoutTrend.latestWorsened,
    roeLatestWorsened: roeTrend.latestWorsened,
    appliedPenalty: interactionPenalty
  };

  trendPenalty += interactionPenalty;
  trendPenalty = Math.min(TREND_PENALTY_CAP, trendPenalty);

  return { trendPenalty, trendBreakdown };
}

function statusFromScore(score) {
  if (score == null) return 'Pending data';
  if (score >= STATUS_BANDS.green) return 'Green';
  if (score >= STATUS_BANDS.amber) return 'Amber';
  return 'Red';
}

function computeHealthScore(record = {}) {
  const metrics = record.metrics || {};
  const riskByMetric = {};
  const weightedContributors = {};
  const usedMetrics = [];
  const missingMetrics = [];
  const excludedMetrics = [];
  let metricAdjustments = {};

  let availableWeight = 0;

  for (const metricKey of SCORE_METRICS) {
    const metricWeight = BASE_WEIGHTS[metricKey] || 0;
    if (metricWeight <= 0) {
      excludedMetrics.push(metricKey);
      continue;
    }

    const currentValue = getCurrentValue(metrics[metricKey]);
    const risk = riskForMetric(metricKey, currentValue);
    if (risk == null) {
      missingMetrics.push(metricKey);
      continue;
    }

    usedMetrics.push(metricKey);
    riskByMetric[metricKey] = risk;
    availableWeight += metricWeight;
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
        excludedMetrics,
        usedMetrics,
        riskByMetric,
        weightedContributors,
        metricAdjustments: {},
        trendBreakdown: {}
      },
      lastScoredAt: nowIso()
    };
  }

  metricAdjustments = adjustNegativeLeverageRisks(metrics, riskByMetric, usedMetrics);

  const weightedRiskSum = usedMetrics.reduce((sum, metricKey) => {
    const metricWeight = BASE_WEIGHTS[metricKey] || 0;
    const metricRisk = riskByMetric[metricKey];
    if (!Number.isFinite(metricRisk)) return sum;
    return sum + (metricRisk * metricWeight);
  }, 0);

  const staticRiskScore = weightedRiskSum / availableWeight;
  const staticHealthScore = 100 - staticRiskScore;

  for (const metricKey of usedMetrics) {
    const metricWeight = BASE_WEIGHTS[metricKey] || 0;
    weightedContributors[metricKey] = (metricWeight * riskByMetric[metricKey]) / availableWeight;
  }

  const { trendPenalty, trendBreakdown } = computeTrendPenalty(metrics, { includedMetrics: usedMetrics });
  const provisionalFinal = clamp(staticHealthScore - trendPenalty, 0, 100);
  const roundedFinal = Math.round(provisionalFinal);

  if (availableWeight < SCORE_COVERAGE_MIN) {
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
        excludedMetrics,
        usedMetrics,
        riskByMetric,
        weightedContributors,
        metricAdjustments,
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
      excludedMetrics,
      usedMetrics,
      riskByMetric,
      weightedContributors,
      metricAdjustments,
      trendBreakdown
    },
    lastScoredAt: nowIso()
  };
}

module.exports = {
  BASE_WEIGHTS,
  SCORE_COVERAGE_MIN,
  STATUS_BANDS,
  TREND_PENALTY_CAP,
  TREND_RULES,
  NEGATIVE_LEVERAGE_SUPPORT,
  RISK_THRESHOLDS,
  SCORE_METRICS,
  clamp,
  scaleRiskHigherWorse,
  scaleRiskLowerWorse,
  riskForMetric,
  computeTrendPenalty,
  computeHealthScore,
  statusFromScore
};
