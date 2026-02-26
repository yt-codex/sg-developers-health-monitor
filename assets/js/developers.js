const SORT_DIRECTIONS = ['none', 'ascending', 'descending'];
const PENDING_RE = /pending/i;
const STATUS_LABEL_MAP = {
  Green: 'Stable',
  Amber: 'Watch',
  Red: 'Elevated',
  'Pending data': 'Data pending'
};
const PILLAR_EXPLANATIONS = {
  leverage: {
    label: 'Leverage pillar',
    rationale: 'Median leverage risk avoids triple-counting debt metrics while preserving balance-sheet pressure signals.'
  },
  liquidity: {
    label: 'Liquidity pillar',
    rationale: 'Current ratio captures short-term refinancing and working-capital stress.'
  },
  resilience: {
    label: 'Resilience pillar',
    rationale: 'Average ROIC/ROE captures sustained return quality through development cycles.'
  }
};
const METRIC_EXPLANATIONS = {
  netDebtToEbitda: {
    label: 'Net Debt / EBITDA',
    category: 'Leverage',
    rationale: 'Primary debt burden signal against operating cash generation.'
  },
  debtToEquity: {
    label: 'Debt / Equity',
    category: 'Leverage',
    rationale: 'Capital structure stress signal when debt dominates equity.'
  },
  netDebtToEquity: {
    label: 'Net Debt / Equity',
    category: 'Leverage',
    rationale: 'Balance-sheet leverage after cash offset, useful for net cash cases.'
  },
  currentRatio: {
    label: 'Current Ratio',
    category: 'Liquidity',
    rationale: 'Near-term obligations buffer for refinancing and working capital.'
  },
  quickRatio: {
    label: 'Quick Ratio',
    category: 'Liquidity',
    rationale: 'More conservative liquidity check excluding less liquid current assets.'
  },
  roic: {
    label: 'ROIC',
    category: 'Profitability',
    rationale: 'Capital efficiency quality check for long-cycle project deployment.'
  },
  roe: {
    label: 'ROE',
    category: 'Profitability',
    rationale: 'Shareholder return quality check across the cycle.'
  },
  payoutRatio: {
    label: 'Payout Ratio',
    category: 'Distribution policy',
    rationale: 'Flags over-distribution risk when payouts exceed sustainable earnings.'
  },
  assetTurnover: {
    label: 'Asset Turnover',
    category: 'Efficiency',
    rationale: 'Secondary operating velocity signal for asset-heavy developers.'
  },
  debtToEbitda: {
    label: 'Debt / EBITDA',
    category: 'Leverage',
    rationale: 'Display reference only in current model.'
  }
};

function parseNumberCandidate(value) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed || PENDING_RE.test(trimmed)) return null;
  const numeric = Number(trimmed.replace(/[,%$]/g, ''));
  return Number.isFinite(numeric) ? numeric : null;
}


function formatNumber(value, decimals = 2) {
  const numeric = parseNumberCandidate(value);
  if (numeric == null) return 'Pending data';
  return numeric.toLocaleString('en-SG', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
}

function formatPercent(value) {
  const numeric = parseNumberCandidate(value);
  if (numeric == null) return 'Pending data';
  const percentValue = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
  return `${percentValue.toLocaleString('en-SG', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
  })}%`;
}

function formatPayoutRatio(value) {
  const numeric = parseNumberCandidate(value);
  if (numeric == null) return '-';
  return formatPercent(numeric);
}

function formatMarketCap(value) {
  const numeric = parseNumberCandidate(value);
  if (numeric == null) return 'Pending data';
  const absoluteValue = numeric * 1_000_000;
  const compact = new Intl.NumberFormat('en-SG', {
    notation: 'compact',
    compactDisplay: 'short',
    minimumFractionDigits: 0,
    maximumFractionDigits: 1
  }).format(absoluteValue);
  return `S$${compact.toUpperCase()}`;
}

function formatLastUpdatedShort(value) {
  if (!value) return 'Pending data';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return 'Pending data';
  return d.toISOString().slice(0, 10);
}

function sortValue(row, key, type) {
  const raw = row.sortData[key];
  if (type === 'number') {
    const numeric = parseNumberCandidate(raw);
    if (numeric != null) return { empty: false, value: numeric };
  }

  const text = raw == null ? '' : String(raw).trim();
  if (!text || PENDING_RE.test(text)) {
    return { empty: true, value: '' };
  }

  return { empty: false, value: type === 'number' ? Number.NaN : text.toLowerCase() };
}

function compareRows(a, b, sortState) {
  if (!sortState || sortState.direction === 'none') {
    return a.originalIndex - b.originalIndex;
  }

  const { key, direction, type } = sortState;
  const av = sortValue(a, key, type);
  const bv = sortValue(b, key, type);

  if (av.empty && !bv.empty) return 1;
  if (!av.empty && bv.empty) return -1;

  let cmp = 0;
  if (type === 'number') {
    cmp = av.value - bv.value;
  } else {
    cmp = String(av.value).localeCompare(String(bv.value));
  }

  if (cmp === 0) cmp = a.originalIndex - b.originalIndex;
  return direction === 'descending' ? -cmp : cmp;
}

function initSortableHeaders(table, state, onSortChange) {
  const headers = table.querySelectorAll('thead th[data-sort-key]');
  headers.forEach((th) => {
    const label = th.textContent.trim();
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'sort-btn';
    btn.innerHTML = `<span>${label}</span><span class="sort-btn-icon" aria-hidden="true">↕</span>`;
    btn.setAttribute('aria-label', `Sort by ${label}`);
    th.textContent = '';
    th.appendChild(btn);

    btn.addEventListener('click', () => {
      const key = th.dataset.sortKey;
      const type = th.dataset.sortType || 'text';
      const currentDirection = state.sortState && state.sortState.key === key ? state.sortState.direction : 'none';
      const nextDirection = SORT_DIRECTIONS[(SORT_DIRECTIONS.indexOf(currentDirection) + 1) % SORT_DIRECTIONS.length];

      state.sortState = nextDirection === 'none' ? null : { key, type, direction: nextDirection };
      onSortChange();
      updateHeaderIndicators(table, state.sortState);
    });
  });

  updateHeaderIndicators(table, state.sortState);
}

function updateHeaderIndicators(table, sortState) {
  table.querySelectorAll('thead th').forEach((th) => {
    const btn = th.querySelector('.sort-btn');
    if (!btn) return;

    const icon = btn.querySelector('.sort-btn-icon');
    const isActive = sortState && sortState.key === th.dataset.sortKey;
    const direction = isActive ? sortState.direction : 'none';

    th.setAttribute('aria-sort', direction);
    icon.textContent = direction === 'ascending' ? '▲' : direction === 'descending' ? '▼' : '↕';
  });
}

function renderRows(tableBody, rows, sortState) {
  const sortedRows = [...rows].sort((a, b) => compareRows(a, b, sortState));
  tableBody.innerHTML = sortedRows.map((row) => row.markup).join('');

}

function getCurrentMetric(metricObj) {
  if (!metricObj) return null;
  const current = metricObj.values?.Current;
  if (current != null) return current;
  const fallbackKey = Object.keys(metricObj.values || {}).find((label) => /^FY\s+\d{4}$/i.test(label));
  return fallbackKey ? metricObj.values[fallbackKey] : null;
}

function normalizeTicker(value) {
  return String(value || '').trim().toUpperCase().replace(/\.SI$/, '');
}

function readFirstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null);
}

function resolveHealthScore(entry) {
  if (!entry) return null;
  return readFirstDefined(
    entry.healthScore,
    entry.health_score,
    entry.score,
    entry.scoring?.healthScore,
    entry.scoring?.health_score,
    entry.scoring?.score,
    entry.current?.healthScore,
    entry.current?.health_score,
    entry.current?.score
  );
}


function resolveScoreCoverage(entry) {
  if (!entry) return null;
  return readFirstDefined(
    entry.scoreCoverage,
    entry.score_coverage,
    entry.scoring?.scoreCoverage,
    entry.scoring?.score_coverage,
    entry.current?.scoreCoverage,
    entry.current?.score_coverage
  );
}

function buildScoreTooltip(entry) {
  const components = entry?.healthScoreComponents;
  if (!components) return null;
  const staticScore = components.staticHealthScore;
  const trendPenalty = components.trendPenalty;
  const rawTrendPenalty = components.rawTrendPenalty;
  const worseningMetricCount = components.worseningMetricCount;
  const coverage = resolveScoreCoverage(entry);
  const pillarContributors = Object.entries(components.pillarContributors || {})
    .map(([pillar, detail]) => ({
      pillar,
      weightedRiskContribution: detail?.weightedRiskContribution,
      pillarRiskScore: detail?.pillarRiskScore,
      metricKeys: detail?.metricKeys || [],
      aggregation: detail?.aggregation
    }))
    .filter((detail) => Number.isFinite(detail.weightedRiskContribution))
    .sort((a, b) => b.weightedRiskContribution - a.weightedRiskContribution)
    .slice(0, 3)
    .map((detail) => {
      const pillarInfo = PILLAR_EXPLANATIONS[detail.pillar] || { label: detail.pillar };
      const metricLabels = detail.metricKeys
        .map((metricKey) => resolveMetricExplanation(metricKey).label)
        .join(', ');
      const aggregationNote = detail.aggregation
        ? `${detail.aggregation} of ${metricLabels || 'pillar metrics'}`
        : metricLabels;
      const riskScore = Number.isFinite(detail.pillarRiskScore)
        ? `pillar risk ${detail.pillarRiskScore.toFixed(1)}`
        : null;
      return `${pillarInfo.label}: ${detail.weightedRiskContribution.toFixed(2)} pts${riskScore ? ` (${riskScore}; ${aggregationNote})` : ''}`;
    });
  const legacyTop3 = !pillarContributors.length
    ? Object.entries(components.weightedContributors || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([metric, contribution]) => {
        const label = resolveMetricExplanation(metric).label;
        return `${label}: ${contribution.toFixed(2)} pts`;
      })
    : [];
  const topContributors = pillarContributors.length ? pillarContributors : legacyTop3;
  const worseningLabel = worseningMetricCount === 1 ? 'metric' : 'metrics';
  const trendContext = Number.isFinite(trendPenalty) && Number.isFinite(worseningMetricCount)
    ? (worseningMetricCount >= 2
      ? `Trend penalty: ${trendPenalty.toFixed(1)} (raw ${Number.isFinite(rawTrendPenalty) ? rawTrendPenalty.toFixed(1) : 'n/a'}, applied because ${worseningMetricCount} key ${worseningLabel} worsened)`
      : `Trend penalty: ${trendPenalty.toFixed(1)} (not applied: only ${worseningMetricCount} key ${worseningLabel} worsened)`)
    : null;
  const topContributorLine = topContributors.length
    ? `Top weighted deductions (points removed from static score): ${topContributors.join('; ')}`
    : null;

  const excluded = Array.isArray(components.excludedMetrics) ? components.excludedMetrics : [];
  const debtToEbitdaReferenceOnly = excluded.includes('debtToEbitda')
    ? 'Debt / EBITDA shown for reference only (not used in score)'
    : null;

  return [
    Number.isFinite(staticScore) ? `Static: ${staticScore.toFixed(1)}` : null,
    trendContext || (Number.isFinite(trendPenalty) ? `Trend penalty: ${trendPenalty.toFixed(1)}` : null),
    Number.isFinite(coverage) ? `Coverage: ${(coverage * 100).toFixed(0)}%` : null,
    topContributorLine,
    debtToEbitdaReferenceOnly
  ].filter(Boolean).join(' | ');
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function resolveMetricExplanation(metricKey) {
  return METRIC_EXPLANATIONS[metricKey] || {
    label: metricKey,
    category: 'Other',
    rationale: 'Model input metric.'
  };
}

function renderStatusLegend(statusBands = {}) {
  if (statusBands.green == null || statusBands.amber == null) return '';
  return `
    <p class="methodology-line">
      <strong>Status labels (UI):</strong> Stable (model Green, score >= ${statusBands.green}),
      Watch (model Amber, score >= ${statusBands.amber}),
      Elevated (model Red, score &lt; ${statusBands.amber}).
    </p>
  `;
}

function buildMethodologyHtml(scoringModel) {
  if (!scoringModel) {
    return '<p class="methodology-line">Health score uses weighted risk metrics plus a capped trend penalty. Detailed methodology is unavailable when ratios data is not loaded.</p>';
  }

  const pillarWeights = scoringModel.pillarWeights || {};
  const pillarDefinitions = scoringModel.pillars || {};
  const weightedPillars = Object.entries(pillarWeights)
    .filter(([, weight]) => Number.isFinite(weight) && weight > 0)
    .sort((a, b) => b[1] - a[1]);
  const pillarRows = weightedPillars.map(([pillar, weight]) => {
    const pillarInfo = PILLAR_EXPLANATIONS[pillar] || { label: pillar, rationale: 'Policy scoring pillar.' };
    const definition = pillarDefinitions[pillar] || {};
    const metrics = Array.isArray(definition.metrics) ? definition.metrics : [];
    const metricLabels = metrics.map((metricKey) => resolveMetricExplanation(metricKey).label).join(', ');
    const aggregation = definition.aggregation || 'average';
    const weightPct = `${(weight * 100).toFixed(0)}%`;
    return `<li><strong>${escapeHtml(pillarInfo.label)}</strong> (${weightPct}) - ${escapeHtml(pillarInfo.rationale)} Inputs: ${escapeHtml(metricLabels)}. Aggregation: ${escapeHtml(aggregation)}.</li>`;
  }).join('');
  const weightSummary = weightedPillars
    .map(([pillar, weight]) => `${(PILLAR_EXPLANATIONS[pillar]?.label || pillar)}: ${(weight * 100).toFixed(0)}%`)
    .join(' | ');
  const statusBands = scoringModel.bands?.status || {};
  const coverageThreshold = Number.isFinite(scoringModel.coverageThreshold)
    ? `${Math.round(scoringModel.coverageThreshold * 100)}%`
    : null;
  const trendPenaltyCap = Number.isFinite(scoringModel.trendPenalty?.cap)
    ? String(scoringModel.trendPenalty.cap)
    : (Number.isFinite(scoringModel.trendPenaltyCap) ? String(scoringModel.trendPenaltyCap) : null);
  const trendMultiplier = Number.isFinite(scoringModel.trendPenalty?.multiplier)
    ? String(scoringModel.trendPenalty.multiplier)
    : null;
  const trendMinWorsening = Number.isFinite(scoringModel.trendPenalty?.minWorseningMetrics)
    ? String(scoringModel.trendPenalty.minWorseningMetrics)
    : null;
  const excludedMetricKeys = Array.isArray(scoringModel.excludedMetrics) ? scoringModel.excludedMetrics : [];
  const excluded = excludedMetricKeys.length
    ? excludedMetricKeys.map((metricKey) => resolveMetricExplanation(metricKey).label).join(', ')
    : null;
  const simplificationNote = excludedMetricKeys.length
    ? 'Simplification update: reference-only metrics are still shown in the table, but excluded from score computation.'
    : null;
  const negativeLeverageHandling = scoringModel.negativeLeverageHandling;
  const negativeTargets = Array.isArray(negativeLeverageHandling?.targetMetrics)
    ? negativeLeverageHandling.targetMetrics
    : [];
  const negativeSupportMetrics = Array.isArray(negativeLeverageHandling?.supportMetrics)
    ? negativeLeverageHandling.supportMetrics
    : [];
  const noSupportFloor = negativeLeverageHandling?.riskFloors?.noSupport;
  const mixedSupportFloor = negativeLeverageHandling?.riskFloors?.mixedSupport;
  const weakSupportFloor = negativeLeverageHandling?.riskFloors?.weakSupport;
  const netCashSoftener = negativeLeverageHandling?.netCashLeverageSoftener;
  const negativeLeverageSummary = negativeLeverageHandling
    ? `Negative leverage handling: ${negativeTargets.join(', ')} require support from ${negativeSupportMetrics.join(', ')}; net-cash leverage pillar softener=${netCashSoftener ?? 'n/a'}; risk floors no/mixed/weak support = ${noSupportFloor}/${mixedSupportFloor}/${weakSupportFloor}`
    : null;
  const thresholdSummaries = weightedPillars
    .flatMap(([pillar]) => {
      const metrics = Array.isArray(pillarDefinitions[pillar]?.metrics) ? pillarDefinitions[pillar].metrics : [];
      return metrics;
    })
    .map((metricKey) => {
      const explanation = resolveMetricExplanation(metricKey);
      const threshold = scoringModel.metricThresholds?.[metricKey];
      if (!threshold) return null;
      const directionText = threshold.direction === 'higherWorse' ? 'higher is worse' : 'lower is worse';
      return `${explanation.label}: ${directionText}; 0 risk at ${threshold.direction === 'higherWorse' ? '<=' : '>='}${threshold.good}, 100 risk at ${threshold.direction === 'higherWorse' ? '>=' : '<='}${threshold.bad}`;
    })
    .filter(Boolean);

  return `
    <p class="methodology-line"><strong>Formula:</strong> ${escapeHtml(scoringModel.formula || 'round(clamp((100 - weightedPillarRisk) - trendPenalty, 0, 100))')}</p>
    <p class="methodology-line"><strong>Weighted risk:</strong> ${escapeHtml(scoringModel.weightedRisk || 'sum(pillarWeight * pillarRisk) / sum(availablePillarWeight)')}</p>
    ${renderStatusLegend(statusBands)}
    <p class="methodology-line"><strong>Pillar weights:</strong> ${escapeHtml(weightSummary || 'Not available')}</p>
    ${coverageThreshold ? `<p class="methodology-line"><strong>Minimum coverage to score:</strong> ${escapeHtml(coverageThreshold)}</p>` : ''}
    ${trendPenaltyCap ? `<p class="methodology-line"><strong>Trend penalty:</strong> cap ${escapeHtml(trendPenaltyCap)}${trendMultiplier ? `, multiplier ${escapeHtml(trendMultiplier)}` : ''}${trendMinWorsening ? `, applied only when at least ${escapeHtml(trendMinWorsening)} key metrics worsen` : ''}.</p>` : ''}
    ${excluded ? `<p class="methodology-line"><strong>Reference-only metrics (excluded from score):</strong> ${escapeHtml(excluded)}</p>` : ''}
    ${simplificationNote ? `<p class="methodology-line">${escapeHtml(simplificationNote)}</p>` : ''}
    ${negativeLeverageSummary ? `<p class="methodology-line"><strong>Negative leverage handling:</strong> ${escapeHtml(negativeLeverageSummary.replace(/^Negative leverage handling:\s*/, ''))}</p>` : ''}
    ${thresholdSummaries.length ? `<p class="methodology-line"><strong>Risk calibration (sector-adjusted):</strong> ${escapeHtml(thresholdSummaries.join(' | '))}</p>` : ''}
    <ul class="methodology-list">${pillarRows}</ul>
    <p class="methodology-line"><strong>Interpretation:</strong> "Top weighted deductions" in score hover means how many score points each pillar removed from the static 100 baseline before trend penalty.</p>
  `;
}

function toDisplayStatusLabel(rawStatus) {
  if (!rawStatus) return STATUS_LABEL_MAP['Pending data'];
  return STATUS_LABEL_MAP[rawStatus] || rawStatus;
}

function resolveHealthStatus(entry) {
  if (!entry) return null;
  return readFirstDefined(
    entry.healthStatus,
    entry.health_status,
    entry.status,
    entry.scoring?.healthStatus,
    entry.scoring?.health_status,
    entry.scoring?.status,
    entry.current?.healthStatus,
    entry.current?.health_status,
    entry.current?.status
  );
}

function buildRatiosMap(payload) {
  if (!payload || !Array.isArray(payload.developers)) return new Map();
  return new Map(payload.developers.map((dev) => [normalizeTicker(dev.ticker), dev]));
}

async function initDevelopersPage() {
  const tableBody = document.getElementById('developers-body');
  const methodology = document.getElementById('methodology-content');
  const table = tableBody?.closest('table');
  if (!tableBody || !table) return;

  try {
    const [data, ratiosData] = await Promise.all([
      App.fetchJson('./data/listed_developers.json'),
      App.fetchJson('./data/processed/developer_ratios_history.json').catch(() => null)
    ]);
    const ratiosMap = buildRatiosMap(ratiosData);
    const minimumCoverage = Number.isFinite(ratiosData?.scoringModel?.coverageThreshold)
      ? ratiosData.scoringModel.coverageThreshold
      : 0.5;
    methodology.innerHTML = buildMethodologyHtml(ratiosData?.scoringModel);

    const rows = data.developers.map((dev, index) => {
      const ratioEntry = ratiosMap.get(normalizeTicker(dev.ticker));
      const scoreValue = resolveHealthScore(ratioEntry);
      const scoreCoverage = resolveScoreCoverage(ratioEntry);
      const insufficientCoverage = Number.isFinite(scoreCoverage) && scoreCoverage < minimumCoverage;
      const rawStatus = insufficientCoverage ? 'Pending data' : (resolveHealthStatus(ratioEntry) || 'Pending data');
      const status = toDisplayStatusLabel(rawStatus);
      const score = insufficientCoverage || scoreValue == null ? 'Pending' : String(scoreValue);
      const cls = rawStatus === 'Green'
        ? 'status-green'
        : rawStatus === 'Red'
          ? 'status-red'
          : rawStatus === 'Pending data'
            ? 'status-pending'
            : 'status-amber';
      const currentMarketCap = ratioEntry?.current?.marketCap ?? getCurrentMetric(ratioEntry?.metrics?.marketCap);
      const currentNetDebtToEbitda = ratioEntry?.current?.netDebtToEbitda ?? getCurrentMetric(ratioEntry?.metrics?.netDebtToEbitda);
      const currentDebtToEquity = ratioEntry?.current?.debtToEquity ?? getCurrentMetric(ratioEntry?.metrics?.debtToEquity);
      const currentNetDebtToEquity = ratioEntry?.current?.netDebtToEquity ?? getCurrentMetric(ratioEntry?.metrics?.netDebtToEquity);
      const currentDebtToEbitda = ratioEntry?.current?.debtToEbitda ?? getCurrentMetric(ratioEntry?.metrics?.debtToEbitda);
      const currentQuickRatio = ratioEntry?.current?.quickRatio ?? getCurrentMetric(ratioEntry?.metrics?.quickRatio);
      const currentCurrentRatio = ratioEntry?.current?.currentRatio ?? getCurrentMetric(ratioEntry?.metrics?.currentRatio);
      const currentRoic = ratioEntry?.current?.roic ?? getCurrentMetric(ratioEntry?.metrics?.roic);
      const currentRoe = ratioEntry?.current?.roe ?? getCurrentMetric(ratioEntry?.metrics?.roe);
      const currentPayoutRatio = ratioEntry?.current?.payoutRatio ?? getCurrentMetric(ratioEntry?.metrics?.payoutRatio);
      const currentAssetTurnover = ratioEntry?.current?.assetTurnover ?? getCurrentMetric(ratioEntry?.metrics?.assetTurnover);
      const currentLastUpdated = ratioEntry?.lastFetchedAt || dev.lastUpdated;
      const marketCapDisplay = formatMarketCap(currentMarketCap);
      const netDebtToEbitdaDisplay = formatNumber(currentNetDebtToEbitda);
      const debtToEquityDisplay = formatNumber(currentDebtToEquity);
      const netDebtToEquityDisplay = formatNumber(currentNetDebtToEquity);
      const debtToEbitdaDisplay = formatNumber(currentDebtToEbitda);
      const quickRatioDisplay = formatNumber(currentQuickRatio);
      const currentRatioDisplay = formatNumber(currentCurrentRatio);
      const roicDisplay = formatPercent(currentRoic);
      const roeDisplay = formatPercent(currentRoe);
      const payoutRatioDisplay = formatPayoutRatio(currentPayoutRatio);
      const assetTurnoverDisplay = formatNumber(currentAssetTurnover);
      const lastUpdatedDisplay = formatLastUpdatedShort(currentLastUpdated);
      const scoreTooltip = buildScoreTooltip(ratioEntry);
      const safeScoreTooltip = escapeHtml(scoreTooltip || '');
      return {
        originalIndex: index,
        sortData: {
          name: dev.name,
          ticker: dev.ticker,
          segment: dev.segment,
          score,
          status: rawStatus,
          marketCap: currentMarketCap,
          netDebtToEbitda: currentNetDebtToEbitda,
          debtToEquity: currentDebtToEquity,
          netDebtToEquity: currentNetDebtToEquity,
          debtToEbitda: currentDebtToEbitda,
          quickRatio: currentQuickRatio,
          currentRatio: currentCurrentRatio,
          roic: currentRoic,
          roe: currentRoe,
          payoutRatio: currentPayoutRatio,
          assetTurnover: currentAssetTurnover,
          lastUpdated: lastUpdatedDisplay
        },
        markup: `
          <tr>
            <td data-sticky-col="1">${dev.name}</td><td>${dev.ticker}</td><td>${dev.segment}</td>
            <td><strong title="${safeScoreTooltip}">${score}</strong></td><td><span class="status-pill ${cls}" title="${safeScoreTooltip}">${status}</span></td>
            <td>${marketCapDisplay}</td><td>${netDebtToEbitdaDisplay}</td><td>${debtToEquityDisplay}</td>
            <td>${netDebtToEquityDisplay}</td><td>${debtToEbitdaDisplay}</td><td>${quickRatioDisplay}</td><td>${currentRatioDisplay}</td>
            <td>${roicDisplay}</td><td>${roeDisplay}</td><td>${payoutRatioDisplay}</td><td>${assetTurnoverDisplay}</td>
            <td>${lastUpdatedDisplay}</td>
          </tr>
        `
      };
    });

    const state = { sortState: null };
    const onSortChange = () => renderRows(tableBody, rows, state.sortState);

    initSortableHeaders(table, state, onSortChange);
    renderRows(tableBody, rows, state.sortState);
  } catch (e) {
    tableBody.innerHTML = `<tr><td colspan="17" class="empty">Unable to load developer data: ${e.message}</td></tr>`;
  }
}

document.addEventListener('DOMContentLoaded', initDevelopersPage);
