const STRESS_TOOLTIPS = {
  sector_performance: 'Stress if construction_gdp is negative for 2 consecutive quarters.',
  labour_cost: 'Stress if Construction Unit Labour Cost (ULC) YoY growth ≥ 8% for 2 consecutive quarters.',
  interest_rate: 'Stress if SORA overnight is above its 5-year 80th percentile AND the 6-month change ≥ +0.75pp.'
};

function formatSgtTimestamp(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Singapore',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).formatToParts(date);
  const get = (type) => parts.find((part) => part.type === type)?.value;
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}`;
}

function quarterIndex(period) {
  if (!period) return null;
  const raw = String(period).trim();
  let match = raw.match(/^(\d{4})-Q([1-4])$/);
  if (!match) match = raw.match(/^(\d{4})Q([1-4])$/);
  if (!match) {
    const monthMatch = raw.match(/^(\d{4})-(\d{2})$/);
    if (!monthMatch) return null;
    const month = Number(monthMatch[2]);
    if (!Number.isFinite(month) || month < 1 || month > 12) return null;
    const quarter = Math.floor((month - 1) / 3) + 1;
    return Number(monthMatch[1]) * 4 + (quarter - 1);
  }
  return Number(match[1]) * 4 + (Number(match[2]) - 1);
}

function quarterPeriodFromIndex(index) {
  const year = Math.floor(index / 4);
  const quarter = (index % 4) + 1;
  return `${year}-Q${quarter}`;
}

function normalizeQuarterlyValues(values = []) {
  const deduped = new Map();
  for (const row of values) {
    const idx = quarterIndex(row?.period);
    const numeric = Number(row?.value);
    if (idx == null || !Number.isFinite(numeric)) continue;
    deduped.set(idx, { period: quarterPeriodFromIndex(idx), value: numeric });
  }
  return [...deduped.entries()].sort((a, b) => a[0] - b[0]);
}

function monthIndex(period) {
  const match = String(period || '').match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
  return year * 12 + (month - 1);
}

function monthPeriodFromIndex(index) {
  const year = Math.floor(index / 12);
  const month = (index % 12) + 1;
  return `${year}-${String(month).padStart(2, '0')}`;
}

function normalizeMonthlyValues(values = []) {
  const deduped = new Map();
  for (const row of values) {
    const rawPeriod = row?.period || String(row?.date || '').slice(0, 7);
    const idx = monthIndex(rawPeriod);
    const numeric = Number(row?.value);
    if (idx == null || !Number.isFinite(numeric)) continue;
    deduped.set(idx, { period: monthPeriodFromIndex(idx), value: numeric });
  }
  return [...deduped.entries()].sort((a, b) => a[0] - b[0]);
}

function percentileLinearInterpolation(sortedValues, q) {
  if (!sortedValues.length) return null;
  if (sortedValues.length === 1) return sortedValues[0];
  const position = (sortedValues.length - 1) * q;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sortedValues[lower];
  const weight = position - lower;
  return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
}

function computeSectorPerformanceSignal(values = []) {
  const normalized = normalizeQuarterlyValues(values);
  const latest = normalized[normalized.length - 1]?.[1] || null;
  const prev = normalized[normalized.length - 2]?.[1] || null;
  const note = (!latest || !prev) ? 'Missing latest or previous quarter for construction_gdp; defaulted to Normal.' : undefined;
  const status = latest && prev && latest.value < 0 && prev.value < 0 ? 'Stress' : 'Normal';

  return {
    status,
    series_id: 'construction_gdp',
    as_of: latest?.period || null,
    details: { latest, prev },
    tooltip: STRESS_TOOLTIPS.sector_performance,
    ...(note ? { note } : {})
  };
}

function computeLabourCostSignal(values = []) {
  const normalizedEntries = normalizeQuarterlyValues(values);
  const byQuarter = new Map(normalizedEntries);
  const latestQuarter = normalizedEntries[normalizedEntries.length - 1]?.[0] ?? null;

  if (latestQuarter == null) {
    return {
      status: 'Normal',
      series_id: 'unit_labour_cost_construction',
      as_of: null,
      details: { yoy_latest: null, yoy_prev: null },
      tooltip: STRESS_TOOLTIPS.labour_cost,
      note: 'Missing latest quarter for unit_labour_cost_construction; defaulted to Normal.'
    };
  }

  const required = [latestQuarter, latestQuarter - 1, latestQuarter - 4, latestQuarter - 5];
  const missing = required.filter((idx) => !byQuarter.has(idx));
  if (missing.length) {
    return {
      status: 'Normal',
      series_id: 'unit_labour_cost_construction',
      as_of: quarterPeriodFromIndex(latestQuarter),
      details: { yoy_latest: null, yoy_prev: null },
      tooltip: STRESS_TOOLTIPS.labour_cost,
      note: `Missing required quarter(s) for ULC YoY calculation: ${missing.map(quarterPeriodFromIndex).join(', ')}; defaulted to Normal.`
    };
  }

  const yoyLatest = byQuarter.get(latestQuarter).value / byQuarter.get(latestQuarter - 4).value - 1;
  const yoyPrev = byQuarter.get(latestQuarter - 1).value / byQuarter.get(latestQuarter - 5).value - 1;
  const status = yoyLatest >= 0.08 && yoyPrev >= 0.08 ? 'Stress' : 'Normal';

  return {
    status,
    series_id: 'unit_labour_cost_construction',
    as_of: quarterPeriodFromIndex(latestQuarter),
    details: {
      yoy_latest: { period: quarterPeriodFromIndex(latestQuarter), value: Number(yoyLatest.toFixed(6)) },
      yoy_prev: { period: quarterPeriodFromIndex(latestQuarter - 1), value: Number(yoyPrev.toFixed(6)) }
    },
    tooltip: STRESS_TOOLTIPS.labour_cost
  };
}

function computeInterestRateSignal(values = []) {
  const normalized = normalizeMonthlyValues(values);
  const latestEntry = normalized[normalized.length - 1]?.[1] || null;

  if (!latestEntry) {
    return {
      status: 'Normal',
      series_id: 'sora_overnight',
      as_of: null,
      details: { current: null, p80_5y: null, chg_6m_pp: null },
      tooltip: STRESS_TOOLTIPS.interest_rate,
      note: 'Missing latest monthly SORA observation; defaulted to Normal.'
    };
  }

  const trailingWindow = normalized.slice(-60).map((entry) => entry[1].value).sort((a, b) => a - b);
  const p80 = percentileLinearInterpolation(trailingWindow, 0.8);

  const byMonth = new Map(normalized);
  const latestMonthIdx = normalized[normalized.length - 1][0];
  const lag6 = byMonth.get(latestMonthIdx - 6)?.value;
  if (!Number.isFinite(lag6)) {
    return {
      status: 'Normal',
      series_id: 'sora_overnight',
      as_of: latestEntry.period,
      details: {
        current: latestEntry,
        p80_5y: Number(p80.toFixed(6)),
        chg_6m_pp: null
      },
      tooltip: STRESS_TOOLTIPS.interest_rate,
      note: `Missing t-6 month (${monthPeriodFromIndex(latestMonthIdx - 6)}) for SORA 6-month change; defaulted to Normal.`
    };
  }

  const change6m = latestEntry.value - lag6;
  const status = latestEntry.value > p80 && change6m >= 0.75 ? 'Stress' : 'Normal';
  return {
    status,
    series_id: 'sora_overnight',
    as_of: latestEntry.period,
    details: {
      current: latestEntry,
      p80_5y: Number(p80.toFixed(6)),
      chg_6m_pp: Number(change6m.toFixed(6))
    },
    tooltip: STRESS_TOOLTIPS.interest_rate
  };
}

function generateMacroStressSignals(seriesById = {}, now = new Date()) {
  return {
    last_updated_sgt: formatSgtTimestamp(now),
    signals: {
      sector_performance: computeSectorPerformanceSignal(seriesById.construction_gdp?.values || []),
      labour_cost: computeLabourCostSignal(seriesById.unit_labour_cost_construction?.values || []),
      interest_rate: computeInterestRateSignal(seriesById.sora_overnight?.values || [])
    }
  };
}

module.exports = {
  STRESS_TOOLTIPS,
  percentileLinearInterpolation,
  computeSectorPerformanceSignal,
  computeLabourCostSignal,
  computeInterestRateSignal,
  generateMacroStressSignals
};
