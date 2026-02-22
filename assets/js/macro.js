const MAJOR_CATEGORY = {
  A: 'A_real_economy_pipeline_absorption',
  B: 'B_margin_pressures',
  C: 'C_funding_credit',
  D: 'D_macro_regime'
};

const CATEGORY_UI = {
  [MAJOR_CATEGORY.A]: { short: 'A: Pipeline & absorption', label: 'Pipeline & absorption' },
  [MAJOR_CATEGORY.B]: { short: 'B: Margin pressures', label: 'Margin pressures' },
  [MAJOR_CATEGORY.C]: { short: 'C: Funding & credit', label: 'Funding & credit' },
  [MAJOR_CATEGORY.D]: { short: 'D: Macro regime', label: 'Macro regime' }
};

const LEGACY_CATEGORY_TO_MAJOR = {
  A_pipeline_absorption: MAJOR_CATEGORY.A,
  B_margin_pressures: MAJOR_CATEGORY.B,
  C_funding_credit: MAJOR_CATEGORY.C,
  D_macro_regime: MAJOR_CATEGORY.D
};

const MACRO_INDICATOR_METADATA = [
  ['commind_business_park_planned_others', 'Business Park Space Pipeline (Planned: Other)', 'Planned business park space that is still early-stage. A larger pipeline can increase future supply pressure if take-up slows.', 'K Sqm Gross'],
  ['commind_business_park_planned_provisional_permission', 'Business Park Space Pipeline (Planned: Provisional Permission)', 'Planned business park space with provisional planning approval. Rising volumes can add future supply and weigh on rents if demand softens.', 'K Sqm Gross'],
  ['commind_business_park_planned_written_permission', 'Business Park Space Pipeline (Planned: Written Permission)', 'Planned business park space with written planning approval. Higher levels signal more committed future supply and potential leasing competition.', 'K Sqm Gross'],
  ['commind_business_park_total', 'Business Park Space Pipeline (Total)', 'Total business park pipeline across planning stages. Useful for gauging forthcoming supply and the risk of overhang.', 'K Sqm Gross'],
  ['commind_business_park_under_construction', 'Business Park Space Pipeline (Under Construction)', 'Business park space currently being built. A rising under-construction stock can increase near-term completion and leasing competition.', 'K Sqm Gross'],
  ['commind_office_planned_others', 'Office Space Pipeline (Planned: Other)', 'Planned office space that is still early-stage. A larger pipeline can raise future supply risk if absorption weakens.', 'K Sqm Gross'],
  ['commind_office_planned_provisional_permission', 'Office Space Pipeline (Planned: Provisional Permission)', 'Planned office space with provisional planning approval. Higher levels can translate into more deliverable supply over time.', 'K Sqm Gross'],
  ['commind_office_planned_written_permission', 'Office Space Pipeline (Planned: Written Permission)', 'Planned office space with written planning approval. Indicates a more advanced pipeline and potential future supply pressure.', 'K Sqm Gross'],
  ['commind_office_total', 'Office Space Pipeline (Total)', 'Total office pipeline across planning stages. Signals future supply and potential impact on vacancy and rents.', 'K Sqm Gross'],
  ['commind_office_under_construction', 'Office Space Pipeline (Under Construction)', 'Office space currently being built. Rising levels can increase near-term completions and raise vacancy risk if leasing demand slows.', 'K Sqm Gross'],
  ['commind_retail_planned_others', 'Retail Space Pipeline (Planned: Other)', 'Planned retail space that is still early-stage. A larger pipeline can increase overhang risk if retail leasing demand weakens.', 'K Sqm Gross'],
  ['commind_retail_planned_provisional_permission', 'Retail Space Pipeline (Planned: Provisional Permission)', 'Planned retail space with provisional planning approval. Higher levels can add to future supply and weigh on rents if take-up is slow.', 'K Sqm Gross'],
  ['commind_retail_planned_written_permission', 'Retail Space Pipeline (Planned: Written Permission)', 'Planned retail space with written planning approval. Indicates more committed future supply and potential leasing competition.', 'K Sqm Gross'],
  ['commind_retail_total', 'Retail Space Pipeline (Total)', 'Total retail pipeline across planning stages. Helps assess future supply pressure and potential vacancy/rent risk.', 'K Sqm Gross'],
  ['commind_retail_under_construction', 'Retail Space Pipeline (Under Construction)', 'Retail space currently being built. More completions can pressure occupancy and rents if demand does not keep pace.', 'K Sqm Gross'],
  ['industrial_pipeline_others', 'Industrial Space Pipeline (Planned: Other)', 'Planned industrial space at an early stage. Growth can signal more future supply and potential competitive pressure on rents.', 'K Sqm Gross'],
  ['industrial_pipeline_provisional_permission', 'Industrial Space Pipeline (Planned: Provisional Permission)', 'Planned industrial space with provisional planning approval. Higher levels can point to a larger future delivery pipeline.', 'K Sqm Gross'],
  ['industrial_pipeline_total', 'Industrial Space Pipeline (Total)', 'Total industrial pipeline across planning stages. Tracks future supply risk and potential overhang.', 'K Sqm Gross'],
  ['industrial_pipeline_under_construction', 'Industrial Space Pipeline (Under Construction)', 'Industrial space currently being built. Rising levels can increase near-term completions and vacancy risk if demand slows.', 'K Sqm Gross'],
  ['industrial_pipeline_written_permission', 'Industrial Space Pipeline (Planned: Written Permission)', 'Planned industrial space with written planning approval. Signals a more committed pipeline and higher likelihood of future supply.', 'K Sqm Gross'],
  ['prp_pipeline_non_landed_planned_others', 'Private Residential (Non-Landed) Pipeline (Planned: Other)', 'Early-stage non-landed private residential pipeline. Larger values can increase future housing supply and competitive pressure.', 'Units'],
  ['prp_pipeline_non_landed_planned_provisional_permission', 'Private Residential (Non-Landed) Pipeline (Planned: Provisional Permission)', 'Non-landed private residential pipeline with provisional planning approval. Rising levels can add to future supply and increase overhang risk.', 'Units'],
  ['prp_pipeline_non_landed_planned_written_permission', 'Private Residential (Non-Landed) Pipeline (Planned: Written Permission)', 'Non-landed private residential pipeline with written planning approval. Indicates a more advanced pipeline and greater likelihood of delivery.', 'Units'],
  ['prp_pipeline_non_landed_under_construction', 'Private Residential (Non-Landed) Pipeline (Under Construction)', 'Non-landed private residential units under construction. Higher levels increase near-term completions and sales absorption requirements.', 'Units'],
  ['prp_pipeline_total_non_landed', 'Private Residential (Non-Landed) Pipeline (Total)', 'Total non-landed private residential pipeline across stages. Tracks future supply and potential inventory overhang if take-up slows.', 'Units'],
  ['private_vacant_private_sector_business_park_space_vacant', 'Business Park Vacant Space (Private Sector)', 'Amount of private-sector business park space that is vacant. Higher vacancy typically signals weaker leasing demand and downside risk to rents and asset values.', 'K Sqm Nett'],
  ['private_vacant_private_sector_multiple_user_factory_space_vacant', 'Multiple-User Factory Vacant Space (Private Sector)', 'Amount of private-sector multiple-user factory space that is vacant. Rising vacancy can indicate softer industrial demand and pressure on rents.', 'K Sqm Nett'],
  ['private_vacant_private_sector_office_space_vacant', 'Office Vacant Space (Private Sector)', 'Amount of private-sector office space that is vacant. Higher vacancy can weigh on rents, incentives, and valuation assumptions for commercial assets.', 'K Sqm Nett'],
  ['private_vacant_private_sector_retail_space_vacant', 'Retail Vacant Space  (Private Sector)', 'Amount of private-sector retail space that is vacant. Higher vacancy can signal weak tenant demand and greater downside risk to rents and income.', 'K Sqm Nett'],
  ['construction_material_cement_in_bulk_ordinary_portland_cement', 'Construction Material Price: Ordinary Portland Cement (Bulk)', 'Tracks cement prices. Sustained increases raise construction costs and can compress development margins, especially for projects without cost pass-through.', '$/Tonne'],
  ['construction_material_concreting_sand', 'Construction Material Price: Concreting Sand', 'Tracks concreting sand prices. Rising costs can lift project costs and increase budget and tender risks.', '$/Tonne'],
  ['construction_material_steel_reinforcement_bars_16_32mm_high_tensile', 'Construction Material Price: Steel Reinforcement Bars (16–32mm, High Tensile)', 'Tracks rebar prices. Higher steel costs can materially increase structural costs and pressure margins for ongoing and future builds.', '$/Tonne'],
  ['construction_material_granite_20mm_aggregate', 'Construction Material Price: Granite Aggregate (20mm)', 'Tracks aggregate prices. Increases raise concreting costs and can contribute to construction cost inflation.', '$/Tonne'],
  ['construction_material_ready_mixed_concrete', 'Construction Material Price: Ready-Mixed Concrete', 'Tracks ready-mixed concrete prices. Higher prices raise direct build costs and can reduce profitability if selling prices do not adjust.', '$/Tonne'],
  ['unit_labour_cost_construction', 'Construction Unit Labour Cost', 'Measures labour cost per unit of construction output. Persistent increases raise overall construction costs and can squeeze margins and delay project timelines.', 'Index'],
  ['demand_construction_materials_cement', 'Construction Materials Demand: Cement', 'Measures demand for cement. Strong demand can indicate a heated construction cycle, potential capacity constraints, and upward pressure on input costs.', 'K Tonnes'],
  ['demand_construction_materials_granite', 'Construction Materials Demand: Granite', 'Measures demand for granite aggregate. Higher demand can signal stronger construction activity and potential cost pressure from supply tightness.', 'K Tonnes'],
  ['demand_construction_materials_ready_mixed_concrete', 'Construction Materials Demand: Ready-Mixed Concrete', 'Measures demand for ready-mixed concrete. Rising demand can coincide with tighter capacity and higher concreting costs.', 'K Tonnes'],
  ['demand_construction_materials_steel_reinforcement_bars', 'Construction Materials Demand: Steel Reinforcement Bars', 'Measures demand for reinforcing steel. Strong demand can signal a busy construction pipeline and possible upward pressure on steel-related costs.', 'K Tonnes'],
  ['sgs_10y', 'Singapore Government Securities (SGS) 10-Year Yield', 'Long-term risk-free yield benchmark. Higher yields raise discount rates and borrowing costs, which can pressure valuations and refinancing conditions.', '%'],
  ['sgs_2y', 'Singapore Government Securities (SGS) 2-Year Yield', 'Shorter-term risk-free yield benchmark. Higher yields typically translate into higher floating/short-tenor funding costs and tighter affordability for leveraged borrowers.', '%'],
  ['term_spread_10y_2y', 'Yield Curve Slope (10-Year minus 2-Year)', 'Measures the steepness of the yield curve. A flatter or inverted curve often coincides with tighter credit conditions and weaker growth expectations.', 'pp'],
  ['sora_overnight', 'Singapore Overnight Rate Average (SORA)', 'Key overnight interest rate benchmark. Higher rates increase debt servicing costs and can raise refinancing risk for leveraged developers.', '%'],
  ['loan_bc_construction', 'Bank Loans to Building & Construction (Construction)', 'Outstanding bank loans to the construction segment within building & construction. Slowing growth or contraction can signal tighter credit supply or weaker borrowing demand.', 'S$M'],
  ['loan_bc_real_property', 'Bank Loans to Building & Construction (Real Property)', 'Outstanding bank loans to the real property segment within building & construction. Useful for tracking credit conditions facing property-related borrowers.', 'S$M'],
  ['loan_bc_total', 'Bank Loans to Building & Construction (Total)', 'Total outstanding bank loans to building & construction. Indicates overall credit exposure and whether lending to the sector is expanding or tightening.', 'S$M'],
  ['loan_limits_granted_building_construction', 'Loan Limits Granted to Building & Construction', 'Total credit limits granted by banks to the building & construction sector. A decline can indicate reduced bank risk appetite or tighter underwriting.', 'S$M'],
  ['loan_limits_utilised_building_construction', 'Utilisation Rate of Loan Limits (Building & Construction)', 'Share of granted credit limits that is utilised. Rising utilisation can indicate tighter liquidity buffers and less headroom if conditions worsen.', '%'],
  ['construction_gdp', 'Construction Sector GDP Growth (SA)', 'Growth in construction sector output. Weakening growth can signal softer sector momentum, project delays, or a downturn in construction activity.', '%'],
].map(([seriesId, title, why, unit]) => ({ seriesId, title, why, unit }));

function inferFrequency(point, fallbackFrequency = null) {
  const raw = String(point?.rawDate || '');
  if (/^\d{4}Q[1-4]$/.test(raw) || /^\d{4}[1-4]Q$/.test(raw)) return 'Q';
  if (/^\d{4}-(\d{2})$/.test(raw)) return 'M';
  if (fallbackFrequency) return fallbackFrequency;
  return null;
}

function formatPointDate(point, fallbackFrequency = null) {
  if (!point) return 'No data';
  const frequency = inferFrequency(point, fallbackFrequency);

  if (frequency === 'M' && point.date) {
    const date = new Date(point.date);
    if (!Number.isNaN(date.getTime())) {
      const month = date.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' });
      return `${date.getUTCFullYear()} ${month}`;
    }
  }

  if (frequency === 'Q') {
    const quarterMatch = String(point.rawDate).match(/^(\d{4})Q([1-4])$/) || String(point.rawDate).match(/^(\d{4})([1-4])Q$/);
    if (quarterMatch) return `${quarterMatch[1]} Q${quarterMatch[2]}`;
    if (point.date) {
      const date = new Date(point.date);
      if (!Number.isNaN(date.getTime())) {
        const quarter = Math.floor(date.getUTCMonth() / 3) + 1;
        return `${date.getUTCFullYear()} Q${quarter}`;
      }
    }
  }

  if (point.date) return App.formatDate(point.date);
  return point.rawDate;
}

function sparklineSvg(series, options = {}) {
  if (!series || series.length < 2) return '<svg class="sparkline"></svg>';
  const { frequency = null, unit = '' } = options;
  const values = series.map((s) => s.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const w = 240;
  const h = 72;
  const pointRadius = 3;
  const clipPad = pointRadius + 2;
  const step = w / (values.length - 1);
  const coordinates = values
    .map((v, i) => ({
      x: (i * step).toFixed(1),
      y: (h - clipPad - ((v - min) / range) * (h - clipPad * 2)).toFixed(1),
      value: v,
      periodLabel: formatPointDate(series[i], frequency)
    }));
  const points = coordinates
    .map((coord) => `${coord.x},${coord.y}`)
    .join(' ');

  const circles = coordinates
    .map((coord) => `<circle class="sparkline-point-dot" cx="${coord.x}" cy="${coord.y}" r="${pointRadius - 0.2}"></circle>`)
    .join('');
  const encodedPoints = encodeURIComponent(JSON.stringify(coordinates));
  const clipId = `sparkline-clip-${Math.random().toString(36).slice(2, 9)}`;

  return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" data-points="${encodedPoints}" data-unit="${escapeHtml(unit)}" data-width="${w}" data-height="${h}" data-point-radius="${pointRadius}">
    <defs>
      <clipPath id="${clipId}">
        <rect x="${-clipPad}" y="${-clipPad}" width="${w + clipPad * 2}" height="${h + clipPad * 2}"></rect>
      </clipPath>
    </defs>
    <g clip-path="url(#${clipId})">
      <polyline class="sparkline-line" points="${points}" />
      ${circles}
      <circle class="sparkline-focus" cx="0" cy="0" r="${pointRadius}"></circle>
    </g>
    <rect class="sparkline-hover-layer" x="0" y="0" width="${w}" height="${h}" fill="transparent"></rect>
  </svg>`;
}

function createTooltip(container) {
  let tooltip = container.querySelector('.chart-tooltip');
  if (tooltip) return tooltip;
  tooltip = document.createElement('div');
  tooltip.className = 'chart-tooltip';
  container.appendChild(tooltip);
  return tooltip;
}

function showTooltip(tooltip, periodLabel, valueFormatted, unitLabel) {
  tooltip.innerHTML = `<div class="chart-tooltip-period">${escapeHtml(periodLabel)}</div><div class="chart-tooltip-value">${escapeHtml(`${valueFormatted} ${unitLabel}`.trim())}</div>`;
  tooltip.classList.add('visible');
}

function moveTooltip(tooltip, container, x, y, offset = 10) {
  const containerRect = container.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  const maxLeft = Math.max(0, containerRect.width - tooltipRect.width);
  const maxTop = Math.max(0, containerRect.height - tooltipRect.height);
  const left = Math.min(Math.max(x + offset, 0), maxLeft);
  const top = Math.min(Math.max(y + offset, 0), maxTop);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function hideTooltip(tooltip) {
  tooltip.classList.remove('visible');
}

function formatTooltipValue(value) {
  return new Intl.NumberFormat('en-SG', { maximumFractionDigits: 2 }).format(value);
}

function initSparklineInteractions() {
  const bisect = (points, targetX) => {
    let low = 0;
    let high = points.length - 1;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (Number(points[mid].x) < targetX) low = mid + 1;
      else high = mid;
    }

    if (low === 0) return 0;
    const current = Number(points[low].x);
    const prev = Number(points[low - 1].x);
    return Math.abs(current - targetX) < Math.abs(targetX - prev) ? low : low - 1;
  };

  document.querySelectorAll('.indicator-tile').forEach((tile) => {
    const svg = tile.querySelector('.sparkline[data-points]');
    if (!svg) return;
    const hoverLayer = svg.querySelector('.sparkline-hover-layer');
    const focus = svg.querySelector('.sparkline-focus');
    if (!hoverLayer || !focus) return;

    const points = JSON.parse(decodeURIComponent(svg.dataset.points || '[]'));
    if (!points.length) return;

    const unit = svg.dataset.unit || '';
    const width = Number(svg.dataset.width) || 240;
    const tooltip = createTooltip(tile);

    const updateHoverState = (event) => {
      const rect = hoverLayer.getBoundingClientRect();
      const chartX = ((event.clientX - rect.left) / rect.width) * width;
      const nearestIndex = bisect(points, chartX);
      const point = points[nearestIndex];
      focus.setAttribute('cx', point.x);
      focus.setAttribute('cy', point.y);
      focus.classList.add('visible');

      showTooltip(tooltip, point.periodLabel, formatTooltipValue(point.value), unit);

      const tileRect = tile.getBoundingClientRect();
      moveTooltip(tooltip, tile, event.clientX - tileRect.left, event.clientY - tileRect.top);
    };

    hoverLayer.addEventListener('mouseenter', updateHoverState);
    hoverLayer.addEventListener('mousemove', updateHoverState);
    hoverLayer.addEventListener('mouseleave', () => {
      focus.classList.remove('visible');
      hideTooltip(tooltip);
    });
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeCategory(rawCategory) {
  if (LEGACY_CATEGORY_TO_MAJOR[rawCategory]) return LEGACY_CATEGORY_TO_MAJOR[rawCategory];
  return null;
}

function parsePeriodToDate(period) {
  const monthly = String(period).match(/^(\d{4})-(\d{2})$/);
  if (monthly) return `${monthly[1]}-${monthly[2]}-01`;

  const quarter = String(period).match(/^(\d{4})Q([1-4])$/) || String(period).match(/^(\d{4})([1-4])Q$/);
  if (!quarter) return null;

  const year = Number(quarter[1]);
  const q = Number(quarter[2]);
  const monthByQuarter = { 1: 3, 2: 6, 3: 9, 4: 12 };
  const month = String(monthByQuarter[q]).padStart(2, '0');
  return `${year}-${month}-01`;
}

function normalizeSeriesPoints(rawSeries = []) {
  const points = rawSeries
    .map((point) => {
      const value = Number(point.value);
      if (!Number.isFinite(value)) return null;
      const dateIso = point.date || parsePeriodToDate(point.period);
      return {
        rawDate: point.date || point.period || 'n/a',
        date: dateIso,
        value
      };
    })
    .filter(Boolean);

  points.sort((a, b) => {
    if (a.date && b.date) return a.date.localeCompare(b.date);
    return a.rawDate.localeCompare(b.rawDate);
  });

  return points;
}

function formatLastPointDate(point) {
  return formatPointDate(point, point?.frequency || null);
}

const LATEST_VALUE_ROUNDED_UNITS = new Set(['K Sqm Gross', 'Units', 'K Sqm Nett', 'K Tonnes', 'S$M']);

function formatLatestValue(point, unit = '') {
  if (!point) return 'No data';
  const normalizedUnit = String(unit).trim();
  const displayValue = LATEST_VALUE_ROUNDED_UNITS.has(normalizedUnit) ? Math.round(point.value) : point.value.toFixed(2);
  return `${displayValue} ${normalizedUnit}`.trim();
}

async function loadFrequencyMap() {
  const response = await fetch('./data/macro indicators meta.csv');
  if (!response.ok) return {};

  const csvText = await response.text();
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return {};

  const parseRow = (line) => {
    const out = [];
    let token = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i += 1) {
      const char = line[i];
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          token += '"';
          i += 1;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        out.push(token.trim());
        token = '';
      } else {
        token += char;
      }
    }
    out.push(token.trim());
    return out;
  };

  const headers = parseRow(lines[0]).map((header) => header.toLowerCase());
  const seriesIdIndex = headers.findIndex((header) => header === 'series id');
  const frequencyIndex = headers.findIndex((header) => header === 'frequency' || header === 'freq');
  if (seriesIdIndex === -1 || frequencyIndex === -1) return {};

  return lines.slice(1).reduce((acc, line) => {
    const row = parseRow(line);
    const seriesId = row[seriesIdIndex];
    const frequency = row[frequencyIndex];
    if (!seriesId || !frequency) return acc;

    const normalized = frequency.toUpperCase().startsWith('Q') ? 'Q' : frequency.toUpperCase().startsWith('M') ? 'M' : null;
    if (normalized) acc[seriesId] = normalized;
    return acc;
  }, {});
}

function computeMacroRisk(cards) {
  const scores = cards.map((card) => {
    if (!card.latest || !card.thresholds) return 1;
    const latest = card.latest.value;
    if (latest >= card.thresholds.critical) return 4;
    if (latest >= card.thresholds.warning) return 3;
    if (latest >= card.thresholds.watch) return 2;
    return 1;
  });

  if (!scores.length) return 'Watch';
  const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
  if (avg >= 3.4) return 'Critical';
  if (avg >= 2.6) return 'Warning';
  return 'Watch';
}

function validateMacroCardsOrThrow(cards, macroSeries) {
  const missingSeries = cards.filter((card) => !macroSeries[card.seriesId]).map((card) => card.seriesId);
  const missingCategory = cards.filter((card) => !card.majorCategory).map((card) => card.seriesId);

  const countsByCategory = cards.reduce((acc, card) => {
    acc[card.majorCategory || 'missing'] = (acc[card.majorCategory || 'missing'] || 0) + 1;
    return acc;
  }, {});

  console.info('[macro-validation] card counts by category', countsByCategory);
  if (missingCategory.length) {
    console.error('[macro-validation] missing major_category mapping for series IDs:', missingCategory);
  }

  if (missingSeries.length || missingCategory.length) {
    const parts = [];
    if (missingSeries.length) parts.push(`missing series in data store: ${missingSeries.join(', ')}`);
    if (missingCategory.length) parts.push(`missing major_category mapping: ${missingCategory.join(', ')}`);
    throw new Error(`Macro page card validation failed (${parts.join(' | ')})`);
  }
}

function mapCardsFromSeries(data, frequencyMap = {}) {
  const macroSeries = data?.macro_indicators?.series || {};

  const cards = MACRO_INDICATOR_METADATA.map((meta) => {
    const seriesObj = macroSeries[meta.seriesId];
    const normalized = normalizeSeriesPoints(seriesObj?.values || []);
    const latest = normalized[normalized.length - 1] || null;
    const prior = normalized[normalized.length - 2] || null;
    const majorCategory = normalizeCategory(seriesObj?.major_category);

    const frequency = seriesObj?.freq || frequencyMap[meta.seriesId] || null;

    return {
      ...meta,
      majorCategory,
      categoryLabel: majorCategory ? CATEGORY_UI[majorCategory]?.short : null,
      unit: meta.unit || seriesObj?.units || '',
      frequency,
      sparkline: normalized.slice(-24),
      latest: latest ? { ...latest, frequency } : null,
      prior,
      thresholds: null
    };
  });

  validateMacroCardsOrThrow(cards, macroSeries);
  return cards;
}

async function initMacroPage() {
  const wrap = document.getElementById('macro-grid');
  const riskNode = document.getElementById('macro-risk');
  const categoryFilter = document.getElementById('category-filter');
  if (!wrap) return;

  try {
    const data = await App.fetchJson('./data/macro_indicators.json');
    const frequencyMap = await loadFrequencyMap();
    const cards = mapCardsFromSeries(data, frequencyMap);

    const render = () => {
      const selectedCategory = categoryFilter.value;
      const filtered = cards.filter((card) => (selectedCategory === 'all' ? true : card.majorCategory === selectedCategory));
      const risk = computeMacroRisk(filtered);

      riskNode.innerHTML = `<span class="badge sev-${risk.toLowerCase()}">Macro headwinds: ${risk}</span>`;
      if (!filtered.length) {
        wrap.innerHTML = '<p class="empty">No indicators for selected monitoring category.</p>';
        return;
      }

      wrap.innerHTML = filtered
        .map((card, index) => {
          const delta = card.latest && card.prior ? (card.latest.value - card.prior.value).toFixed(2) : 'No data';
          const latestText = formatLatestValue(card.latest, card.unit);
          const tooltipId = `macro-tooltip-${index}`;
          const categoryPill = card.categoryLabel ? `<span class="badge macro-category-pill macro-category-pill-${card.majorCategory?.[0]?.toLowerCase() || 'default'}">${escapeHtml(card.categoryLabel)}</span>` : '';
          return `<article class="panel indicator-tile">
            <div class="indicator-top-row">
              ${categoryPill}
            </div>
            <div class="indicator-title-row">
              <h3>${escapeHtml(card.title)}</h3>
              <span class="info-tooltip">
                <button class="tooltip-trigger" type="button" aria-label="Why it matters for ${escapeHtml(card.title)}" aria-describedby="${tooltipId}">?</button>
                <span class="tooltip-content" id="${tooltipId}" role="tooltip">${escapeHtml(card.why)}</span>
              </span>
            </div>
            <div class="value">${latestText}</div>
            <div class="meta-row">Change vs prior: ${delta}</div>
            <div class="meta-row">Last point: ${formatLastPointDate(card.latest)}</div>
            ${sparklineSvg(card.sparkline, { frequency: card.frequency, unit: card.unit })}
          </article>`;
        })
        .join('');

      initSparklineInteractions();
    };

    categoryFilter.addEventListener('change', render);
    window.addEventListener('resize', () => {
      document.querySelectorAll('.chart-tooltip.visible').forEach((tooltip) => hideTooltip(tooltip));
    });
    render();
  } catch (e) {
    wrap.innerHTML = `<p class="empty">Unable to load macro data: ${e.message}</p>`;
  }
}

document.addEventListener('DOMContentLoaded', initMacroPage);
