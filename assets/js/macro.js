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

const MACRO_CARD_DEFS = [
  {
    seriesId: 'sora_overnight',
    title: 'Singapore interest rate proxy',
    fallbackUnit: '%',
    why: 'Higher rates increase debt servicing burden and raise refinancing risk for leveraged developers.'
  },
  {
    seriesId: 'term_spread_10y_2y',
    title: 'Corporate credit spread proxy',
    fallbackUnit: 'pp',
    why: 'A flatter or inverted curve can coincide with tighter lending and more selective credit conditions.'
  },
  {
    seriesId: 'unit_labour_cost_construction',
    title: 'Construction cost inflation proxy',
    fallbackUnit: 'index',
    why: 'Persistent construction cost pressure can compress gross margins and increase completion risk.'
  },
  {
    seriesId: 'private_vacant_private_sector_office_space_vacant',
    title: 'Office vacancy proxy',
    fallbackUnit: 'sqm',
    why: 'Higher vacant office stock can weigh on leasing momentum, rental growth, and valuation assumptions.'
  },
  {
    seriesId: 'construction_gdp',
    title: 'Construction GDP growth proxy',
    fallbackUnit: '% YoY',
    why: 'Weaker construction activity can point to softer sector demand and lower operating momentum.'
  },
  {
    seriesId: 'prp_pipeline_total_non_landed',
    title: 'Private home supply pipeline',
    fallbackUnit: 'units',
    why: 'A large non-landed pipeline may increase inventory overhang risk if absorption slows.'
  }
];

function sparklineSvg(series) {
  if (!series || series.length < 2) return '<svg class="sparkline"></svg>';
  const values = series.map((s) => s.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const w = 240;
  const h = 72;
  const step = w / (values.length - 1);
  const points = values
    .map((v, i) => `${(i * step).toFixed(1)},${(h - ((v - min) / range) * h).toFixed(1)}`)
    .join(' ');
  return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><polyline fill="none" stroke="#64748b" stroke-width="2" points="${points}" /></svg>`;
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
  if (!point) return 'No data';
  if (point.date) return App.formatDate(point.date);
  return point.rawDate;
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

function mapCardsFromSeries(data) {
  const macroSeries = data?.macro_indicators?.series || {};

  const cards = MACRO_CARD_DEFS.map((def) => {
    const seriesObj = macroSeries[def.seriesId];
    const normalized = normalizeSeriesPoints(seriesObj?.values || []);
    const latest = normalized[normalized.length - 1] || null;
    const prior = normalized[normalized.length - 2] || null;
    const majorCategory = normalizeCategory(seriesObj?.major_category);

    return {
      ...def,
      majorCategory,
      categoryLabel: majorCategory ? CATEGORY_UI[majorCategory]?.short : null,
      unit: seriesObj?.units || def.fallbackUnit,
      frequency: seriesObj?.freq || null,
      sparkline: normalized.slice(-24),
      latest,
      prior,
      thresholds: def.thresholds || null
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
    const cards = mapCardsFromSeries(data);

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
          const latestText = card.latest ? `${card.latest.value.toFixed(2)} ${card.unit}` : `No data ${card.unit}`;
          const tooltipId = `macro-tooltip-${index}`;
          const categoryPill = card.categoryLabel ? `<span class="badge macro-category-pill">${escapeHtml(card.categoryLabel)}</span>` : '';
          return `<article class="panel indicator-tile">
            <div class="indicator-title-row">
              <h3>${escapeHtml(card.title)}</h3>
              ${categoryPill}
              <span class="info-tooltip">
                <button class="tooltip-trigger" type="button" aria-label="Why it matters for ${escapeHtml(card.title)}" aria-describedby="${tooltipId}">?</button>
                <span class="tooltip-content" id="${tooltipId}" role="tooltip">${escapeHtml(card.why)}</span>
              </span>
            </div>
            <div class="value">${latestText}</div>
            <div class="meta-row">Change vs prior: ${delta}</div>
            <div class="meta-row">Last point: ${formatLastPointDate(card.latest)}</div>
            ${sparklineSvg(card.sparkline)}
          </article>`;
        })
        .join('');
    };

    categoryFilter.addEventListener('change', render);
    render();
  } catch (e) {
    wrap.innerHTML = `<p class="empty">Unable to load macro data: ${e.message}</p>`;
  }
}

document.addEventListener('DOMContentLoaded', initMacroPage);
