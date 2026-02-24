function normalize(value, lowRiskGood = true) {
  const v = Math.max(0, Math.min(1.5, value));
  const score = Math.max(0, Math.min(100, (1 - v / 1.5) * 100));
  return lowRiskGood ? score : 100 - score;
}

function computeScore(dev, model) {
  const w = model.weights;
  const m = dev.metrics;
  const componentScores = {
    leverage: normalize(m.netGearing, true),
    liquidity: normalize(m.cashToShortDebt, false),
    maturity: normalize(m.debtMaturity12mPct, true),
    coverage: normalize(m.interestCoverage / 5, false),
    sales: normalize(m.presalesCoverage, false)
  };
  const weighted = Object.entries(w).reduce((acc, [k, wt]) => acc + componentScores[k] * wt, 0);
  return { total: Math.round(weighted), componentScores };
}

function statusFromScore(score, model) {
  const bands = model.bands.status;
  if (score >= bands.green) return 'Green';
  if (score >= bands.amber) return 'Amber';
  return 'Red';
}

const SORT_DIRECTIONS = ['none', 'ascending', 'descending'];
const PENDING_RE = /pending/i;

function parseNumberCandidate(value) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed || PENDING_RE.test(trimmed)) return null;
  const numeric = Number(trimmed.replace(/[,%$]/g, ''));
  return Number.isFinite(numeric) ? numeric : null;
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

  tableBody.querySelectorAll('button[data-target]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const row = document.getElementById(btn.dataset.target);
      row.hidden = !row.hidden;
    });
  });
}

async function initDevelopersPage() {
  const tableBody = document.getElementById('developers-body');
  const methodology = document.getElementById('methodology-content');
  const table = tableBody?.closest('table');
  if (!tableBody || !table) return;

  try {
    const data = await App.fetchJson('./data/listed_developers.json');
    methodology.textContent = `Score = Σ(weight × normalized metric). Weights: ${JSON.stringify(data.scoringModel.weights)}. Status bands: Green ≥ ${data.scoringModel.bands.status.green}, Amber ≥ ${data.scoringModel.bands.status.amber}, otherwise Red.`;

    const rows = data.developers.map((dev, index) => {
      const scoreObj = computeScore(dev, data.scoringModel);
      const score = dev.precomputedHealthScore ?? scoreObj.total;
      const status = dev.statusOverride ?? statusFromScore(score, data.scoringModel);
      const cls = `status-${status.toLowerCase()}`;
      const id = dev.ticker.replace(/\W/g, '');
      return {
        originalIndex: index,
        sortData: {
          name: dev.name,
          ticker: dev.ticker,
          segment: dev.segment,
          score,
          status,
          leverage: dev.drivers.leverage,
          coverage: dev.drivers.coverage,
          maturity: dev.drivers.maturity,
          liquidity: dev.drivers.liquidity,
          sales: dev.drivers.sales,
          exposure: dev.drivers.exposure
        },
        markup: `
          <tr>
            <td data-sticky-col="1">${dev.name}</td><td>${dev.ticker}</td><td>${dev.segment}</td>
            <td><strong>${score}</strong></td><td><span class="status-pill ${cls}">${status}</span></td>
            <td>${dev.drivers.leverage}</td><td>${dev.drivers.coverage}</td><td>${dev.drivers.maturity}</td>
            <td>${dev.drivers.liquidity}</td><td>${dev.drivers.sales}</td><td>${dev.drivers.exposure}</td>
            <td><button data-target="${id}">Details</button></td>
          </tr>
          <tr id="${id}" hidden>
            <td colspan="12">
              <strong>Driver notes:</strong>
              <ul>${dev.notes.map((n) => `<li>${n}</li>`).join('')}</ul>
              <div class="meta-row">Last updated: ${App.formatDate(dev.lastUpdated)}</div>
            </td>
          </tr>
        `
      };
    });

    const state = { sortState: null };
    const onSortChange = () => renderRows(tableBody, rows, state.sortState);

    initSortableHeaders(table, state, onSortChange);
    renderRows(tableBody, rows, state.sortState);
  } catch (e) {
    tableBody.innerHTML = `<tr><td colspan="12" class="empty">Unable to load developer data: ${e.message}</td></tr>`;
  }
}

document.addEventListener('DOMContentLoaded', initDevelopersPage);
