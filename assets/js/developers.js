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

async function initDevelopersPage() {
  const tableBody = document.getElementById('developers-body');
  const methodology = document.getElementById('methodology-content');
  if (!tableBody) return;

  try {
    const data = await App.fetchJson('./data/listed_developers.json');
    methodology.textContent = `Score = Σ(weight × normalized metric). Weights: ${JSON.stringify(data.scoringModel.weights)}. Status bands: Green ≥ ${data.scoringModel.bands.status.green}, Amber ≥ ${data.scoringModel.bands.status.amber}, otherwise Red.`;

    const rows = data.developers.map((dev) => {
      const scoreObj = computeScore(dev, data.scoringModel);
      const score = dev.precomputedHealthScore ?? scoreObj.total;
      const status = dev.statusOverride ?? statusFromScore(score, data.scoringModel);
      const cls = `status-${status.toLowerCase()}`;
      const id = dev.ticker.replace(/\W/g, '');
      return `
        <tr>
          <td>${dev.name}</td><td>${dev.ticker}</td><td>${dev.segment}</td>
          <td><strong>${score}</strong></td><td class="${cls}">${status}</td>
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
      `;
    });

    tableBody.innerHTML = rows.join('');
    tableBody.querySelectorAll('button[data-target]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const row = document.getElementById(btn.dataset.target);
        row.hidden = !row.hidden;
      });
    });
  } catch (e) {
    tableBody.innerHTML = `<tr><td colspan="12" class="empty">Unable to load developer data: ${e.message}</td></tr>`;
  }
}

document.addEventListener('DOMContentLoaded', initDevelopersPage);
