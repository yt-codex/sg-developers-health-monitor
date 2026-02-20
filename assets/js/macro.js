function sparklineSvg(series) {
  if (!series || series.length < 2) return '<svg class="sparkline"></svg>';
  const values = series.map((s) => s.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const w = 240;
  const h = 42;
  const step = w / (values.length - 1);
  const points = values
    .map((v, i) => `${(i * step).toFixed(1)},${(h - ((v - min) / range) * h).toFixed(1)}`)
    .join(' ');
  return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><polyline fill="none" stroke="#1d4ed8" stroke-width="2" points="${points}" /></svg>`;
}

function computeMacroRisk(indicators) {
  const scores = indicators.map((ind) => {
    const latest = ind.series[ind.series.length - 1]?.value;
    if (latest == null) return 1;
    if (latest >= ind.thresholds.critical) return 4;
    if (latest >= ind.thresholds.warning) return 3;
    if (latest >= ind.thresholds.watch) return 2;
    return 1;
  });
  const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
  if (avg >= 3.4) return 'Critical';
  if (avg >= 2.6) return 'Warning';
  return 'Watch';
}

async function initMacroPage() {
  const wrap = document.getElementById('macro-grid');
  const riskNode = document.getElementById('macro-risk');
  const frequency = document.getElementById('frequency-filter');
  if (!wrap) return;

  try {
    const data = await App.fetchJson('./data/macro_indicators.json');
    const render = () => {
      const freq = frequency.value;
      const indicators = data.indicators.filter((i) => (freq === 'all' ? true : i.frequency === freq));
      riskNode.innerHTML = `<span class="badge sev-${computeMacroRisk(indicators).toLowerCase()}">Macro headwinds: ${computeMacroRisk(indicators)}</span>`;
      if (!indicators.length) {
        wrap.innerHTML = '<p class="empty">No indicators for selected frequency.</p>';
        return;
      }
      wrap.innerHTML = indicators
        .map((i) => {
          const latest = i.series[i.series.length - 1];
          const prior = i.series[i.series.length - 2];
          const delta = prior ? (latest.value - prior.value).toFixed(2) : 'n/a';
          return `<article class="panel indicator-tile">
            <h3>${i.name}</h3>
            <div class="value">${latest.value.toFixed(2)} ${i.unit}</div>
            <div class="meta-row">Change vs prior: ${delta}</div>
            <div class="meta-row">Last point: ${App.formatDate(latest.date)}</div>
            ${sparklineSvg(i.series)}
            <details>
              <summary>Why it matters</summary>
              <p>${i.why}</p>
            </details>
          </article>`;
        })
        .join('');
    };
    frequency.addEventListener('change', render);
    render();
  } catch (e) {
    wrap.innerHTML = `<p class="empty">Unable to load macro data: ${e.message}</p>`;
  }
}

document.addEventListener('DOMContentLoaded', initMacroPage);
