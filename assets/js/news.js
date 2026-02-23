const SEVERITY_META = {
  critical: {
    title: 'Critical',
    description: 'Credible distress or default-risk signals such as missed payments, insolvency proceedings, liquidation risk, covenant breaches, or formal restructuring.'
  },
  warning: {
    title: 'Warning',
    description: 'Material balance-sheet pressure indicators, including refinancing stress, liquidity-raising actions, rating pressure, or rights issues to shore up funding.'
  },
  watch: {
    title: 'Watch',
    description: 'Early warning signs like weaker take-up, slower presales, margin/cost pressure, rising unsold inventory, or project execution delays.'
  },
  info: {
    title: 'Info',
    description: 'Neutral mention with no negative distress signal identified in the article title or snippet.'
  }
};

function escapeHtml(value = '') {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderSeverityBadge(severity) {
  const key = (severity || 'info').toLowerCase();
  const meta = SEVERITY_META[key] || SEVERITY_META.info;
  return `
    <div class="severity-wrap">
      <span class="badge sev-${key}">${meta.title}</span>
      <span class="severity-tooltip" tabindex="0" role="note" aria-label="${meta.title} severity details">
        <span class="severity-tooltip-trigger" aria-hidden="true">ⓘ</span>
        <span class="severity-tooltip-content">
          <strong>${meta.title}</strong>
          <span>${meta.description}</span>
        </span>
      </span>
    </div>
  `;
}

function renderNewsItems(items) {
  const list = document.getElementById('news-list');
  if (!items.length) {
    list.innerHTML = '<p class="empty">No news items match your filters.</p>';
    return;
  }

  list.innerHTML = items
    .map((item) => {
      const tags = (item.tags || []).map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`).join('');
      const matched = (item.matched_terms || []).slice(0, 4).map((term) => `<span class="chip">Matched: ${escapeHtml(term)}</span>`).join('');
      return `
        <article class="news-item">
          <div class="news-header-row">
            ${renderSeverityBadge(item.severity)}
            <h3><a href="${item.link}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.title)}</a></h3>
          </div>
          <div class="meta-row">${App.formatDateTime(item.pubDate)} • ${item.source} • Developer: ${escapeHtml(item.developer || 'Unknown')}</div>
          <div class="chips">${tags}${matched}</div>
          <p>${escapeHtml(item.snippet || '')}</p>
        </article>
      `;
    })
    .join('');
}

async function loadNewsData() {
  try {
    const latest = await App.fetchJson('./data/news_latest_90d.json');
    return Array.isArray(latest.items) ? latest.items : [];
  } catch {
    const all = await App.fetchJson('./data/news_all.json');
    return Array.isArray(all.items) ? all.items : [];
  }
}

function initNewsPage() {
  const newsList = document.getElementById('news-list');
  if (!newsList) return;

  Promise.all([loadNewsData(), App.fetchJson('./data/meta.json')])
    .then(([items, meta]) => {
      const ninetyDayCutoff = Date.now() - 90 * 24 * 60 * 60 * 1000;
      const processed = items
        .filter((item) => new Date(item.pubDate).getTime() >= ninetyDayCutoff)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

      const developers = Array.from(new Set(processed.map((i) => i.developer || 'Unknown'))).sort();
      const sources = Array.from(new Set(processed.map((i) => i.source))).sort();

      const sevSelect = document.getElementById('severity-filter');
      const devSelect = document.getElementById('developer-filter');
      const sourceSelect = document.getElementById('source-filter');

      sevSelect.innerHTML = '<option value="all" selected>All severities</option>';
      devSelect.innerHTML = '<option value="all" selected>All developers</option>';
      sourceSelect.innerHTML = '<option value="all" selected>All sources</option>';

      ['critical', 'warning', 'watch', 'info'].forEach((s) => {
        const label = SEVERITY_META[s].title;
        sevSelect.insertAdjacentHTML('beforeend', `<option value="${s}">${label}</option>`);
      });
      developers.forEach((d) => devSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(d)}">${escapeHtml(d)}</option>`));
      sources.forEach((s) => sourceSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`));

      const runFilter = () => {
        const sevSel = sevSelect.value;
        const devSel = devSelect.value;
        const srcSel = sourceSelect.value;
        const selectedDays = Number(document.getElementById('date-range').value || 30);
        const rangeDays = Number.isFinite(selectedDays) ? Math.min(selectedDays, 90) : 90;
        const search = document.getElementById('search-text').value.trim().toLowerCase();
        const cutoff = Date.now() - rangeDays * 24 * 60 * 60 * 1000;

        const output = processed.filter((item) => {
          if (new Date(item.pubDate).getTime() < cutoff) return false;
          if (sevSel !== 'all' && (item.severity || 'info').toLowerCase() !== sevSel) return false;
          if (devSel !== 'all' && (item.developer || 'Unknown') !== devSel) return false;
          if (srcSel !== 'all' && item.source !== srcSel) return false;
          if (search) {
            const blob = `${item.title || ''} ${item.snippet || ''}`.toLowerCase();
            if (!blob.includes(search)) return false;
          }
          return true;
        });

        renderNewsItems(output);
      };

      document.querySelectorAll('#news-filters select, #news-filters input').forEach((el) => {
        el.addEventListener('change', runFilter);
      });
      document.getElementById('search-text').addEventListener('input', runFilter);

      const metaNode = document.getElementById('global-last-updated');
      if (metaNode && meta?.last_updated_sgt) {
        metaNode.textContent = `${App.formatDateTime(meta.last_updated_sgt)} (SGT)`;
      }

      runFilter();
    })
    .catch((error) => {
      newsList.innerHTML = `<p class="empty">Unable to load news data: ${error.message}</p>`;
    });
}

document.addEventListener('DOMContentLoaded', initNewsPage);
