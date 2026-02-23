const ITEMS_PER_PAGE = 15;

const SEVERITY_META = {
  warning: {
    title: 'Warning',
    description:
      'Elevated risk signals (e.g., distress/default/covenant breach/restructuring or serious refinancing stress and major liquidity actions). Includes previous critical tier.'
  },
  watch: {
    title: 'Watch',
    description:
      'Early-to-moderate risk signals (e.g., weaker presales/take-up, rising unsold stock, margin pressure, cost escalation, softer guidance). Includes previous warning tier.'
  },
  info: {
    title: 'Info',
    description: 'Neutral monitoring items without clear risk signals (e.g., routine launches, corporate updates, sector commentary).'
  }
};

function normalizeSeverity(severity) {
  const key = String(severity || 'info').toLowerCase();
  if (key === 'critical') return 'warning';
  if (key === 'warning') return 'watch';
  if (key === 'watch') return 'watch';
  return 'info';
}

function escapeHtml(value = '') {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderSeverityBadge(severity) {
  const key = String(severity || 'info').toLowerCase();
  const meta = SEVERITY_META[key] || SEVERITY_META.info;
  return `
    <span class="severity-tooltip" tabindex="0" role="note" aria-label="${meta.title}: ${meta.description}">
      <span class="badge sev-${key}">${meta.title}</span>
      <span class="severity-tooltip-content">
        <strong>${meta.title}</strong>
        <span>${meta.description}</span>
      </span>
    </span>
  `;
}

function buildPaginationButtons(totalPages, currentPage) {
  if (totalPages <= 1) return [];

  const pages = new Set([1, totalPages]);
  for (let page = currentPage - 2; page <= currentPage + 2; page += 1) {
    if (page >= 1 && page <= totalPages) pages.add(page);
  }

  const sortedPages = [...pages].sort((a, b) => a - b);
  const buttons = [];

  for (let i = 0; i < sortedPages.length; i += 1) {
    const page = sortedPages[i];
    const previous = sortedPages[i - 1];

    if (previous && page - previous > 1) {
      buttons.push('<span class="pagination-ellipsis" aria-hidden="true">…</span>');
    }

    buttons.push(`
      <button type="button" class="pagination-page ${page === currentPage ? 'is-active' : ''}" data-page="${page}" aria-label="Go to page ${page}" ${
        page === currentPage ? 'aria-current="page"' : ''
      }>
        ${page}
      </button>
    `);
  }

  return buttons;
}

function renderNewsItems(items, currentPage) {
  const list = document.getElementById('news-list');
  const pagination = document.getElementById('news-pagination');

  if (!items.length) {
    list.innerHTML = '<p class="empty">No news items match your filters.</p>';
    pagination.innerHTML = '';
    return;
  }

  const totalPages = Math.max(1, Math.ceil(items.length / ITEMS_PER_PAGE));
  const safePage = Math.min(Math.max(currentPage, 1), totalPages);
  const start = (safePage - 1) * ITEMS_PER_PAGE;
  const pagedItems = items.slice(start, start + ITEMS_PER_PAGE);

  list.innerHTML = pagedItems
    .map((item) => {
      const tags = (item.tags || []).map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`).join('');
      const matched = (item.matched_terms || [])
        .slice(0, 4)
        .map((term) => `<span class="chip">Matched: ${escapeHtml(term)}</span>`)
        .join('');
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

  const pageButtons = buildPaginationButtons(totalPages, safePage).join('');
  pagination.innerHTML = `
    <div class="pagination-inner" role="navigation" aria-label="News pagination">
      <button type="button" class="pagination-nav" data-page="${safePage - 1}" ${safePage === 1 ? 'disabled' : ''}>Prev</button>
      <div class="pagination-pages">${pageButtons}</div>
      <button type="button" class="pagination-nav" data-page="${safePage + 1}" ${safePage === totalPages ? 'disabled' : ''}>Next</button>
    </div>
  `;
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
        .map((item) => ({ ...item, severity: normalizeSeverity(item.severity) }))
        .filter((item) => new Date(item.pubDate).getTime() >= ninetyDayCutoff)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

      const developers = Array.from(new Set(processed.map((i) => i.developer || 'Unknown'))).sort();
      const sources = Array.from(new Set(processed.map((i) => i.source))).sort();

      const sevSelect = document.getElementById('severity-filter');
      const devSelect = document.getElementById('developer-filter');
      const sourceSelect = document.getElementById('source-filter');
      const dateRange = document.getElementById('date-range');
      const searchInput = document.getElementById('search-text');
      const pagination = document.getElementById('news-pagination');

      sevSelect.innerHTML = '<option value="all" selected>All severities</option>';
      devSelect.innerHTML = '<option value="all" selected>All developers</option>';
      sourceSelect.innerHTML = '<option value="all" selected>All sources</option>';

      ['warning', 'watch', 'info'].forEach((s) => {
        const label = SEVERITY_META[s].title;
        sevSelect.insertAdjacentHTML('beforeend', `<option value="${s}">${label}</option>`);
      });
      developers.forEach((d) => devSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(d)}">${escapeHtml(d)}</option>`));
      sources.forEach((s) => sourceSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`));

      let currentPage = 1;
      let filteredItems = [];

      const applyFilters = () => {
        const sevSel = sevSelect.value;
        const devSel = devSelect.value;
        const srcSel = sourceSelect.value;
        const selectedDays = Number(dateRange.value || 30);
        const rangeDays = Number.isFinite(selectedDays) ? Math.min(selectedDays, 90) : 90;
        const search = searchInput.value.trim().toLowerCase();
        const cutoff = Date.now() - rangeDays * 24 * 60 * 60 * 1000;

        filteredItems = processed.filter((item) => {
          if (new Date(item.pubDate).getTime() < cutoff) return false;
          if (sevSel !== 'all' && item.severity !== sevSel) return false;
          if (devSel !== 'all' && (item.developer || 'Unknown') !== devSel) return false;
          if (srcSel !== 'all' && item.source !== srcSel) return false;
          if (search) {
            const blob = `${item.title || ''} ${item.snippet || ''}`.toLowerCase();
            if (!blob.includes(search)) return false;
          }
          return true;
        });

        renderNewsItems(filteredItems, currentPage);
      };

      const onFilterChanged = () => {
        currentPage = 1;
        applyFilters();
      };

      document.querySelectorAll('#news-filters select').forEach((el) => {
        el.addEventListener('change', onFilterChanged);
      });
      searchInput.addEventListener('input', onFilterChanged);

      pagination.addEventListener('click', (event) => {
        const button = event.target.closest('button[data-page]');
        if (!button || button.disabled) return;
        const targetPage = Number(button.dataset.page);
        if (!Number.isFinite(targetPage)) return;
        currentPage = targetPage;
        renderNewsItems(filteredItems, currentPage);
      });

      const metaNode = document.getElementById('global-last-updated');
      if (metaNode && meta?.last_updated_sgt) {
        metaNode.textContent = `${App.formatDateTime(meta.last_updated_sgt)} (SGT)`;
      }

      applyFilters();
    })
    .catch((error) => {
      newsList.innerHTML = `<p class="empty">Unable to load news data: ${error.message}</p>`;
    });
}

document.addEventListener('DOMContentLoaded', initNewsPage);
