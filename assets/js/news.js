const ITEMS_PER_PAGE = App.newsConfig.pageSize;
const SEVERITY_META = App.newsConfig.severityMeta;
const DATE_RANGE_OPTIONS = App.newsConfig.dateRangeOptions;
const TRACKING_PREFIXES = ['utm_', 'fbclid', 'gclid', 'mc_cid', 'mc_eid', 'cmpid', 'igshid', 's_cid'];

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
  if (!list || !pagination) return;

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

function canonicalizeNewsLink(link) {
  try {
    const url = new URL(link);
    for (const key of [...url.searchParams.keys()]) {
      const lower = key.toLowerCase();
      if (TRACKING_PREFIXES.some((prefix) => lower === prefix || lower.startsWith(prefix))) {
        url.searchParams.delete(key);
      }
    }
    url.hash = '';
    if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, '');
    return url.toString();
  } catch {
    return String(link || '').trim();
  }
}

function datePartForDedup(pubDate) {
  const parsed = new Date(pubDate);
  if (Number.isNaN(parsed.getTime())) return 'unknown';
  return parsed.toISOString().slice(0, 10);
}

function normalizeTitleForDedup(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((token) => token && !['a', 'an', 'the'].includes(token))
    .join(' ')
    .trim();
}

function dedupeNewsItems(items = []) {
  const seen = new Set();
  const deduped = [];
  let dropped = 0;

  for (const item of items) {
    const titleDateKey = `title_date:${normalizeTitleForDedup(item.title)}|${datePartForDedup(item.pubDate)}`;
    const fallbackKey = `fallback:${String(item.source || '').toLowerCase().trim()}|${String(item.publisher || '').toLowerCase().trim()}|${datePartForDedup(item.pubDate)}`;
    const keys = [canonicalizeNewsLink(item.link), titleDateKey, fallbackKey].filter(Boolean);
    if (keys.some((key) => seen.has(key))) {
      dropped += 1;
      continue;
    }
    keys.forEach((key) => seen.add(key));
    deduped.push(item);
  }

  return { items: deduped, dropped };
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

function initializeDateRangeOptions(dateRangeEl) {
  if (!dateRangeEl) return;
  dateRangeEl.innerHTML = DATE_RANGE_OPTIONS.map((days) => {
    const selected = days === 30 ? ' selected' : '';
    return `<option value="${days}"${selected}>Last ${days} days</option>`;
  }).join('');
}

function initNewsPage() {
  const newsList = document.getElementById('news-list');
  if (!newsList || newsList.dataset.initialized === 'true') return;
  newsList.dataset.initialized = 'true';

  Promise.all([loadNewsData(), App.fetchJson('./data/meta.json')])
    .then(([items, meta]) => {
      const ninetyDayCutoff = Date.now() - 90 * 24 * 60 * 60 * 1000;
      const processed = items
        .map((item) => ({ ...item, severity: App.normalizeNewsSeverity(item.severity) }))
        .filter((item) => new Date(item.pubDate).getTime() >= ninetyDayCutoff)
        .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
      const deduped = dedupeNewsItems(processed);
      const processedItems = deduped.items;
      if (deduped.dropped > 0) {
        console.info(`[news] filtered ${deduped.dropped} duplicate items from dashboard feed`);
      }

      const developers = Array.from(new Set(processedItems.map((i) => i.developer || 'Unknown'))).sort();
      const sources = Array.from(new Set(processedItems.map((i) => i.source))).sort();

      const sevSelect = document.getElementById('severity-filter');
      const devSelect = document.getElementById('developer-filter');
      const sourceSelect = document.getElementById('source-filter');
      const dateRange = document.getElementById('date-range');
      const searchInput = document.getElementById('search-text');
      const pagination = document.getElementById('news-pagination');

      if (!sevSelect || !devSelect || !sourceSelect || !dateRange || !searchInput || !pagination) {
        throw new Error('Missing required filter controls on news page');
      }

      initializeDateRangeOptions(dateRange);
      sevSelect.innerHTML = '<option value="all" selected>All severities</option>';
      devSelect.innerHTML = '<option value="all" selected>All developers</option>';
      sourceSelect.innerHTML = '<option value="all" selected>All sources</option>';

      ['warning', 'watch', 'info'].forEach((severityKey) => {
        const label = SEVERITY_META[severityKey].title;
        sevSelect.insertAdjacentHTML('beforeend', `<option value="${severityKey}">${label}</option>`);
      });
      developers.forEach((developer) => devSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(developer)}">${escapeHtml(developer)}</option>`));
      sources.forEach((source) => sourceSelect.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(source)}">${escapeHtml(source)}</option>`));

      let currentPage = 1;
      let filteredItems = [];

      const applyFilters = () => {
        const selectedSeverity = sevSelect.value;
        const selectedDeveloper = devSelect.value;
        const selectedSource = sourceSelect.value;
        const selectedDays = Number(dateRange.value || 30);
        const rangeDays = Number.isFinite(selectedDays) ? Math.min(selectedDays, 90) : 90;
        const searchText = searchInput.value.trim().toLowerCase();
        const cutoff = Date.now() - rangeDays * 24 * 60 * 60 * 1000;

        filteredItems = processedItems.filter((item) => {
          if (new Date(item.pubDate).getTime() < cutoff) return false;
          if (selectedSeverity !== 'all' && item.severity !== selectedSeverity) return false;
          if (selectedDeveloper !== 'all' && (item.developer || 'Unknown') !== selectedDeveloper) return false;
          if (selectedSource !== 'all' && item.source !== selectedSource) return false;
          if (searchText) {
            const blob = `${item.title || ''} ${item.snippet || ''}`.toLowerCase();
            if (!blob.includes(searchText)) return false;
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
        el.addEventListener('change', onFilterChanged, { passive: true });
      });
      searchInput.addEventListener('input', onFilterChanged, { passive: true });

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
