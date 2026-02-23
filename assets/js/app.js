const App = (() => {
  const severityOrder = { Critical: 4, Warning: 3, Watch: 2, Info: 1 };
  const NEWS_PAGE_SIZE = 15;
  const NEWS_DATE_RANGE_OPTIONS = [7, 30, 90];
  const NEWS_SEVERITY_META = {
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

  function setActiveNav() {
    const page = document.body.dataset.page;
    document.querySelectorAll('.nav-tabs a').forEach((link) => {
      if (link.dataset.page === page) link.classList.add('active');
    });
  }

  async function fetchJson(path) {
    const res = await fetch(path, { cache: 'no-cache' });
    if (!res.ok) throw new Error(`Failed to load ${path}`);
    return res.json();
  }

  function formatDate(dateStr) {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString('en-SG', { day: '2-digit', month: 'short', year: 'numeric' });
  }

  function formatDateTime(dateStr) {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleString('en-SG', {
      day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit'
    });
  }

  function highestSeverity(tags = []) {
    if (!tags.length) return 'Info';
    return tags.sort((a, b) => severityOrder[b] - severityOrder[a])[0];
  }

  function normalizeNewsSeverity(severity) {
    const key = String(severity || 'info').toLowerCase();
    if (key === 'critical') return 'warning';
    if (key === 'warning') return 'watch';
    if (key === 'watch') return 'watch';
    return 'info';
  }

  async function setGlobalLastUpdated() {
    const node = document.getElementById('global-last-updated');
    if (!node) return;
    try {
      const meta = await fetchJson('./data/site_meta.json');
      const txt = `${formatDateTime(meta.lastUpdated)} (${meta.timezone})`;
      node.textContent = txt;
      document.querySelectorAll('.last-updated-copy').forEach((n) => (n.textContent = txt));
    } catch {
      node.textContent = 'Unavailable';
    }
  }

  return {
    setActiveNav,
    fetchJson,
    formatDate,
    formatDateTime,
    highestSeverity,
    severityOrder,
    setGlobalLastUpdated,
    normalizeNewsSeverity,
    newsConfig: {
      pageSize: NEWS_PAGE_SIZE,
      dateRangeOptions: NEWS_DATE_RANGE_OPTIONS,
      severityMeta: NEWS_SEVERITY_META
    }
  };
})();

document.addEventListener('DOMContentLoaded', () => {
  App.setActiveNav();
  App.setGlobalLastUpdated();
});
