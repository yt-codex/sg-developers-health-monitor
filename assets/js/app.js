const App = (() => {
  const severityOrder = { Critical: 4, Warning: 3, Watch: 2, Info: 1 };

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

  return { setActiveNav, fetchJson, formatDate, formatDateTime, highestSeverity, severityOrder, setGlobalLastUpdated };
})();

document.addEventListener('DOMContentLoaded', () => {
  App.setActiveNav();
  App.setGlobalLastUpdated();
});
