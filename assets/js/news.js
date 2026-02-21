function applyRiskRules(item, rules) {
  const content = {
    title: item.title || '',
    summary: item.summary || '',
    raw: item.raw || ''
  };
  const matches = [];
  for (const rule of rules) {
    const re = new RegExp(rule.regex, rule.flags || 'i');
    const hit = (rule.appliesTo || ['title', 'summary']).some((field) => re.test(content[field] || ''));
    if (hit) matches.push(rule);
  }
  const severities = matches.map((m) => m.severity);
  return {
    matches,
    primary: App.highestSeverity(severities)
  };
}

function renderNewsItems(items) {
  const list = document.getElementById('news-list');
  if (!items.length) {
    list.innerHTML = '<p class="empty">No news items match your filters.</p>';
    return;
  }
  list.innerHTML = items
    .map((item) => `
      <article class="news-item">
        <div><span class="badge sev-${item.primarySeverity.toLowerCase()}">${item.primarySeverity}</span></div>
        <div class="news-header-row">
          <h3><a href="${item.url}" target="_blank" rel="noopener noreferrer">${item.title}</a></h3>
          <div class="chips">${item.matches.map((m) => `<span class="chip" title="${m.rationale}">Matched: ${m.label}</span>`).join('')}</div>
        </div>
        <div class="meta-row">${App.formatDateTime(item.published)} • ${item.source} • Developers: ${item.developerTags.join(', ') || 'Unspecified'}</div>
        <p>${item.summary}</p>
      </article>
    `)
    .join('');
}

async function initNewsPage() {
  const newsList = document.getElementById('news-list');
  if (!newsList) return;

  try {
    const [news, rules, devs] = await Promise.all([
      App.fetchJson('./data/developer_news.json'),
      App.fetchJson('./data/risk_rules.json'),
      App.fetchJson('./data/listed_developers.json')
    ]);

    const processed = news.items
      .map((item) => {
        const tagged = applyRiskRules(item, rules.rules);
        return { ...item, primarySeverity: tagged.primary, matches: tagged.matches };
      })
      .sort((a, b) => new Date(b.published) - new Date(a.published));

    const developers = Array.from(new Set([...devs.developers.map((d) => d.name), ...processed.flatMap((i) => i.developerTags || [])])).sort();
    const sources = Array.from(new Set(processed.map((i) => i.source))).sort();

    const sevSelect = document.getElementById('severity-filter');
    const devSelect = document.getElementById('developer-filter');
    const sourceSelect = document.getElementById('source-filter');

    sevSelect.insertAdjacentHTML('beforeend', '<option value="all" selected>All severities</option>');
    devSelect.insertAdjacentHTML('beforeend', '<option value="all" selected>All developers</option>');
    sourceSelect.insertAdjacentHTML('beforeend', '<option value="all" selected>All sources</option>');

    ['Critical', 'Warning', 'Watch', 'Info'].forEach((s) => sevSelect.insertAdjacentHTML('beforeend', `<option value="${s}">${s}</option>`));
    developers.forEach((d) => devSelect.insertAdjacentHTML('beforeend', `<option value="${d}">${d}</option>`));
    sources.forEach((s) => sourceSelect.insertAdjacentHTML('beforeend', `<option value="${s}">${s}</option>`));

    const runFilter = () => {
      const sevs = sevSelect.value;
      const devSel = devSelect.value;
      const srcSel = sourceSelect.value;
      const range = document.getElementById('date-range').value;
      const search = document.getElementById('search-text').value.trim().toLowerCase();
      const now = Date.now();
      const dayMap = { '7': 7, '30': 30, '90': 90, all: 9999 };

      const out = processed.filter((i) => {
        const ageDays = (now - new Date(i.published).getTime()) / (1000 * 60 * 60 * 24);
        if (ageDays > dayMap[range]) return false;
        if (sevs !== 'all' && i.primarySeverity !== sevs) return false;
        if (devSel !== 'all' && !(i.developerTags || []).includes(devSel)) return false;
        if (srcSel !== 'all' && i.source !== srcSel) return false;
        if (search) {
          const blob = `${i.title} ${i.summary}`.toLowerCase();
          if (!blob.includes(search)) return false;
        }
        return true;
      });
      renderNewsItems(out);
    };

    document.querySelectorAll('#news-filters select, #news-filters input').forEach((el) => el.addEventListener('change', runFilter));
    document.getElementById('search-text').addEventListener('input', runFilter);
    runFilter();
  } catch (e) {
    newsList.innerHTML = `<p class="empty">Unable to load news data: ${e.message}</p>`;
  }
}

document.addEventListener('DOMContentLoaded', initNewsPage);
