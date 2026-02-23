const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
let XMLParser;
try {
  ({ XMLParser } = require('fast-xml-parser'));
} catch {
  XMLParser = null;
}

const ROOT = process.cwd();
const DATA_DIR = path.join(ROOT, 'data');
const CONFIG_DIR = path.join(ROOT, 'config');
const NEWS_ALL_PATH = path.join(DATA_DIR, 'news_all.json');
const NEWS_90D_PATH = path.join(DATA_DIR, 'news_latest_90d.json');
const META_PATH = path.join(DATA_DIR, 'meta.json');

const FEEDS = [
  { source: 'CNA', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml' },
  { source: 'CNA', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936' },
  { source: 'CNA', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=10416' },
  { source: 'BT', url: 'https://www.businesstimes.com.sg/rss/property' },
  { source: 'BT', url: 'https://www.businesstimes.com.sg/rss/reits-property' },
  { source: 'ST', url: 'https://www.straitstimes.com/news/singapore/rss.xml' },
  { source: 'ST', url: 'https://www.straitstimes.com/news/business/rss.xml' }
];

const TRACKING_PREFIXES = ['utm_', 'fbclid', 'gclid', 'mc_cid', 'mc_eid', 'cmpid', 'igshid', 's_cid'];
const SEVERITY_SCORE = { critical: 4, warning: 3, watch: 2, info: 1 };

const parser = XMLParser
  ? new XMLParser({
      ignoreAttributes: false,
      cdataPropName: '#cdata',
      trimValues: true,
      parseTagValue: true
    })
  : null;

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
}

function nowSgtIso() {
  const now = new Date();
  const sgt = new Date(now.getTime() + 8 * 60 * 60 * 1000);
  return `${sgt.toISOString().replace('Z', '')}+08:00`;
}

function toSgtIso(input) {
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return null;
  const sgt = new Date(d.getTime() + 8 * 60 * 60 * 1000);
  return `${sgt.toISOString().replace('Z', '')}+08:00`;
}

function stripHtml(text = '') {
  return text
    .replace(/<!\[CDATA\[(.*?)\]\]>/gis, '$1')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function asArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function canonicalizeLink(link) {
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
    return link;
  }
}

function buildId(source, canonicalLink) {
  return crypto.createHash('sha256').update(`${source}|${canonicalLink}`).digest('hex').slice(0, 16);
}

function compileRules(tagRules) {
  return tagRules.rules.flatMap((rule) =>
    rule.patterns.map((pattern) => ({
      severity: rule.severity,
      tag: rule.tag,
      pattern,
      regex: new RegExp(pattern, 'i')
    }))
  );
}

function classifySeverity(text, compiledRules, severityOrder) {
  const tags = new Set();
  const matchedTerms = new Set();
  let bestSeverity = 'info';

  for (const rule of compiledRules) {
    const match = text.match(rule.regex);
    if (!match) continue;
    tags.add(rule.tag);
    matchedTerms.add(match[0]);
    if (SEVERITY_SCORE[rule.severity] > SEVERITY_SCORE[bestSeverity]) {
      bestSeverity = rule.severity;
    }
  }

  const fallback = severityOrder[severityOrder.length - 1] || 'info';
  return {
    severity: bestSeverity || fallback,
    tags: [...tags],
    matched_terms: [...matchedTerms]
  };
}

function extractDeveloper(text, developerConfig) {
  const hits = developerConfig.developers.filter((dev) =>
    dev.aliases.some((alias) => text.includes(alias.toLowerCase()))
  );
  if (hits.length === 0) return 'Unknown';
  if (hits.length > 1) return 'Multiple';
  return hits[0].name;
}


function parseItemsWithRegex(xml, source) {
  const items = [];
  const itemMatches = [...xml.matchAll(/<item\b[\s\S]*?<\/item>/gi)];
  for (const match of itemMatches) {
    const block = match[0];
    const title = stripHtml((block.match(/<title>([\s\S]*?)<\/title>/i) || [])[1] || 'Untitled');
    const link = stripHtml((block.match(/<link>([\s\S]*?)<\/link>/i) || [])[1] || '');
    const description = stripHtml((block.match(/<description>([\s\S]*?)<\/description>/i) || [])[1] || '');
    const pubDate = stripHtml((block.match(/<pubDate>([\s\S]*?)<\/pubDate>/i) || [])[1] || '');
    if (!title || !link || !pubDate) continue;
    const parsed = Date.parse(pubDate);
    if (!Number.isFinite(parsed)) continue;
    items.push({ source, title, link, pubDate: new Date(parsed).toISOString(), snippet: description });
  }
  return items;
}

function parseItemsFromXml(xml, source) {
  if (!parser) return parseItemsWithRegex(xml, source);
  const doc = parser.parse(xml);
  const rssItems = asArray(doc?.rss?.channel?.item);
  const atomEntries = asArray(doc?.feed?.entry);
  const nodes = rssItems.length ? rssItems : atomEntries;

  return nodes
    .map((node) => {
      const title = stripHtml(node.title?.['#text'] || node.title || 'Untitled');
      const linkRaw = typeof node.link === 'object' ? node.link['@_href'] || node.link.href : node.link;
      const link = (linkRaw || '').trim();
      const description = stripHtml(node.description || node.summary || node.content || '');
      const pubDate = node.pubDate || node.published || node.updated || node['dc:date'];
      if (!title || !link || !pubDate) return null;
      const parsedDate = Date.parse(pubDate);
      if (!Number.isFinite(parsedDate)) return null;
      const iso = new Date(parsedDate).toISOString();
      return {
        source,
        title,
        link,
        pubDate: iso,
        snippet: description
      };
    })
    .filter(Boolean);
}

async function fetchFeed(feed) {
  const res = await fetch(feed.url, { headers: { 'user-agent': 'sg-dev-health-monitor/1.0' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const body = await res.text();
  return parseItemsFromXml(body, feed.source);
}

function daysAgoCutoff(days) {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

async function run() {
  ensureDir(DATA_DIR);

  const tagRules = readJson(path.join(CONFIG_DIR, 'tag_rules.json'), null);
  const developerConfig = readJson(path.join(CONFIG_DIR, 'developers.json'), null);
  if (!tagRules || !developerConfig) {
    throw new Error('Missing config/tag_rules.json or config/developers.json');
  }

  const compiledRules = compileRules(tagRules);
  const allStore = readJson(NEWS_ALL_PATH, { items: [] });
  const existingItems = Array.isArray(allStore.items) ? allStore.items : [];
  const existingIds = new Set(existingItems.map((item) => item.id));
  const fetchedAtSgt = nowSgtIso();

  const feedResults = [];
  const newItems = [];

  for (const feed of FEEDS) {
    try {
      const parsedItems = await fetchFeed(feed);
      feedResults.push({ source: feed.source, url: feed.url, status: 'ok', items_fetched: parsedItems.length });

      for (const rawItem of parsedItems) {
        const canonicalLink = canonicalizeLink(rawItem.link);
        const id = buildId(rawItem.source, canonicalLink);
        if (existingIds.has(id)) continue;

        const combined = `${rawItem.title} ${rawItem.snippet}`.toLowerCase();
        const { severity, tags, matched_terms } = classifySeverity(combined, compiledRules, tagRules.severity_order);
        const item = {
          id,
          title: rawItem.title,
          link: canonicalLink,
          source: rawItem.source,
          pubDate: rawItem.pubDate,
          pubDate_sgt: toSgtIso(rawItem.pubDate),
          developer: extractDeveloper(combined, developerConfig),
          severity,
          tags,
          snippet: rawItem.snippet,
          matched_terms,
          fetched_at_sgt: fetchedAtSgt
        };
        existingIds.add(id);
        newItems.push(item);
      }
    } catch (error) {
      feedResults.push({ source: feed.source, url: feed.url, status: 'error', error: error.message, items_fetched: 0 });
      console.error(`[update_news] feed failed: ${feed.url} -> ${error.message}`);
    }
  }

  if (newItems.length > 0) {
    writeJson(NEWS_ALL_PATH, { items: [...existingItems, ...newItems] });
  } else if (!fs.existsSync(NEWS_ALL_PATH)) {
    writeJson(NEWS_ALL_PATH, { items: existingItems });
  }

  const mergedAllItems = newItems.length > 0 ? [...existingItems, ...newItems] : existingItems;
  const latest90 = mergedAllItems
    .filter((item) => new Date(item.pubDate).getTime() >= daysAgoCutoff(90))
    .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

  const hasSuccessfulFeed = feedResults.some((r) => r.status === 'ok');
  const hasFeedErrors = feedResults.some((r) => r.status === 'error');

  if (latest90.length > 0 || hasSuccessfulFeed || fs.existsSync(NEWS_90D_PATH)) {
    if (!(newItems.length === 0 && !hasSuccessfulFeed && hasFeedErrors)) {
      writeJson(NEWS_90D_PATH, { items: latest90 });
    }
  }

  const meta = {
    last_updated_sgt: fetchedAtSgt,
    status: hasFeedErrors ? (hasSuccessfulFeed ? 'partial_success' : 'error') : 'success',
    counts: {
      existing_total: existingItems.length,
      new_items: newItems.length,
      total_all: mergedAllItems.length,
      latest_90d: latest90.length
    },
    feeds: feedResults
  };
  writeJson(META_PATH, meta);

  console.log(`[update_news] done. new_items=${newItems.length} total_all=${mergedAllItems.length} latest_90d=${latest90.length}`);
}

run().catch((error) => {
  console.error(`[update_news] fatal: ${error.message}`);
  process.exitCode = 1;
});
