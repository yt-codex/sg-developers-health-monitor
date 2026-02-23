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
const REJECTED_LOG_PATH = path.join(DATA_DIR, 'rejected_items.log');
const RELEVANCE_RULES_PATH = path.join(CONFIG_DIR, 'relevance_rules.json');
const GOOGLE_QUERIES_PATH = path.join(CONFIG_DIR, 'google_news_queries.json');

const FEEDS = [
  { source: 'CNA', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936' },
  { source: 'CNA', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=10416' },
  { source: 'BT', url: 'https://www.businesstimes.com.sg/rss/property' },
  { source: 'BT', url: 'https://www.businesstimes.com.sg/rss/reits-property' },
  { source: 'ST', url: 'https://www.straitstimes.com/news/singapore/rss.xml' },
  { source: 'ST', url: 'https://www.straitstimes.com/news/business/rss.xml' }
];

const TRACKING_PREFIXES = ['utm_', 'fbclid', 'gclid', 'mc_cid', 'mc_eid', 'cmpid', 'igshid', 's_cid'];
const SEVERITY_SCORE = { warning: 3, watch: 2, info: 1 };

function mapSeverityToCurrent(severity) {
  const key = String(severity || 'info').toLowerCase();
  if (key === 'critical') return 'warning';
  if (key === 'warning') return 'watch';
  if (key === 'watch') return 'watch';
  return 'info';
}

function normalizeSeverityForScoring(severity) {
  const key = String(severity || 'info').toLowerCase();
  if (key === 'critical') return 'warning';
  return ['warning', 'watch', 'info'].includes(key) ? key : 'info';
}
const GOOGLE_RSS_BASE = 'https://news.google.com/rss/search';

const parser = XMLParser
  ? new XMLParser({
      ignoreAttributes: false,
      cdataPropName: '#cdata',
      trimValues: true,
      parseTagValue: true
    })
  : null;

function parseArgs(argv) {
  const parsed = {};
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const [key, value] = token.slice(2).split('=');
    parsed[key] = value === undefined ? true : value;
  }
  return {
    cleanup: Boolean(parsed.cleanup),
    source: String(parsed.source || 'default').toLowerCase(),
    mode: String(parsed.mode || 'delta').toLowerCase(),
    days: Number.parseInt(parsed.days || '7', 10),
    maxQueries: parsed.max_queries ? Number.parseInt(parsed.max_queries, 10) : null
  };
}

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
    return (link || '').trim();
  }
}

function normalizeTitle(text) {
  return (text || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function buildId(source, key) {
  return crypto.createHash('sha256').update(`${source}|${key}`).digest('hex').slice(0, 16);
}

function buildFallbackDedupKey(title, publisher, pubDate) {
  const datePart = pubDate ? new Date(pubDate).toISOString().slice(0, 10) : 'unknown';
  const raw = `${normalizeTitle(title)}|${normalizeTitle(publisher || '')}|${datePart}`;
  return `fallback:${crypto.createHash('sha256').update(raw).digest('hex').slice(0, 24)}`;
}

function deriveDedupKey(item) {
  if (item.resolved_link) return canonicalizeLink(item.resolved_link);
  if (item.source_url) return canonicalizeLink(item.source_url);
  if (item.link) return canonicalizeLink(item.link);
  return buildFallbackDedupKey(item.title, item.publisher, item.pubDate);
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
    const normalizedRuleSeverity = normalizeSeverityForScoring(rule.severity);
    if (SEVERITY_SCORE[normalizedRuleSeverity] > SEVERITY_SCORE[bestSeverity]) {
      bestSeverity = normalizedRuleSeverity;
    }
  }

  const fallback = normalizeSeverityForScoring(severityOrder[severityOrder.length - 1] || 'info');
  const severity = normalizeSeverityForScoring(bestSeverity || fallback);
  return {
    severity,
    tags: [...tags],
    matched_terms: [...matchedTerms]
  };
}

function extractDeveloper(text, developerConfig) {
  const hits = developerConfig.developers.filter((dev) => dev.aliases.some((alias) => text.includes(alias.toLowerCase())));
  if (hits.length === 0) return 'Unknown';
  if (hits.length > 1) return 'Multiple';
  return hits[0].name;
}

function escapeRegex(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function termToRegex(term) {
  const normalized = term.toLowerCase().trim().replace(/\s+/g, '\\s+');
  const escaped = escapeRegex(normalized).replace(/\\\s\+/g, '\\s+');
  const startsWord = /^[a-z0-9]/i.test(term);
  const endsWord = /[a-z0-9]$/i.test(term);
  const prefix = startsWord ? '\\b' : '';
  const suffix = endsWord ? '\\b' : '';
  return new RegExp(`${prefix}${escaped}${suffix}`, 'i');
}

function includesTerm(text, terms) {
  for (const term of terms) {
    if (termToRegex(term).test(text)) return term;
  }
  return null;
}

function findDeveloperMatches(text, developerConfig) {
  const matches = [];
  for (const dev of developerConfig.developers || []) {
    for (const alias of dev.aliases || []) {
      if (text.includes(alias.toLowerCase())) {
        matches.push(alias);
      }
    }
  }
  return [...new Set(matches)];
}

function isSingaporeContext(text, relevanceRules) {
  const directTerm = includesTerm(text, relevanceRules.singapore_context_terms || []);
  if (directTerm) return { pass: true, terms: [directTerm] };

  const optionalComboTerm = includesTerm(text, relevanceRules.singapore_optional_combo_terms || []);
  if (optionalComboTerm) {
    const anchor = includesTerm(text, relevanceRules.singapore_anchor_terms || []);
    if (anchor) {
      return { pass: true, terms: [optionalComboTerm, anchor] };
    }
  }

  return { pass: false, terms: [] };
}

function isPropertyDeveloperTopic(text, relevanceRules) {
  const groups = relevanceRules.property_developer_topic_groups || {};
  const matched = [];
  for (const terms of Object.values(groups)) {
    const hit = includesTerm(text, terms || []);
    if (hit) matched.push(hit);
  }
  return { pass: matched.length > 0, terms: matched };
}

function hasNegativeMatch(text, relevanceRules) {
  for (const [group, terms] of Object.entries(relevanceRules.hard_negative_groups || {})) {
    const term = includesTerm(text, terms || []);
    if (term) {
      return { group, term };
    }
  }
  return null;
}

function matchesManualAllowlist(text, relevanceRules) {
  const term = includesTerm(text, relevanceRules.manual_allowlist_terms || []);
  if (!term) return [];
  return [term];
}

function evaluateRelevance(text, developerConfig, relevanceRules) {
  const developerMatches = findDeveloperMatches(text, developerConfig);
  if (developerMatches.length > 0) {
    return {
      pass: true,
      relevance_reason: 'developer_match',
      relevance_terms: developerMatches,
      reject_reason: null
    };
  }

  const allowlistMatches = matchesManualAllowlist(text, relevanceRules);
  if (allowlistMatches.length > 0) {
    return {
      pass: true,
      relevance_reason: 'manual_allowlist',
      relevance_terms: allowlistMatches,
      reject_reason: null
    };
  }

  const negative = hasNegativeMatch(text, relevanceRules);
  if (negative) {
    return {
      pass: false,
      relevance_reason: null,
      relevance_terms: [],
      reject_reason: `hard_negative:${negative.group}:${negative.term}`
    };
  }

  const sgContext = isSingaporeContext(text, relevanceRules);
  if (!sgContext.pass) {
    return {
      pass: false,
      relevance_reason: null,
      relevance_terms: [],
      reject_reason: 'missing_singapore_context'
    };
  }

  const propertyTopic = isPropertyDeveloperTopic(text, relevanceRules);
  if (!propertyTopic.pass) {
    return {
      pass: false,
      relevance_reason: null,
      relevance_terms: [],
      reject_reason: 'missing_property_developer_topic'
    };
  }

  return {
    pass: true,
    relevance_reason: 'sg_property_topic',
    relevance_terms: [...new Set([...sgContext.terms, ...propertyTopic.terms])],
    reject_reason: null
  };
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
    const sourceUrl = stripHtml((block.match(/<source[^>]*url=["']([^"']+)["'][^>]*>/i) || [])[1] || '');
    if (!title || !link || !pubDate) continue;
    const parsed = Date.parse(pubDate);
    if (!Number.isFinite(parsed)) continue;
    items.push({ source, title, link, pubDate: new Date(parsed).toISOString(), snippet: description, source_url: sourceUrl || null });
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
      const sourceNode = node.source;
      const sourceUrl = typeof sourceNode === 'object' ? sourceNode['@_url'] || sourceNode.url : null;
      if (!title || !link || !pubDate) return null;
      const parsedDate = Date.parse(pubDate);
      if (!Number.isFinite(parsedDate)) return null;
      const iso = new Date(parsedDate).toISOString();
      return {
        source,
        title,
        link,
        pubDate: iso,
        snippet: description,
        source_url: sourceUrl || null
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

function buildGoogleUrl(query) {
  const url = new URL(GOOGLE_RSS_BASE);
  url.searchParams.set('q', query);
  url.searchParams.set('hl', 'en-SG');
  url.searchParams.set('gl', 'SG');
  url.searchParams.set('ceid', 'SG:en');
  return url.toString();
}

function parsePublisherFromTitle(title) {
  const parts = String(title || '').split(' - ');
  if (parts.length < 2) return null;
  return parts[parts.length - 1].trim() || null;
}

function trimPublisherFromTitle(title, publisher) {
  if (!publisher) return title;
  const suffix = ` - ${publisher}`;
  if (title.endsWith(suffix)) return title.slice(0, -suffix.length).trim();
  return title;
}

function decodeGoogleLinkCandidate(link) {
  try {
    const url = new URL(link);
    if (!url.hostname.includes('news.google.com')) return null;
    const direct = url.searchParams.get('url') || url.searchParams.get('u');
    if (direct) return direct;
    const pathMatch = url.pathname.match(/\/rss\/articles\/(.+)$/);
    if (!pathMatch) return null;
    const maybeEncoded = decodeURIComponent(pathMatch[1]);
    const embedded = maybeEncoded.match(/https?:\/\/[^\s&]+/i);
    return embedded ? embedded[0] : null;
  } catch {
    return null;
  }
}

async function resolveGoogleLink(item) {
  const original = item.link;
  if (item.source_url) {
    return { original_link: original, resolved_link: canonicalizeLink(item.source_url), resolution: 'source_url' };
  }

  const decoded = decodeGoogleLinkCandidate(original);
  if (decoded) {
    return { original_link: original, resolved_link: canonicalizeLink(decoded), resolution: 'decoded' };
  }

  try {
    const res = await fetch(original, {
      method: 'GET',
      redirect: 'follow',
      headers: { 'user-agent': 'sg-dev-health-monitor/1.0' }
    });
    return { original_link: original, resolved_link: canonicalizeLink(res.url), resolution: 'redirect' };
  } catch {
    return { original_link: original, resolved_link: canonicalizeLink(original), resolution: 'fallback' };
  }
}

function windowsForDays(days, mode) {
  if (mode === 'backfill') {
    const template = [7, 30, 90, 180, Math.max(days, 1)];
    const out = [...new Set(template.filter((d) => d <= days))];
    if (!out.includes(days)) out.push(days);
    return out.sort((a, b) => a - b);
  }
  return [Math.max(Math.min(days || 7, 30), 1)];
}

async function fetchGoogleNewsItems(queries, mode, days, maxQueries = null) {
  const selectedQueries = maxQueries ? queries.slice(0, maxQueries) : queries;
  const windows = windowsForDays(days, mode);
  const all = [];
  const feedResults = [];

  for (const query of selectedQueries) {
    for (const whenDays of windows) {
      const q = `${query} when:${whenDays}d`;
      const feed = { source: 'google_news', url: buildGoogleUrl(q) };
      try {
        const parsedItems = await fetchFeed(feed);
        feedResults.push({ source: 'google_news', query, window_days: whenDays, status: 'ok', items_fetched: parsedItems.length });
        for (const item of parsedItems) {
          all.push({ ...item, query });
        }
      } catch (error) {
        feedResults.push({
          source: 'google_news',
          query,
          window_days: whenDays,
          status: 'error',
          error: error.message,
          items_fetched: 0
        });
        console.error(`[update_news] google feed failed: query="${query}" when:${whenDays}d -> ${error.message}`);
      }
    }
  }

  return { items: all, feedResults };
}

function daysAgoCutoff(days) {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

function appendRejectedLog(entries) {
  if (entries.length === 0) return;
  const lines = entries.map((entry) => {
    const safeTitle = entry.title.replace(/\s*\|\s*/g, ' / ').trim();
    return `${entry.timestamp} | ${entry.source} | ${entry.reason} | ${safeTitle}`;
  });
  fs.appendFileSync(REJECTED_LOG_PATH, `${lines.join('\n')}\n`, 'utf8');
}

function refreshLatest90AndMeta(allItems, fetchedAtSgt, feedResults, existingCount, newItemsCount, rejectedCount) {
  const latest90 = allItems
    .filter((item) => new Date(item.pubDate).getTime() >= daysAgoCutoff(90))
    .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

  const hasSuccessfulFeed = feedResults.some((r) => r.status === 'ok');
  const hasFeedErrors = feedResults.some((r) => r.status === 'error');

  if (latest90.length > 0 || hasSuccessfulFeed || fs.existsSync(NEWS_90D_PATH)) {
    if (!(newItemsCount === 0 && !hasSuccessfulFeed && hasFeedErrors)) {
      writeJson(NEWS_90D_PATH, { items: latest90 });
    }
  }

  const meta = {
    last_updated_sgt: fetchedAtSgt,
    status: hasFeedErrors ? (hasSuccessfulFeed ? 'partial_success' : 'error') : 'success',
    counts: {
      existing_total: existingCount,
      new_items: newItemsCount,
      rejected_items: rejectedCount,
      total_all: allItems.length,
      latest_90d: latest90.length
    },
    feeds: feedResults
  };
  writeJson(META_PATH, meta);

  return latest90;
}

function cleanupExistingNews(items, developerConfig, relevanceRules) {
  const cleaned = [];
  const rejectedLogs = [];

  for (const item of items) {
    const combined = `${item.title || ''} ${item.snippet || ''}`.toLowerCase();
    const relevance = evaluateRelevance(combined, developerConfig, relevanceRules);
    if (!relevance.pass) {
      rejectedLogs.push({
        title: item.title || 'Untitled',
        source: item.source || 'unknown',
        reason: `cleanup_${relevance.reject_reason}`,
        timestamp: nowSgtIso()
      });
      continue;
    }

    cleaned.push({
      ...item,
      severity: mapSeverityToCurrent(item.severity),
      relevance_reason: relevance.relevance_reason,
      relevance_terms: relevance.relevance_terms
    });
  }

  return { cleaned, rejectedLogs };
}


function migrateLegacySeverities(items) {
  let changed = false;
  const migrated = items.map((item) => {
    const mappedSeverity = mapSeverityToCurrent(item.severity);
    if ((item.severity || 'info') !== mappedSeverity) changed = true;
    return { ...item, severity: mappedSeverity };
  });
  return { items: migrated, changed };
}

function backupNewsAll() {
  if (!fs.existsSync(NEWS_ALL_PATH)) return null;
  const now = new Date();
  const date = now.toISOString().slice(0, 10);
  const stamp = now.toISOString().replace(/[:.]/g, '-');
  const datedPath = path.join(DATA_DIR, `news_all_backup_${date}.json`);
  const stampedPath = path.join(DATA_DIR, `news_all_backup_${stamp}.json`);
  const backupPath = fs.existsSync(datedPath) ? stampedPath : datedPath;
  fs.copyFileSync(NEWS_ALL_PATH, backupPath);
  return backupPath;
}

async function run() {
  ensureDir(DATA_DIR);
  const args = parseArgs(process.argv.slice(2));

  const tagRules = readJson(path.join(CONFIG_DIR, 'tag_rules.json'), null);
  const developerConfig = readJson(path.join(CONFIG_DIR, 'developers.json'), null);
  const relevanceRules = readJson(RELEVANCE_RULES_PATH, null);
  if (!tagRules || !developerConfig || !relevanceRules) {
    throw new Error('Missing config/tag_rules.json, config/developers.json, or config/relevance_rules.json');
  }

  if (args.cleanup) {
    const allStore = readJson(NEWS_ALL_PATH, { items: [] });
    const existingItemsRaw = Array.isArray(allStore.items) ? allStore.items : [];
    const migrated = migrateLegacySeverities(existingItemsRaw);
    const backupPath = backupNewsAll();
    const existingItems = migrated.items;
    const { cleaned, rejectedLogs } = cleanupExistingNews(existingItems, developerConfig, relevanceRules);

    writeJson(NEWS_ALL_PATH, { items: cleaned });
    const cleanup90 = cleaned
      .filter((item) => new Date(item.pubDate).getTime() >= daysAgoCutoff(90))
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
    writeJson(NEWS_90D_PATH, { items: cleanup90 });
    appendRejectedLog(rejectedLogs);
    writeJson(META_PATH, {
      last_updated_sgt: nowSgtIso(),
      status: 'success',
      counts: {
        existing_total: existingItems.length,
        new_items: 0,
        rejected_items: rejectedLogs.length,
        total_all: cleaned.length,
        latest_90d: cleanup90.length
      },
      feeds: []
    });
    console.log(
      `[update_news] cleanup complete. backup=${backupPath || 'none'} original=${existingItems.length} cleaned=${cleaned.length} rejected=${rejectedLogs.length}`
    );
    return;
  }

  const compiledRules = compileRules(tagRules);
  const allStore = readJson(NEWS_ALL_PATH, { items: [] });
  let existingItems = Array.isArray(allStore.items) ? allStore.items : [];
  const migrated = migrateLegacySeverities(existingItems);
  if (migrated.changed) {
    backupNewsAll();
    existingItems = migrated.items;
    writeJson(NEWS_ALL_PATH, { items: existingItems });
    console.log('[update_news] migrated legacy severities in data/news_all.json');
  }
  const existingDedup = new Set(existingItems.map((item) => deriveDedupKey(item)));
  const fetchedAtSgt = nowSgtIso();

  const feedResults = [];
  const newItems = [];
  const rejectedLogs = [];

  if (args.source === 'google') {
    const config = readJson(GOOGLE_QUERIES_PATH, null);
    const queries = Array.isArray(config?.queries) ? config.queries : [];
    if (queries.length === 0) {
      throw new Error('Missing or empty config/google_news_queries.json (queries array required)');
    }

    const { items: parsedItems, feedResults: googleResults } = await fetchGoogleNewsItems(
      queries,
      args.mode,
      Number.isFinite(args.days) ? args.days : 7,
      args.maxQueries
    );
    feedResults.push(...googleResults);

    for (const rawItem of parsedItems) {
      const publisherFromTitle = parsePublisherFromTitle(rawItem.title);
      const resolved = await resolveGoogleLink(rawItem);
      const resolvedLink = resolved.resolved_link || canonicalizeLink(rawItem.link);
      const cleanedTitle = trimPublisherFromTitle(rawItem.title, publisherFromTitle);
      const dedupKey = resolvedLink || canonicalizeLink(rawItem.source_url || rawItem.link);
      if (existingDedup.has(dedupKey)) continue;

      const combined = `${cleanedTitle} ${rawItem.snippet || ''}`.toLowerCase();
      const relevance = evaluateRelevance(combined, developerConfig, relevanceRules);
      if (!relevance.pass) {
        rejectedLogs.push({
          title: cleanedTitle,
          source: 'google_news',
          reason: relevance.reject_reason,
          timestamp: fetchedAtSgt
        });
        continue;
      }

      const { severity, tags, matched_terms } = classifySeverity(combined, compiledRules, tagRules.severity_order);
      const id = buildId('google_news', dedupKey || buildFallbackDedupKey(cleanedTitle, publisherFromTitle, rawItem.pubDate));
      newItems.push({
        id,
        title: cleanedTitle,
        original_link: rawItem.link,
        resolved_link: resolvedLink,
        link: resolvedLink || rawItem.link,
        source: 'google_news',
        aggregator: 'google_news',
        publisher: publisherFromTitle || null,
        query: rawItem.query,
        pubDate: rawItem.pubDate,
        pubDate_sgt: toSgtIso(rawItem.pubDate),
        developer: extractDeveloper(combined, developerConfig),
        severity,
        tags,
        snippet: rawItem.snippet,
        matched_terms,
        fetched_at_sgt: fetchedAtSgt,
        relevance_reason: relevance.relevance_reason,
        relevance_terms: relevance.relevance_terms
      });
      existingDedup.add(dedupKey || buildFallbackDedupKey(cleanedTitle, publisherFromTitle, rawItem.pubDate));
    }
  } else {
    for (const feed of FEEDS) {
      try {
        const parsedItems = await fetchFeed(feed);
        feedResults.push({ source: feed.source, url: feed.url, status: 'ok', items_fetched: parsedItems.length });

        for (const rawItem of parsedItems) {
          const canonicalLink = canonicalizeLink(rawItem.link);
          if (existingDedup.has(canonicalLink)) continue;

          const combined = `${rawItem.title} ${rawItem.snippet}`.toLowerCase();
          const relevance = evaluateRelevance(combined, developerConfig, relevanceRules);
          if (!relevance.pass) {
            rejectedLogs.push({
              title: rawItem.title,
              source: rawItem.source,
              reason: relevance.reject_reason,
              timestamp: fetchedAtSgt
            });
            continue;
          }

          const { severity, tags, matched_terms } = classifySeverity(combined, compiledRules, tagRules.severity_order);
          const item = {
            id: buildId(rawItem.source, canonicalLink),
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
            fetched_at_sgt: fetchedAtSgt,
            relevance_reason: relevance.relevance_reason,
            relevance_terms: relevance.relevance_terms
          };
          existingDedup.add(canonicalLink);
          newItems.push(item);
        }
      } catch (error) {
        feedResults.push({ source: feed.source, url: feed.url, status: 'error', error: error.message, items_fetched: 0 });
        console.error(`[update_news] feed failed: ${feed.url} -> ${error.message}`);
      }
    }
  }

  const mergedAllItems = newItems.length > 0 ? [...existingItems, ...newItems] : existingItems;
  if (newItems.length > 0) {
    writeJson(NEWS_ALL_PATH, { items: mergedAllItems });
  } else if (!fs.existsSync(NEWS_ALL_PATH)) {
    writeJson(NEWS_ALL_PATH, { items: existingItems });
  }

  appendRejectedLog(rejectedLogs);
  const latest90 = refreshLatest90AndMeta(
    mergedAllItems,
    fetchedAtSgt,
    feedResults,
    existingItems.length,
    newItems.length,
    rejectedLogs.length
  );

  console.log(
    `[update_news] done. source=${args.source} mode=${args.mode} days=${args.days} new_items=${newItems.length} rejected=${rejectedLogs.length} total_all=${mergedAllItems.length} latest_90d=${latest90.length}`
  );
}

run().catch((error) => {
  console.error(`[update_news] fatal: ${error.message}`);
  process.exitCode = 1;
});
