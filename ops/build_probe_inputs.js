#!/usr/bin/env node

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

const NEWS_REQUIRED_FIELDS = ['id', 'title', 'link', 'source', 'pubDate', 'severity'];
const MAS_REQUIRED_FIELDS = ['year', 'month', 'bc_lmtgrtd', 'bc_utl'];
const UPSTREAM_WORKFLOW_NAMES = [
  'Update dashboard data',
  'Update macro indicators',
  'Update developer news'
];
const STATUS_PRIORITY = { OK: 0, WARN: 1, FAIL: 2 };
const THIRTY_SIX_HOURS_SECONDS = 36 * 60 * 60;
const SEVENTY_TWO_HOURS_SECONDS = 72 * 60 * 60;
const REQUIRED_INPUT_FILES = [
  'data/meta.json',
  'data/news_all.json',
  'data/news_latest_90d.json',
  'data/macro_indicators.json',
  'data/processed/developer_ratios_history.json'
];
const OPTIONAL_INPUT_FILES = ['data/processed/developer_health_diagnostics.json'];
const DEFAULT_TMP_DIR = path.join('ops', 'tmp');

function toIsoUtc(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function parseDate(value) {
  if (!value || typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function toInt(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function toLagSeconds(now, past) {
  if (!(now instanceof Date) || !(past instanceof Date)) return null;
  const lag = Math.floor((now.getTime() - past.getTime()) / 1000);
  return lag > 0 ? lag : 0;
}

function elevateStatus(current, next) {
  if (!Object.prototype.hasOwnProperty.call(STATUS_PRIORITY, next)) return current;
  if (!Object.prototype.hasOwnProperty.call(STATUS_PRIORITY, current)) return next;
  return STATUS_PRIORITY[next] > STATUS_PRIORITY[current] ? next : current;
}

function normalizeCheckStatus(value) {
  const upper = String(value || 'WARN').toUpperCase();
  return upper === 'OK' || upper === 'WARN' || upper === 'FAIL' ? upper : 'WARN';
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function readJsonFile(rootDir, relativePath) {
  const absolutePath = path.join(rootDir, relativePath);
  if (!fs.existsSync(absolutePath)) {
    return { ok: false, error: `Missing file: ${relativePath}`, data: null };
  }

  try {
    const raw = fs.readFileSync(absolutePath, 'utf8');
    const data = JSON.parse(raw);
    return { ok: true, error: null, data };
  } catch (error) {
    return {
      ok: false,
      error: `Invalid JSON in ${relativePath}: ${error.message}`,
      data: null
    };
  }
}

function stableClone(value) {
  if (Array.isArray(value)) {
    return value.map(stableClone);
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = stableClone(value[key]);
    }
    return out;
  }
  return value;
}

function stableStringify(value) {
  return JSON.stringify(stableClone(value));
}

function buildObjectKeySignature(objects) {
  const union = new Set();
  const shapeCounts = new Map();
  for (const item of objects) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const keys = Object.keys(item).sort();
    keys.forEach((key) => union.add(key));
    const shape = keys.join('|');
    shapeCounts.set(shape, (shapeCounts.get(shape) || 0) + 1);
  }
  return {
    union_keys: [...union].sort(),
    shapes: [...shapeCounts.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([keys, count]) => ({ keys: keys ? keys.split('|') : [], count }))
  };
}

function toHumanReadableSignature(signature) {
  return signature.union_keys.join(',');
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function isValidUri(value) {
  if (!value || typeof value !== 'string') return false;
  try {
    const parsed = new URL(value);
    return Boolean(parsed.protocol);
  } catch {
    return false;
  }
}

function dedupeArtifacts(artifacts) {
  const seen = new Set();
  const out = [];
  for (const artifact of artifacts) {
    if (!artifact || typeof artifact !== 'object') continue;
    const label = String(artifact.label || 'artifact').trim() || 'artifact';
    const url = String(artifact.url || '').trim();
    if (!isValidUri(url)) continue;
    if (seen.has(url)) continue;
    seen.add(url);
    out.push({ label, url });
  }
  return out;
}

function slugifyWorkflowName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 64) || 'workflow';
}

function getCurrentRunMeta(env) {
  const repo = env.GITHUB_REPOSITORY || null;
  const runId = env.GITHUB_RUN_ID || null;
  const serverUrl = env.GITHUB_SERVER_URL || 'https://github.com';
  const runUrl = repo && runId ? `${serverUrl}/${repo}/actions/runs/${runId}` : null;
  return {
    repo,
    run_id: runId,
    run_url: runUrl,
    workflow: env.GITHUB_WORKFLOW || null,
    job: env.GITHUB_JOB || null,
    sha: env.GITHUB_SHA || null
  };
}

function buildArtifactUrl(rootDir, relativePath, meta) {
  const normalized = relativePath.replace(/\\/g, '/');
  if (meta.repo && meta.sha) {
    return `https://github.com/${meta.repo}/blob/${meta.sha}/${normalized}`;
  }
  return pathToFileURL(path.join(rootDir, relativePath)).toString();
}

async function fetchLatestWorkflowRuns({
  repo,
  token,
  workflowNames = UPSTREAM_WORKFLOW_NAMES
}) {
  if (!repo || !token) {
    return {
      runsByName: {},
      warning: 'Missing GITHUB_REPOSITORY or GITHUB_TOKEN; workflow conclusions unavailable.'
    };
  }

  const url = `https://api.github.com/repos/${repo}/actions/runs?per_page=100`;
  const response = await fetch(url, {
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28'
    }
  });

  if (!response.ok) {
    throw new Error(`GitHub Actions API failed (${response.status})`);
  }

  const payload = await response.json();
  const workflowRuns = asArray(payload.workflow_runs);
  const runsByName = {};

  for (const workflowName of workflowNames) {
    const match = workflowRuns.find((run) => run && run.name === workflowName);
    if (!match) continue;
    runsByName[workflowName] = {
      id: match.id || null,
      name: match.name || workflowName,
      status: match.status || null,
      conclusion: match.conclusion || null,
      completed_at: match.completed_at || null,
      created_at: match.created_at || null,
      html_url: match.html_url || null,
      event: match.event || null
    };
  }

  return { runsByName, warning: null };
}

function getBaselineMacroFailures(previousProbe) {
  const rowCounts = asObject(previousProbe?.row_counts);
  return {
    macro_failed_count: toInt(rowCounts.macro_failed_count, 0),
    macro_failed_series_count: toInt(rowCounts.macro_failed_series_count, 0)
  };
}

function addCheckFactory({ keyChecks, warnings, getStatus, setStatus }) {
  return function addCheck(name, status, detail, metric) {
    const normalized = normalizeCheckStatus(status);
    const check = { name: String(name), status: normalized };
    if (detail != null && String(detail).trim()) check.detail = String(detail).trim();
    if (metric !== undefined) check.metric = metric;
    keyChecks.push(check);
    setStatus(elevateStatus(getStatus(), normalized));
    if (normalized !== 'OK' && check.detail) warnings.push(`${check.name}: ${check.detail}`);
  };
}

function findMissingNewsFields(items, label) {
  const missing = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item || typeof item !== 'object' || Array.isArray(item)) {
      missing.push(`${label}[${index}] is not an object`);
      continue;
    }
    for (const field of NEWS_REQUIRED_FIELDS) {
      const value = item[field];
      if (value == null || String(value).trim() === '') {
        missing.push(`${label}[${index}] missing ${field}`);
      }
    }
  }
  return missing;
}

function deriveFreshnessStatus(lagSeconds) {
  if (lagSeconds == null) return 'FAIL';
  if (lagSeconds > SEVENTY_TWO_HOURS_SECONDS) return 'FAIL';
  if (lagSeconds > THIRTY_SIX_HOURS_SECONDS) return 'WARN';
  return 'OK';
}

function buildFailureFallback({
  now,
  error,
  meta,
  artifactLinks
}) {
  return {
    status: 'FAIL',
    freshness: {
      max_date: null,
      lag_seconds: null
    },
    row_counts: {
      news_feeds_total: 0,
      news_feeds_ok: 0,
      news_feeds_error: 0,
      news_feeds_nonempty: 0,
      news_total_all: 0,
      news_latest_90d: 0,
      news_new_items: 0,
      macro_ok_count: 0,
      macro_failed_count: 0,
      macro_failed_series_count: 0,
      dev_total: 0,
      dev_ok: 0,
      dev_partial: 0,
      dev_error: 0
    },
    schema_hash: sha256(`fallback:${String(error || 'unknown')}`),
    key_checks: [
      {
        name: 'build_probe_inputs',
        status: 'FAIL',
        detail: `Probe input builder failed: ${String(error || 'unknown error')}`
      }
    ],
    warnings: [`Probe input builder failed at ${toIsoUtc(now)}: ${String(error || 'unknown error')}`],
    artifact_links: dedupeArtifacts(artifactLinks),
    meta: {
      ...meta,
      generated_at_utc: toIsoUtc(now),
      schema_signatures: {},
      workflow_conclusions: {}
    }
  };
}

async function buildProbeInputs(options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const now = options.now instanceof Date ? options.now : new Date();
  const env = options.env || process.env;
  const workflowNames = options.workflowNames || UPSTREAM_WORKFLOW_NAMES;
  const loadWorkflowRuns = options.fetchWorkflowRuns || fetchLatestWorkflowRuns;
  const providedWorkflowRuns = options.workflowRunsByName || null;
  const previousProbe = options.previousProbe || null;

  const warnings = [];
  const keyChecks = [];
  let status = 'OK';
  const getStatus = () => status;
  const setStatus = (value) => {
    status = value;
  };
  const addCheck = addCheckFactory({ keyChecks, warnings, getStatus, setStatus });

  const meta = getCurrentRunMeta(env);
  const artifactLinks = [];
  if (meta.run_url) {
    artifactLinks.push({ label: 'workflow_run', url: meta.run_url });
  }

  const requiredFiles = {};
  const optionalFiles = {};

  for (const relativePath of REQUIRED_INPUT_FILES) {
    const loaded = readJsonFile(rootDir, relativePath);
    requiredFiles[relativePath] = loaded;
    if (!loaded.ok) {
      addCheck(`required_file.${relativePath.replace(/[\\/]/g, '_')}`, 'FAIL', loaded.error);
    } else {
      addCheck(`required_file.${relativePath.replace(/[\\/]/g, '_')}`, 'OK', 'parsed');
      artifactLinks.push({
        label: `input:${relativePath.replace(/[\\/]/g, '_')}`,
        url: buildArtifactUrl(rootDir, relativePath, meta)
      });
    }
  }

  for (const relativePath of OPTIONAL_INPUT_FILES) {
    const loaded = readJsonFile(rootDir, relativePath);
    optionalFiles[relativePath] = loaded;
    if (loaded.ok) {
      artifactLinks.push({
        label: `input:${relativePath.replace(/[\\/]/g, '_')}`,
        url: buildArtifactUrl(rootDir, relativePath, meta)
      });
    } else if (!String(loaded.error || '').startsWith('Missing file:')) {
      addCheck(`optional_file.${relativePath.replace(/[\\/]/g, '_')}`, 'WARN', loaded.error);
    }
  }

  const metaJson = asObject(requiredFiles['data/meta.json']?.data);
  const newsAllJson = asObject(requiredFiles['data/news_all.json']?.data);
  const newsLatestJson = asObject(requiredFiles['data/news_latest_90d.json']?.data);
  const macroJson = asObject(requiredFiles['data/macro_indicators.json']?.data);
  const ratiosJson = asObject(requiredFiles['data/processed/developer_ratios_history.json']?.data);
  const diagnosticsJson = asObject(optionalFiles['data/processed/developer_health_diagnostics.json']?.data);

  const newsAllItems = asArray(newsAllJson.items);
  const newsLatestItems = asArray(newsLatestJson.items);
  const feeds = asArray(metaJson.feeds);
  const macroIndicators = asObject(macroJson.macro_indicators);
  const macroUpdateRun = asObject(macroIndicators.update_run);
  const macroSeries = asObject(macroIndicators.series);
  const macroSeriesEntries = Object.values(macroSeries).filter((value) => value && typeof value === 'object' && !Array.isArray(value));
  const developers = asArray(ratiosJson.developers);

  const rowCounts = {
    news_feeds_total: feeds.length,
    news_feeds_ok: feeds.filter((feed) => String(feed?.status || '').toLowerCase() === 'ok').length,
    news_feeds_error: feeds.filter((feed) => String(feed?.status || '').toLowerCase() === 'error').length,
    news_feeds_nonempty: feeds.filter((feed) => toInt(feed?.items_fetched, 0) > 0).length,
    news_total_all: newsAllItems.length,
    news_latest_90d: newsLatestItems.length,
    news_new_items: toInt(metaJson?.counts?.new_items, 0),
    macro_ok_count: toInt(macroUpdateRun.ok_count, 0),
    macro_failed_count: Math.max(
      toInt(macroUpdateRun.failed_count, 0),
      asArray(macroUpdateRun.failed_items).length
    ),
    macro_failed_series_count: macroSeriesEntries.filter(
      (series) => String(series?.status || '').toLowerCase() === 'failed'
    ).length,
    dev_total: developers.length,
    dev_ok: developers.filter((dev) => String(dev?.fetchStatus || '').toLowerCase() === 'ok').length,
    dev_partial: developers.filter((dev) => String(dev?.fetchStatus || '').toLowerCase() === 'partial').length,
    dev_error: developers.filter((dev) => String(dev?.fetchStatus || '').toLowerCase() === 'error').length
  };

  const allFeedsFailed = rowCounts.news_feeds_total > 0 && rowCounts.news_feeds_error === rowCounts.news_feeds_total;
  const allFeedsEmpty = rowCounts.news_feeds_total > 0 && rowCounts.news_feeds_nonempty === 0;
  const partialFeedFailures = rowCounts.news_feeds_error > 0 && rowCounts.news_feeds_ok > 0;

  if (rowCounts.news_feeds_total === 0) {
    addCheck('news.feed_health', 'FAIL', 'meta.feeds is empty');
  } else if (allFeedsFailed) {
    addCheck('news.feed_health', 'FAIL', 'all feeds failed', rowCounts.news_feeds_error);
  } else if (allFeedsEmpty) {
    addCheck('news.feed_health', 'FAIL', 'all feeds returned zero items', rowCounts.news_feeds_nonempty);
  } else if (partialFeedFailures) {
    addCheck(
      'news.feed_health',
      'WARN',
      `${rowCounts.news_feeds_error}/${rowCounts.news_feeds_total} feeds failed`,
      rowCounts.news_feeds_error
    );
  } else {
    addCheck('news.feed_health', 'OK', 'all feeds healthy', rowCounts.news_feeds_ok);
  }

  const missingInAll = findMissingNewsFields(newsAllItems, 'news_all.items');
  const missingInLatest = findMissingNewsFields(newsLatestItems, 'news_latest_90d.items');
  const missingFields = [...missingInAll, ...missingInLatest];
  if (missingFields.length > 0) {
    addCheck(
      'news.schema_required_fields',
      'FAIL',
      `required field drift detected (${missingFields.length}); e.g. ${missingFields.slice(0, 3).join(' | ')}`,
      missingFields.length
    );
  } else {
    addCheck('news.schema_required_fields', 'OK', 'required fields present');
  }

  const macroFailedItems = asArray(macroUpdateRun.failed_items);
  if (rowCounts.macro_ok_count === 0) {
    addCheck('macro.ok_count', 'FAIL', 'macro_indicators.update_run.ok_count == 0');
  } else {
    addCheck('macro.ok_count', 'OK', `ok_count=${rowCounts.macro_ok_count}`, rowCounts.macro_ok_count);
  }

  const baseline = getBaselineMacroFailures(previousProbe);
  const macroRegression =
    rowCounts.macro_failed_count > baseline.macro_failed_count ||
    rowCounts.macro_failed_series_count > baseline.macro_failed_series_count;

  if (rowCounts.macro_ok_count > 0 && (rowCounts.macro_failed_count > 0 || rowCounts.macro_failed_series_count > 0)) {
    if (macroRegression) {
      const failedSeriesMessages = macroSeriesEntries
        .filter((series) => String(series?.status || '').toLowerCase() === 'failed')
        .map((series) => String(series?.error_summary || 'unknown series failure'))
        .slice(0, 3);
      addCheck(
        'macro.partial_degradation',
        'WARN',
        `macro failures increased vs baseline (${baseline.macro_failed_count}/${baseline.macro_failed_series_count} -> ${rowCounts.macro_failed_count}/${rowCounts.macro_failed_series_count}); ${failedSeriesMessages.join(' | ')}`,
        rowCounts.macro_failed_series_count
      );
    } else {
      addCheck(
        'macro.partial_degradation',
        'OK',
        `macro failures unchanged vs baseline (${rowCounts.macro_failed_count}/${rowCounts.macro_failed_series_count})`,
        rowCounts.macro_failed_series_count
      );
    }
  } else {
    addCheck('macro.partial_degradation', 'OK', 'no macro failed items/series');
  }

  if (macroFailedItems.length > 0) {
    const sample = macroFailedItems
      .slice(0, 3)
      .map((item) => `${item?.name || 'unknown'}:${item?.error_summary || 'no_error_summary'}`)
      .join(' | ');
    warnings.push(`macro.failed_items: ${sample}`);
  }

  if (rowCounts.dev_ok === 0) {
    addCheck('developer.fetch_health', 'FAIL', 'developer ratios have zero ok records');
  } else if (rowCounts.dev_partial > 0 || rowCounts.dev_error > 0) {
    addCheck(
      'developer.fetch_health',
      'WARN',
      `partial=${rowCounts.dev_partial} error=${rowCounts.dev_error}`,
      { partial: rowCounts.dev_partial, error: rowCounts.dev_error }
    );
  } else {
    addCheck('developer.fetch_health', 'OK', 'all developers fetchStatus=ok', rowCounts.dev_ok);
  }

  const problematicDevelopers = developers
    .filter((dev) => {
      const state = String(dev?.fetchStatus || '').toLowerCase();
      return state === 'partial' || state === 'error';
    })
    .slice(0, 5)
    .map((dev) => `${dev?.ticker || 'unknown'}:${dev?.fetchStatus || 'unknown'}:${dev?.fetchError || 'no fetchError'}`);
  if (problematicDevelopers.length > 0) {
    warnings.push(`developer.fetch_errors: ${problematicDevelopers.join(' | ')}`);
  }

  let workflowRuns = {};
  if (providedWorkflowRuns && typeof providedWorkflowRuns === 'object') {
    workflowRuns = providedWorkflowRuns;
  } else {
    try {
      const fetched = await loadWorkflowRuns({
        repo: meta.repo,
        token: env.GITHUB_TOKEN || '',
        workflowNames
      });
      workflowRuns = asObject(fetched.runsByName);
      if (fetched.warning) {
        addCheck('workflow.api_access', 'WARN', fetched.warning);
      } else {
        addCheck('workflow.api_access', 'OK', 'workflow conclusions fetched');
      }
    } catch (error) {
      addCheck('workflow.api_access', 'WARN', `Unable to fetch workflow conclusions: ${error.message}`);
    }
  }

  const workflowConclusions = {};
  for (const workflowName of workflowNames) {
    const run = asObject(workflowRuns[workflowName]);
    const checkName = `workflow.${slugifyWorkflowName(workflowName)}`;
    if (!run || Object.keys(run).length === 0) {
      workflowConclusions[workflowName] = {
        conclusion: null,
        status: null,
        completed_at: null,
        age_seconds: null,
        stale: true
      };
      addCheck(checkName, 'WARN', 'missing latest run metadata');
      continue;
    }

    const completedAt = parseDate(run.completed_at || run.created_at);
    const ageSeconds = completedAt ? toLagSeconds(now, completedAt) : null;
    const lagStatus = deriveFreshnessStatus(ageSeconds);
    const conclusion = String(run.conclusion || '').toLowerCase();
    const conclusionFailure = conclusion && conclusion !== 'success';
    const stale = ageSeconds == null || ageSeconds > THIRTY_SIX_HOURS_SECONDS;

    workflowConclusions[workflowName] = {
      conclusion: run.conclusion || null,
      status: run.status || null,
      completed_at: run.completed_at || run.created_at || null,
      age_seconds: ageSeconds,
      stale,
      run_url: run.html_url || null
    };

    if (run.html_url && isValidUri(run.html_url)) {
      artifactLinks.push({
        label: `workflow:${slugifyWorkflowName(workflowName)}`,
        url: run.html_url
      });
    }

    if (lagStatus === 'FAIL') {
      addCheck(checkName, 'FAIL', `stale workflow run (${ageSeconds}s since completion)`);
      continue;
    }
    if (lagStatus === 'WARN') {
      addCheck(checkName, 'WARN', `aging workflow run (${ageSeconds}s since completion)`);
      continue;
    }
    if (conclusionFailure) {
      addCheck(checkName, 'WARN', `latest conclusion=${run.conclusion || 'unknown'}`);
      continue;
    }
    addCheck(checkName, 'OK', `latest conclusion=${run.conclusion || 'unknown'}`);
  }

  const validateOutcome = String(env.VALIDATE_NEWS_OUTCOME || '').toLowerCase();
  if (validateOutcome) {
    if (validateOutcome === 'success') {
      addCheck('checks.validate_news', 'OK', 'npm run validate:news succeeded');
    } else {
      addCheck('checks.validate_news', 'WARN', `npm run validate:news outcome=${validateOutcome}`);
    }
  }

  const smokeOutcome = String(env.SMOKE_OUTCOME || '').toLowerCase();
  if (smokeOutcome) {
    if (smokeOutcome === 'success') {
      addCheck('checks.smoke', 'OK', 'npm run smoke succeeded');
    } else {
      addCheck('checks.smoke', 'WARN', `npm run smoke outcome=${smokeOutcome}`);
    }
  }

  const newsUpdatedAt = parseDate(metaJson.last_updated_sgt);
  const macroUpdatedAt = parseDate(macroIndicators.last_updated_utc);
  const ratiosUpdatedAt = parseDate(ratiosJson.updatedAt);
  const freshnessBySource = {
    news: newsUpdatedAt ? { date: toIsoUtc(newsUpdatedAt), lag_seconds: toLagSeconds(now, newsUpdatedAt) } : null,
    macro: macroUpdatedAt ? { date: toIsoUtc(macroUpdatedAt), lag_seconds: toLagSeconds(now, macroUpdatedAt) } : null,
    ratios: ratiosUpdatedAt ? { date: toIsoUtc(ratiosUpdatedAt), lag_seconds: toLagSeconds(now, ratiosUpdatedAt) } : null
  };

  const freshnessSourceEntries = Object.entries(freshnessBySource);
  for (const [sourceName, sourceValue] of freshnessSourceEntries) {
    const checkName = `freshness.${sourceName}`;
    if (!sourceValue) {
      addCheck(checkName, 'FAIL', `${sourceName} timestamp missing or invalid`);
      continue;
    }
    const sourceStatus = deriveFreshnessStatus(sourceValue.lag_seconds);
    addCheck(checkName, sourceStatus, `${sourceName} lag=${sourceValue.lag_seconds}s`, sourceValue.lag_seconds);
  }

  const validFreshnessDates = [newsUpdatedAt, macroUpdatedAt, ratiosUpdatedAt].filter(Boolean);
  const maxDate = validFreshnessDates.length
    ? new Date(Math.max(...validFreshnessDates.map((date) => date.getTime())))
    : null;
  const freshnessLagSeconds = maxDate ? toLagSeconds(now, maxDate) : null;
  const freshnessStatus = deriveFreshnessStatus(freshnessLagSeconds);
  addCheck(
    'freshness.overall',
    freshnessStatus,
    freshnessLagSeconds == null ? 'no valid freshness timestamps' : `lag=${freshnessLagSeconds}s`,
    freshnessLagSeconds
  );

  const newsItemSignature = buildObjectKeySignature([...newsAllItems, ...newsLatestItems]);
  const newsFeedSignature = buildObjectKeySignature(feeds);
  const macroUpdateRunSignature = buildObjectKeySignature([macroUpdateRun]);
  const macroSeriesSignature = buildObjectKeySignature(macroSeriesEntries);
  const developerSignature = buildObjectKeySignature(developers);
  const schemaSignaturePayload = {
    news_item_objects: newsItemSignature,
    news_feed_objects: newsFeedSignature,
    macro_update_run: macroUpdateRunSignature,
    macro_series_entry: macroSeriesSignature,
    developer_record: developerSignature,
    expected_critical_fields: {
      news_required_fields: NEWS_REQUIRED_FIELDS,
      mas_required_fields: MAS_REQUIRED_FIELDS
    }
  };
  const schemaHash = sha256(stableStringify(schemaSignaturePayload));

  const schemaSignatures = {
    news_item_keys: toHumanReadableSignature(newsItemSignature),
    news_feed_keys: toHumanReadableSignature(newsFeedSignature),
    macro_update_run_keys: toHumanReadableSignature(macroUpdateRunSignature),
    macro_series_keys: toHumanReadableSignature(macroSeriesSignature),
    developer_record_keys: toHumanReadableSignature(developerSignature),
    expected_news_required_fields: NEWS_REQUIRED_FIELDS.join(','),
    expected_mas_required_fields: MAS_REQUIRED_FIELDS.join(',')
  };

  if (diagnosticsJson && Object.keys(diagnosticsJson).length > 0) {
    addCheck(
      'developer.diagnostics',
      'OK',
      `diagnostics available: totalDevelopers=${toInt(diagnosticsJson.totalDevelopers, developers.length)}`,
      toInt(diagnosticsJson.totalDevelopers, developers.length)
    );
  }

  const dedupedWarnings = [...new Set(warnings)];
  const dedupedArtifacts = dedupeArtifacts(artifactLinks);

  return {
    status,
    freshness: {
      max_date: toIsoUtc(maxDate),
      lag_seconds: freshnessLagSeconds
    },
    row_counts: rowCounts,
    schema_hash: schemaHash,
    key_checks: keyChecks,
    warnings: dedupedWarnings,
    artifact_links: dedupedArtifacts,
    meta: {
      ...meta,
      generated_at_utc: toIsoUtc(now),
      workflow_conclusions: workflowConclusions,
      schema_signatures: schemaSignatures,
      freshness_sources: freshnessBySource
    }
  };
}

function writeProbeInputFiles(rootDir, payload, tmpDir = DEFAULT_TMP_DIR) {
  const outputDir = path.join(rootDir, tmpDir);
  fs.mkdirSync(outputDir, { recursive: true });
  const files = {
    probe_inputs: path.join(outputDir, 'probe_inputs.json'),
    key_checks: path.join(outputDir, 'key_checks.json'),
    warnings: path.join(outputDir, 'warnings.json'),
    artifact_links: path.join(outputDir, 'artifact_links.json'),
    row_counts: path.join(outputDir, 'row_counts.json'),
    freshness: path.join(outputDir, 'freshness.json'),
    meta: path.join(outputDir, 'meta.json')
  };

  fs.writeFileSync(files.probe_inputs, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.key_checks, `${JSON.stringify(payload.key_checks || [], null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.warnings, `${JSON.stringify(payload.warnings || [], null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.artifact_links, `${JSON.stringify(payload.artifact_links || [], null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.row_counts, `${JSON.stringify(payload.row_counts || {}, null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.freshness, `${JSON.stringify(payload.freshness || {}, null, 2)}\n`, 'utf8');
  fs.writeFileSync(files.meta, `${JSON.stringify(payload.meta || {}, null, 2)}\n`, 'utf8');
  return files;
}

async function run() {
  const now = new Date();
  const rootDir = process.cwd();
  const meta = getCurrentRunMeta(process.env);
  const defaultArtifacts = meta.run_url ? [{ label: 'workflow_run', url: meta.run_url }] : [];

  let previousProbe = null;
  const previousProbePath = path.join(rootDir, 'ops', 'probe.json');
  if (fs.existsSync(previousProbePath)) {
    try {
      previousProbe = JSON.parse(fs.readFileSync(previousProbePath, 'utf8'));
    } catch {
      previousProbe = null;
    }
  }

  try {
    const payload = await buildProbeInputs({ rootDir, now, previousProbe });
    const files = writeProbeInputFiles(rootDir, payload);
    console.log(
      `Wrote probe inputs to ${path.relative(rootDir, files.probe_inputs)} (status=${payload.status})`
    );
    return 0;
  } catch (error) {
    const fallback = buildFailureFallback({
      now,
      error,
      meta,
      artifactLinks: defaultArtifacts
    });
    writeProbeInputFiles(rootDir, fallback);
    console.error(`build_probe_inputs failed (fallback emitted): ${error.message}`);
    return 0;
  }
}

if (require.main === module) {
  run().then((code) => process.exit(code));
}

module.exports = {
  NEWS_REQUIRED_FIELDS,
  MAS_REQUIRED_FIELDS,
  UPSTREAM_WORKFLOW_NAMES,
  THIRTY_SIX_HOURS_SECONDS,
  SEVENTY_TWO_HOURS_SECONDS,
  buildProbeInputs,
  writeProbeInputFiles,
  fetchLatestWorkflowRuns
};
