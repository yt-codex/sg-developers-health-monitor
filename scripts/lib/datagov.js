const CKAN_BASE = 'https://data.gov.sg/api/action/datastore_search';
const DEFAULT_HEADERS = {
  accept: 'application/json',
  'user-agent': 'macro-indicator-bot/1.0'
};
const MAX_HTTP_RETRIES = 5;
const DATAGOV_REQUEST_INTERVAL_MS = 1200;

let dataGovQueue = Promise.resolve();
let lastDataGovRequestAt = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterSeconds(retryAfterValue) {
  if (!retryAfterValue) return null;
  const asNumber = Number(retryAfterValue);
  if (Number.isFinite(asNumber)) return Math.max(0, asNumber);

  const asDate = new Date(retryAfterValue);
  if (Number.isNaN(asDate.getTime())) return null;
  return Math.max(0, Math.ceil((asDate.getTime() - Date.now()) / 1000));
}

function buildDataGovHeaders({ apiKey, allowUnauthenticated = false, extraHeaders = {} }) {
  if (!apiKey && !allowUnauthenticated) {
    throw new Error('DATA_GOV_SG_API_KEY is required for data.gov.sg requests');
  }

  return {
    ...DEFAULT_HEADERS,
    ...(apiKey ? { 'X-API-KEY': apiKey } : {}),
    ...extraHeaders
  };
}

async function fetchWithRetry(url, options = {}, { label = url, retries = MAX_HTTP_RETRIES } = {}) {
  let lastError = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const res = await fetch(url, options);
      if (res.ok) return res;

      const shouldRetry = res.status === 429 || res.status >= 500;
      if (!shouldRetry || attempt === retries) {
        throw new Error(`HTTP ${res.status} for ${label}`);
      }

      const retryAfterSeconds = parseRetryAfterSeconds(res.headers.get('retry-after'));
      const backoffMs = retryAfterSeconds != null
        ? retryAfterSeconds * 1000
        : (res.status === 429
          ? Math.min(8000, 1000 * 2 ** (attempt - 1))
          : Math.min(30_000, 1000 * 2 ** (attempt - 1)));

      console.warn(
        `[retry] ${label}: HTTP ${res.status} (attempt ${attempt}/${retries}), waiting ${backoffMs}ms before retry`
      );
      await sleep(backoffMs);
    } catch (err) {
      lastError = err;
      if (attempt === retries) break;
      const backoffMs = Math.min(30_000, 1000 * 2 ** (attempt - 1));
      console.warn(
        `[retry] ${label}: ${err.message} (attempt ${attempt}/${retries}), waiting ${backoffMs}ms before retry`
      );
      await sleep(backoffMs);
    }
  }

  throw lastError || new Error(`Failed to fetch ${label}`);
}

async function enqueueDataGovRequest(task) {
  const run = async () => {
    const waitMs = Math.max(0, DATAGOV_REQUEST_INTERVAL_MS - (Date.now() - lastDataGovRequestAt));
    if (waitMs > 0) await sleep(waitMs);
    try {
      return await task();
    } finally {
      lastDataGovRequestAt = Date.now();
    }
  };

  const scheduled = dataGovQueue.then(run, run);
  dataGovQueue = scheduled.catch(() => undefined);
  return scheduled;
}

async function fetchCkanDatastoreSearch({
  resourceId,
  limit = 10000,
  offset = 0,
  q,
  filters,
  apiKey,
  allowUnauthenticated = false,
  label,
  verifyMode = false
}) {
  const params = new URLSearchParams({
    resource_id: resourceId,
    limit: String(limit),
    offset: String(offset)
  });

  if (q != null && q !== '') params.set('q', String(q));
  if (filters != null) params.set('filters', typeof filters === 'string' ? filters : JSON.stringify(filters));

  const url = `${CKAN_BASE}?${params.toString()}`;
  if (verifyMode) console.log(`[verify-url] ${resourceId} ${url}`);

  const res = await enqueueDataGovRequest(() => fetchWithRetry(
    url,
    {
      headers: buildDataGovHeaders({ apiKey, allowUnauthenticated })
    },
    { label: label || `${resourceId} (offset=${offset})` }
  ));

  return res.json();
}

module.exports = {
  fetchWithRetry,
  fetchCkanDatastoreSearch,
  DEFAULT_HEADERS
};
