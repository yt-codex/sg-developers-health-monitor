const MAS_MSB_I6_PAGE_URL = 'https://www.mas.gov.sg/statistics/monthly-statistical-bulletin/i-6-commercial-banks-loan-limits-granted-to-non-bank-customers-by-industry';
const MAINTENANCE_TEXT = 'Sorry, this service is currently unavailable';

const MONTHS = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12
};

class MasMsbI6TemporaryError extends Error {
  constructor(message) {
    super(message);
    this.name = 'MasMsbI6TemporaryError';
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function decodeHtmlEntities(text) {
  return String(text || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)));
}

function stripHtmlTags(text) {
  return decodeHtmlEntities(String(text || '').replace(/<[^>]*>/g, ' ')).replace(/\s+/g, ' ').trim();
}

function sanitizeLabel(text) {
  return stripHtmlTags(text).replace(/\s+/g, ' ').trim();
}

function normalizeLabel(text) {
  return sanitizeLabel(text).toLowerCase();
}

function parseMonthHeader(rawLabel) {
  const cleaned = sanitizeLabel(rawLabel);
  const m = cleaned.match(/^([A-Za-z]{3,})\s+(\d{4})\s*(?:\((p)\))?$/i);
  if (!m) return null;
  const month = MONTHS[m[1].slice(0, 3).toLowerCase()];
  if (!month) return null;
  return {
    rawLabel: cleaned,
    period: `${m[2]}-${String(month).padStart(2, '0')}`,
    prelim: Boolean(m[3])
  };
}

function parseNumericCell(value) {
  const cleaned = sanitizeLabel(value);
  if (!cleaned || cleaned === '-' || /^na$/i.test(cleaned) || /^n\/a$/i.test(cleaned)) return null;
  const num = Number(cleaned.replace(/,/g, ''));
  return Number.isFinite(num) ? num : null;
}

function rowToSeries(values, headers, rowName) {
  const result = [];
  for (let i = 0; i < headers.length; i += 1) {
    const parsed = parseNumericCell(values[i + 1]);
    if (parsed == null) continue;
    result.push({ period: headers[i].period, prelim: headers[i].prelim, value: parsed });
  }
  if (!result.length) throw new Error(`No numeric values found for ${rowName}`);
  return result;
}

function extractSeriesFromMatrix(matrix) {
  const rows = matrix
    .map((row) => (Array.isArray(row) ? row.map((cell) => sanitizeLabel(cell)) : null))
    .filter((row) => row && row.some(Boolean));

  const headerRow = rows.find((row) => normalizeLabel(row[0]) === 'end of period');
  if (!headerRow) return null;
  const headers = headerRow.slice(1).map(parseMonthHeader).filter(Boolean);
  if (!headers.length) return null;

  const anchorIndex = rows.findIndex((row) => row[0] === 'Building and Construction');
  if (anchorIndex === -1) return null;

  const byLabel = new Map();
  for (let i = anchorIndex + 1; i < rows.length; i += 1) {
    const label = rows[i][0];
    if (!label) continue;
    if (/^total loans/i.test(label)) continue;
    if (label === 'Limits Granted (S$M)' || label === 'Utilised (%)') {
      byLabel.set(label, rows[i]);
    }
    if (byLabel.size === 2) break;
  }

  const granted = byLabel.get('Limits Granted (S$M)');
  const utilised = byLabel.get('Utilised (%)');
  if (!granted || !utilised) return null;

  return {
    monthHeaders: headers,
    grantedValues: rowToSeries(granted, headers, 'Limits Granted (S$M)'),
    utilisedValues: rowToSeries(utilised, headers, 'Utilised (%)')
  };
}

function collectStrings(node, acc = new Set(), depth = 0) {
  if (depth > 8 || node == null) return acc;
  if (typeof node === 'string') {
    acc.add(node);
    return acc;
  }
  if (Array.isArray(node)) {
    for (const item of node) collectStrings(item, acc, depth + 1);
    return acc;
  }
  if (typeof node === 'object') {
    for (const value of Object.values(node)) collectStrings(value, acc, depth + 1);
  }
  return acc;
}

function extractJsonBlobsFromHtml(html) {
  const blobs = [];
  const scriptRegex = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = scriptRegex.exec(String(html || ''))) !== null) {
    const attrs = m[1] || '';
    const body = (m[2] || '').trim();
    if (/\bsrc\s*=/.test(attrs) || !body) continue;
    if (!/__NEXT_DATA__|__INITIAL_STATE__|runtimeConfig|End of Period|Building and Construction/i.test(body)) continue;

    const trimmed = body.startsWith('window.__INITIAL_STATE__')
      ? body.replace(/^window\.__INITIAL_STATE__\s*=\s*/, '').replace(/;\s*$/, '')
      : body;

    if (/^\{[\s\S]*\}$|^\[[\s\S]*\]$/.test(trimmed)) {
      try {
        blobs.push(JSON.parse(trimmed));
      } catch (_) {
        // ignore
      }
    }
  }

  const nextData = String(html || '').match(/<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i);
  if (nextData) {
    try {
      blobs.push(JSON.parse(nextData[1]));
    } catch (_) {
      // ignore
    }
  }

  return blobs;
}

function normalizeCandidateUrl(value, baseUrl) {
  const text = String(value || '').trim().replace(/\\\//g, '/');
  if (!text) return null;
  if (!/api|msb|bulletin|statistics/i.test(text)) return null;

  const cleaned = text.replace(/^['"]|['"]$/g, '');
  try {
    return new URL(cleaned, baseUrl).toString();
  } catch (_) {
    return null;
  }
}

function extractScriptSrcUrls(html, baseUrl) {
  const urls = new Set();
  for (const m of String(html || '').matchAll(/<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["'][^>]*>/gi)) {
    try {
      urls.add(new URL(m[1], baseUrl).toString());
    } catch (_) {
      // ignore
    }
  }
  return [...urls];
}

function extractCandidatesFromTextBlob(text, baseUrl) {
  const candidates = new Set();
  const regex = /(https?:\/\/[^"'\s)]+|\/[a-z0-9._~!$&'()*+,;=:@%\/-]*(?:api|msb|bulletin|statistics)[a-z0-9._~!$&'()*+,;=:@%\/-]*)/gi;
  for (const m of String(text || '').matchAll(regex)) {
    const candidate = normalizeCandidateUrl(m[1], baseUrl);
    if (candidate) candidates.add(candidate);
  }
  return [...candidates];
}

function matrixFromObjectArray(arr) {
  if (!Array.isArray(arr) || !arr.length || !arr.every((x) => x && typeof x === 'object' && !Array.isArray(x))) return null;
  return arr.map((obj) => Object.values(obj));
}

function findSeriesInPayload(payload) {
  const queue = [payload];
  const seen = new Set();
  while (queue.length) {
    const node = queue.shift();
    if (!node || typeof node !== 'object') continue;
    if (seen.has(node)) continue;
    seen.add(node);

    if (Array.isArray(node)) {
      const directMatrix = node.every(Array.isArray) ? node : null;
      const objectMatrix = directMatrix ? null : matrixFromObjectArray(node);
      const matrix = directMatrix || objectMatrix;
      if (matrix) {
        const extracted = extractSeriesFromMatrix(matrix);
        if (extracted) return extracted;
      }
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') queue.push(value);
    }
  }
  return null;
}

async function fetchJsonLike(url, headers) {
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const contentType = res.headers.get('content-type') || '';
  const text = await res.text();
  if (/json|javascript|text\/plain/i.test(contentType) || /^[\[{]/.test(text.trim())) {
    try {
      return JSON.parse(text);
    } catch (_) {
      return { raw: text };
    }
  }
  return { raw: text };
}

async function discoverAndFetchApiSeries(html, { headers, verifyMode = false } = {}) {
  const debug = {
    candidateCount: 0,
    triedEndpoints: [],
    foundViaApi: false,
    foundViaEmbeddedJson: false
  };

  const candidates = new Set();
  extractCandidatesFromTextBlob(html, MAS_MSB_I6_PAGE_URL).forEach((x) => candidates.add(x));

  const jsonBlobs = extractJsonBlobsFromHtml(html);
  for (const blob of jsonBlobs) {
    const embedded = findSeriesInPayload(blob);
    if (embedded) {
      debug.foundViaEmbeddedJson = true;
      return { ...embedded, source: 'embedded-json', debug };
    }
    collectStrings(blob).forEach((s) => extractCandidatesFromTextBlob(s, MAS_MSB_I6_PAGE_URL).forEach((x) => candidates.add(x)));
  }

  const scriptUrls = extractScriptSrcUrls(html, MAS_MSB_I6_PAGE_URL);
  const prioritized = scriptUrls.filter((url) => /main|runtime|chunk|bundle/i.test(url));
  const bundleTargets = prioritized.length ? prioritized.slice(0, 3) : scriptUrls.slice(0, 2);
  for (const scriptUrl of bundleTargets) {
    try {
      const scriptText = await (await fetch(scriptUrl, { headers })).text();
      extractCandidatesFromTextBlob(scriptText, MAS_MSB_I6_PAGE_URL).forEach((x) => candidates.add(x));
    } catch (_) {
      // best effort only
    }
  }

  debug.candidateCount = candidates.size;
  for (const endpoint of candidates) {
    debug.triedEndpoints.push(endpoint);
    try {
      const payload = await fetchJsonLike(endpoint, { ...headers, accept: 'application/json,text/plain,*/*' });
      const extracted = findSeriesInPayload(payload);
      if (extracted) {
        debug.foundViaApi = true;
        return { ...extracted, source: `api:${endpoint}`, debug };
      }
    } catch (_) {
      // continue trying next candidate
    }
  }

  if (verifyMode) {
    console.log(`[verify-mas-msb-i6] api_candidate_count=${debug.candidateCount}`);
    console.log(`[verify-mas-msb-i6] api_candidates_tried=${debug.triedEndpoints.join(', ') || 'none'}`);
  }

  return { result: null, debug };
}

async function scrapeWithPlaywright({ headers, verifyMode = false } = {}) {
  let playwright;
  try {
    playwright = require('playwright');
  } catch (err) {
    throw new Error(`Playwright unavailable for fallback: ${err.message}`);
  }

  const browser = await playwright.chromium.launch({ headless: true });
  const context = await browser.newContext({ extraHTTPHeaders: headers });
  const page = await context.newPage();
  let appeared = false;

  try {
    await page.goto(MAS_MSB_I6_PAGE_URL, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    await page.waitForSelector('#transposed-table', { timeout: 30_000 });
    appeared = true;

    const tableData = await page.evaluate(() => {
      const table = document.querySelector('#transposed-table');
      if (!table) return null;
      const readText = (el) => (el?.textContent || '').replace(/\s+/g, ' ').trim();
      const headerRow = [...table.querySelectorAll('thead tr')]
        .map((tr) => [...tr.querySelectorAll('th,td')].map(readText))
        .find((row) => row[0] === 'End of Period');
      if (!headerRow) return null;

      const bodyRows = [...table.querySelectorAll('tbody tr')].map((tr) => [...tr.querySelectorAll('th,td')].map(readText));
      const anchor = bodyRows.findIndex((row) => row[0] === 'Building and Construction');
      if (anchor === -1) return null;

      let granted;
      let utilised;
      for (let i = anchor + 1; i < bodyRows.length; i += 1) {
        const label = bodyRows[i][0];
        if (label === 'Limits Granted (S$M)') granted = bodyRows[i];
        if (label === 'Utilised (%)') utilised = bodyRows[i];
        if (granted && utilised) break;
      }

      if (!granted || !utilised) return null;
      return { headerRow, granted, utilised };
    });

    if (!tableData) throw new Error('Failed to parse #transposed-table in Playwright fallback');

    const headersParsed = tableData.headerRow.slice(1).map(parseMonthHeader).filter(Boolean);
    if (!headersParsed.length) throw new Error('No month headers parsed in Playwright fallback');

    return {
      source: 'playwright-dom',
      monthHeaders: headersParsed,
      grantedValues: rowToSeries(tableData.granted, headersParsed, 'Limits Granted (S$M)'),
      utilisedValues: rowToSeries(tableData.utilised, headersParsed, 'Utilised (%)'),
      debug: {
        playwrightTableAppeared: appeared
      }
    };
  } finally {
    if (verifyMode) {
      console.log(`[verify-mas-msb-i6] playwright_table_appeared=${appeared}`);
    }
    await context.close();
    await browser.close();
  }
}

async function fetchMasMsbI6Monthly({ verifyMode = false } = {}) {
  const headers = {
    accept: 'text/html,*/*',
    'user-agent': 'macro-indicator-bot/1.0'
  };

  const backoffs = [0, 700, 1400];
  let lastError;

  for (let attempt = 0; attempt < backoffs.length; attempt += 1) {
    if (backoffs[attempt] > 0) await sleep(backoffs[attempt]);

    try {
      const res = await fetch(MAS_MSB_I6_PAGE_URL, { headers });
      if (!res.ok) throw new Error(`MAS MSB I.6 page not reachable: HTTP ${res.status}`);
      const html = await res.text();
      if (html.includes(MAINTENANCE_TEXT)) {
        throw new MasMsbI6TemporaryError(`MAS MSB I.6 page temporary failure: ${MAINTENANCE_TEXT}`);
      }

      const stage1 = await discoverAndFetchApiSeries(html, { headers, verifyMode });
      if (stage1?.monthHeaders?.length) {
        if (verifyMode) {
          console.log(`[verify-mas-msb-i6] extraction_source=${stage1.source}`);
          console.log(`[verify-mas-msb-i6] api_candidate_count=${stage1.debug.candidateCount}`);
          console.log(`[verify-mas-msb-i6] api_candidates_tried=${stage1.debug.triedEndpoints.join(', ') || 'none'}`);
        }
        return stage1;
      }

      if (verifyMode) {
        console.log(`[verify-mas-msb-i6] api_candidate_count=${stage1.debug.candidateCount}`);
        console.log(`[verify-mas-msb-i6] api_candidates_tried=${stage1.debug.triedEndpoints.join(', ') || 'none'}`);
        console.log('[verify-mas-msb-i6] stage1_failed=true');
      }

      try {
        const stage2 = await scrapeWithPlaywright({ headers, verifyMode });
        if (verifyMode) {
          console.log(`[verify-mas-msb-i6] extraction_source=${stage2.source}`);
        }
        return stage2;
      } catch (fallbackErr) {
        const err = new Error(`MAS MSB I.6 extraction failed after API+Playwright attempts: ${fallbackErr.message}`);
        err.cause = fallbackErr;
        throw err;
      }
    } catch (err) {
      if (err instanceof MasMsbI6TemporaryError) throw err;
      lastError = err;
    }
  }

  throw lastError || new Error('Failed to fetch MAS MSB I.6 page');
}

module.exports = {
  MAS_MSB_I6_PAGE_URL,
  MasMsbI6TemporaryError,
  fetchMasMsbI6Monthly
};
