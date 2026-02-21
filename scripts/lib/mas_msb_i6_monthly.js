const fs = require('fs/promises');
const path = require('path');

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

function parseAttributes(attrText) {
  const attrs = {};
  const attrRegex = /([a-zA-Z_:][\w:.-]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+))/g;
  let m;
  while ((m = attrRegex.exec(attrText || '')) !== null) {
    attrs[m[1].toLowerCase()] = decodeHtmlEntities(m[2] ?? m[3] ?? m[4] ?? '');
  }
  return attrs;
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

function extractTableById(html, tableId) {
  const tableRegex = /<table\b([^>]*)>([\s\S]*?)<\/table>/gi;
  let m;
  while ((m = tableRegex.exec(String(html || ''))) !== null) {
    const attrs = parseAttributes(m[1] || '');
    if ((attrs.id || '').trim() === tableId) return m[0];
  }
  return null;
}

function parseRowsWithMeta(sectionHtml) {
  const rows = [];
  const rowRegex = /<tr\b([^>]*)>([\s\S]*?)<\/tr>/gi;
  let rowMatch;
  while ((rowMatch = rowRegex.exec(sectionHtml || '')) !== null) {
    const rowAttrs = parseAttributes(rowMatch[1] || '');
    const cells = [];
    const cellRegex = /<t([hd])\b([^>]*)>([\s\S]*?)<\/t\1>/gi;
    let cellMatch;
    while ((cellMatch = cellRegex.exec(rowMatch[2])) !== null) {
      const attrs = parseAttributes(cellMatch[2] || '');
      cells.push({
        kind: cellMatch[1].toLowerCase(),
        attrs,
        raw: cellMatch[3],
        text: sanitizeLabel(cellMatch[3])
      });
    }
    rows.push({ attrs: rowAttrs, cells });
  }
  return rows;
}

function parseHeaderRow(tableHtml) {
  const theadMatch = String(tableHtml || '').match(/<thead\b[^>]*>([\s\S]*?)<\/thead>/i);
  if (!theadMatch) throw new Error('thead not found in transposed-table');
  const rows = parseRowsWithMeta(theadMatch[1]);
  const headerRow = rows.find((row) => row.cells.length > 1 && normalizeLabel(row.cells[0].text) === 'end of period');
  if (!headerRow) throw new Error('End of Period header row not found');

  const headers = [];
  for (let i = 1; i < headerRow.cells.length; i += 1) {
    const parsed = parseMonthHeader(headerRow.cells[i].text);
    if (parsed) headers.push(parsed);
  }
  if (!headers.length) throw new Error('No month headers parsed under End of Period');
  return headers;
}

function isCategoryHeaderRow(row) {
  if (!row || !row.cells || !row.cells.length) return false;
  const nonEmpty = row.cells.filter((cell) => cell.text);
  if (!nonEmpty.length) return false;
  const hasBoldOrHeader = row.cells.some((cell) => cell.kind === 'th' || /\b(header|section|group)\b/i.test(`${cell.attrs.class || ''} ${cell.attrs.role || ''}`));
  if (hasBoldOrHeader && nonEmpty.length <= 2) return true;
  const spansMostColumns = row.cells.some((cell) => Number(cell.attrs.colspan || '1') >= Math.max(2, row.cells.length - 1));
  if (spansMostColumns && nonEmpty.length === 1) return true;
  return nonEmpty.length === 1 && !/^limits granted \(s\$m\)$|^utilised \(%\)$/i.test(nonEmpty[0].text);
}

function findSectionRows(tbodyRows) {
  const labels = tbodyRows.map((row) => row.cells.map((cell) => cell.text).filter(Boolean).join(' | '));
  const anchorIndex = tbodyRows.findIndex((row) => row.cells.some((cell) => cell.text === 'Building and Construction'));
  if (anchorIndex === -1) {
    const err = new Error('Building and Construction anchor row not found');
    err.debugLabels = labels.slice(0, 40);
    throw err;
  }

  const block = [];
  for (let i = anchorIndex + 1; i < tbodyRows.length; i += 1) {
    const row = tbodyRows[i];
    if (isCategoryHeaderRow(row)) break;
    block.push(row);
  }

  const byFirstCell = new Map();
  for (const row of block) {
    if (!row.cells.length) continue;
    const first = row.cells[0].text;
    if (!first) continue;
    byFirstCell.set(first, row);
  }

  const grantedRow = byFirstCell.get('Limits Granted (S$M)');
  const utilisedRow = byFirstCell.get('Utilised (%)');
  if (!grantedRow || !utilisedRow) {
    const err = new Error('Target rows not found within Building and Construction block');
    err.debugLabels = labels.slice(0, 40);
    throw err;
  }

  return { anchorIndex, labels, grantedRow, utilisedRow };
}

function rowToSeries(row, headers, rowName) {
  const values = [];
  for (let i = 0; i < headers.length; i += 1) {
    const cell = row.cells[i + 1];
    const parsed = parseNumericCell(cell?.text);
    if (parsed == null) continue;
    values.push({ period: headers[i].period, prelim: headers[i].prelim, value: parsed });
  }
  if (!values.length) throw new Error(`No numeric values found for ${rowName}`);
  return values;
}

function extractFromTableHtml(tableHtml) {
  const headers = parseHeaderRow(tableHtml);
  const tbodyMatch = String(tableHtml || '').match(/<tbody\b[^>]*>([\s\S]*?)<\/tbody>/i);
  if (!tbodyMatch) throw new Error('tbody not found in transposed-table');
  const tbodyRows = parseRowsWithMeta(tbodyMatch[1]);
  const section = findSectionRows(tbodyRows);

  const grantedValues = rowToSeries(section.grantedRow, headers, 'Limits Granted (S$M)');
  const utilisedValues = rowToSeries(section.utilisedRow, headers, 'Utilised (%)');
  return {
    source: 'dom-table',
    monthHeaders: headers,
    grantedValues,
    utilisedValues,
    debug: {
      tableFound: true,
      anchorFound: true,
      targetRowsFound: true,
      rowLabels: section.labels
    }
  };
}

function extractJsonFromScripts(html) {
  const scripts = [...String(html || '').matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]);
  const required = ['End of Period', 'Building and Construction', 'Limits Granted (S$M)', 'Utilised (%)'];
  const candidateScript = scripts.find((s) => required.every((token) => s.includes(token)));
  if (!candidateScript) return null;

  const candidates = [];
  const starts = [];
  for (let i = 0; i < candidateScript.length; i += 1) {
    const ch = candidateScript[i];
    if (ch === '{' || ch === '[') starts.push(i);
  }

  function parseBalanced(start) {
    let depth = 0;
    let inString = false;
    let quote = '';
    let escaped = false;
    for (let i = start; i < candidateScript.length; i += 1) {
      const ch = candidateScript[i];
      if (inString) {
        if (escaped) escaped = false;
        else if (ch === '\\') escaped = true;
        else if (ch === quote) inString = false;
        continue;
      }
      if (ch === '"' || ch === "'") {
        inString = true;
        quote = ch;
        continue;
      }
      if (ch === '{' || ch === '[') depth += 1;
      if (ch === '}' || ch === ']') {
        depth -= 1;
        if (depth === 0) return candidateScript.slice(start, i + 1);
      }
    }
    return null;
  }

  for (const start of starts) {
    const snippet = parseBalanced(start);
    if (!snippet) continue;
    try {
      const parsed = JSON.parse(snippet);
      candidates.push(parsed);
    } catch (_) {
      continue;
    }
  }

  const seen = new Set();
  const queue = [...candidates];
  while (queue.length) {
    const node = queue.shift();
    if (!node || typeof node !== 'object') continue;
    if (seen.has(node)) continue;
    seen.add(node);

    if (Array.isArray(node) && node.length && node.every((x) => typeof x === 'object' && x)) {
      const maybeRows = node.map((x) => Object.values(x).join(' ')).join(' ');
      if (required.every((token) => maybeRows.includes(token))) return node;
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') queue.push(value);
    }
  }

  return null;
}

function extractFromEmbeddedJson(html) {
  const rows = extractJsonFromScripts(html);
  if (!rows) return null;
  const normalizedRows = rows
    .map((row) => {
      const values = Object.values(row).map((v) => sanitizeLabel(v));
      return values.filter(Boolean);
    })
    .filter((arr) => arr.length);

  const header = normalizedRows.find((arr) => normalizeLabel(arr[0]) === 'end of period');
  if (!header) return null;
  const monthHeaders = header.slice(1).map(parseMonthHeader).filter(Boolean);
  if (!monthHeaders.length) return null;

  const anchorIdx = normalizedRows.findIndex((arr) => arr[0] === 'Building and Construction');
  if (anchorIdx === -1) return null;
  let granted = null;
  let utilised = null;
  for (let i = anchorIdx + 1; i < normalizedRows.length; i += 1) {
    const label = normalizedRows[i][0];
    if (label === 'Limits Granted (S$M)') granted = normalizedRows[i];
    if (label === 'Utilised (%)') utilised = normalizedRows[i];
    if (granted && utilised) break;
  }
  if (!granted || !utilised) return null;

  const toSeries = (arr) => monthHeaders
    .map((h, idx) => {
      const value = parseNumericCell(arr[idx + 1]);
      return value == null ? null : { period: h.period, prelim: h.prelim, value };
    })
    .filter(Boolean);

  return {
    source: 'embedded-json',
    monthHeaders,
    grantedValues: toSeries(granted),
    utilisedValues: toSeries(utilised),
    debug: {
      tableFound: false,
      anchorFound: true,
      targetRowsFound: true,
      rowLabels: normalizedRows.map((arr) => arr[0]).slice(0, 40)
    }
  };
}

async function maybeWriteArtifact(html, verifyMode) {
  if (!verifyMode) return;
  const artifactPath = path.join(process.cwd(), 'artifacts', 'mas_i6_page.html');
  await fs.mkdir(path.dirname(artifactPath), { recursive: true });
  await fs.writeFile(artifactPath, html, 'utf8');
  console.log(`[verify-mas-msb-i6] wrote debug HTML: ${artifactPath}`);
}

function extractMsbI6MonthlyFromHtml(html, { verifyMode = false } = {}) {
  const body = String(html || '');
  if (body.includes(MAINTENANCE_TEXT)) {
    throw new MasMsbI6TemporaryError(`MAS MSB I.6 page temporary failure: ${MAINTENANCE_TEXT}`);
  }

  const tableHtml = extractTableById(body, 'transposed-table');
  if (tableHtml) {
    return extractFromTableHtml(tableHtml);
  }

  const fallback = extractFromEmbeddedJson(body);
  if (fallback) return fallback;

  if (verifyMode) {
    maybeWriteArtifact(body, true).catch(() => {});
  }
  const err = new Error('transposed-table not found');
  err.debugLabels = [];
  throw err;
}

async function fetchMasMsbI6Monthly({ verifyMode = false } = {}) {
  const headers = {
    accept: 'text/html,*/*',
    'user-agent': 'macro-indicator-bot/1.0'
  };

  const backoffs = [0, 500, 1200, 2500];
  let lastError;
  for (let attempt = 0; attempt < backoffs.length; attempt += 1) {
    if (backoffs[attempt] > 0) await sleep(backoffs[attempt]);
    try {
      const res = await fetch(MAS_MSB_I6_PAGE_URL, { headers });
      if ([429, 500, 502, 503, 504].includes(res.status)) {
        lastError = new Error(`MAS MSB I.6 page not reachable: HTTP ${res.status}`);
        continue;
      }
      if (!res.ok) throw new Error(`MAS MSB I.6 page not reachable: HTTP ${res.status}`);
      const html = await res.text();
      const parsed = extractMsbI6MonthlyFromHtml(html, { verifyMode });
      if (verifyMode) {
        const newest = parsed.monthHeaders.slice(-3).reverse().map((h) => `${h.rawLabel} -> ${h.period}, prelim=${h.prelim}`);
        console.log(`[verify-mas-msb-i6] transposed-table_found=${parsed.debug.tableFound}`);
        console.log(`[verify-mas-msb-i6] newest_headers=${newest.join(' | ')}`);
        console.log('[verify-mas-msb-i6] building_and_construction_anchor_found=true');
        console.log('[verify-mas-msb-i6] target_rows_found=Limits Granted (S$M), Utilised (%)');
      }
      return parsed;
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
  extractMsbI6MonthlyFromHtml,
  fetchMasMsbI6Monthly
};
