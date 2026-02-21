const fs = require('fs/promises');
const path = require('path');

const MAS_MSB_XML_I6_URL = 'https://eservices.mas.gov.sg/statistics/msb-xml/Report.aspx?tableID=I.6&tableSetID=I';
const MAINTENANCE_TEXT = 'Sorry, this service is currently unavailable';

class MasMsbMaintenanceError extends Error {
  constructor(phase) {
    super(`MAS MSB XML ${phase} maintenance: ${MAINTENANCE_TEXT}`);
    this.name = 'MasMsbMaintenanceError';
    this.phase = phase;
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

function normalizeText(text) {
  return stripHtmlTags(text).toLowerCase().replace(/\s+/g, ' ').trim();
}

function parseAttributes(attrText) {
  const attrs = {};
  const attrRegex = /([a-zA-Z_:][\w:.-]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+))/g;
  let m;
  while ((m = attrRegex.exec(attrText || '')) !== null) {
    const key = m[1].toLowerCase();
    attrs[key] = decodeHtmlEntities(m[2] ?? m[3] ?? m[4] ?? '');
  }
  return attrs;
}

function extractFormHtml(html) {
  const forms = [...String(html || '').matchAll(/<form\b[^>]*>[\s\S]*?<\/form>/gi)];
  if (!forms.length) throw new Error('No form found in MAS MSB XML page HTML');
  const withViewState = forms.find((m) => /__VIEWSTATE/i.test(m[0]));
  return (withViewState || forms[0])[0];
}

function extractFormAction(formHtml, fallbackUrl) {
  const openTag = formHtml.match(/<form\b([^>]*)>/i);
  if (!openTag) return fallbackUrl;
  const attrs = parseAttributes(openTag[1]);
  const action = attrs.action || '';
  if (!action) return fallbackUrl;
  return new URL(action, fallbackUrl).toString();
}

function parseHiddenFields(formHtml) {
  const hidden = {};
  const inputRegex = /<input\b([^>]*)>/gi;
  let m;
  while ((m = inputRegex.exec(formHtml)) !== null) {
    const attrs = parseAttributes(m[1]);
    if (String(attrs.type || '').toLowerCase() !== 'hidden') continue;
    if (!attrs.name) continue;
    hidden[attrs.name] = attrs.value || '';
  }
  return hidden;
}

function parseSelects(formHtml) {
  const selects = [];
  const selectRegex = /<select\b([^>]*)>([\s\S]*?)<\/select>/gi;
  let m;
  while ((m = selectRegex.exec(formHtml)) !== null) {
    const attrs = parseAttributes(m[1]);
    if (!attrs.name) continue;
    const options = [];
    const optionRegex = /<option\b([^>]*)>([\s\S]*?)<\/option>/gi;
    let om;
    while ((om = optionRegex.exec(m[2])) !== null) {
      const oattrs = parseAttributes(om[1]);
      options.push({
        value: oattrs.value || stripHtmlTags(om[2]),
        text: stripHtmlTags(om[2]),
        selected: 'selected' in oattrs
      });
    }
    selects.push({ name: attrs.name, id: attrs.id || '', index: m.index, options });
  }
  return selects;
}

function parseInputs(formHtml) {
  const inputs = [];
  const inputRegex = /<input\b([^>]*)>/gi;
  let m;
  while ((m = inputRegex.exec(formHtml)) !== null) {
    const attrs = parseAttributes(m[1]);
    if (!attrs.name && !attrs.id) continue;
    const type = String(attrs.type || 'text').toLowerCase();
    inputs.push({
      name: attrs.name || '',
      id: attrs.id || '',
      type,
      value: attrs.value || '',
      checked: 'checked' in attrs,
      index: m.index,
      raw: m[0]
    });
  }
  return inputs;
}

function contextAt(html, index, radius = 260) {
  const start = Math.max(0, index - radius);
  const end = Math.min(html.length, index + radius);
  return normalizeText(html.slice(start, end));
}

function optionValueByPredicate(select, predicate, fallback = null) {
  const found = select.options.find((opt) => predicate(normalizeText(opt.text), String(opt.value).trim(), opt));
  if (found) return found.value;
  return fallback;
}

function pickYearAndMonthControls(formHtml) {
  const selects = parseSelects(formHtml);
  const monthSelects = selects.filter((s) => {
    const normalized = s.options.map((o) => normalizeText(o.text));
    return normalized.filter((x) => /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/.test(x)).length >= 6;
  });
  const yearSelects = selects.filter((s) => s.options.filter((o) => /^\d{4}$/.test(String(o.value || o.text).trim())).length >= 4);

  if (monthSelects.length < 2 || yearSelects.length < 2) {
    throw new Error('Unable to discover start/end month/year controls');
  }

  const byStartHint = (a, b) => {
    const aCtx = contextAt(formHtml, a.index);
    const bCtx = contextAt(formHtml, b.index);
    const score = (ctx) => (/(start|from)/.test(ctx) ? 2 : 0) + (/(end|to)/.test(ctx) ? -2 : 0);
    return score(bCtx) - score(aCtx);
  };

  const sortedMonths = [...monthSelects].sort(byStartHint);
  const sortedYears = [...yearSelects].sort(byStartHint);

  return {
    startMonth: sortedMonths[0],
    endMonth: sortedMonths[1],
    startYear: sortedYears[0],
    endYear: sortedYears[1]
  };
}

function pickFrequencyControl(formHtml) {
  const selects = parseSelects(formHtml);
  const monthlySelect = selects.find((s) => s.options.some((o) => /monthly/i.test(o.text)));
  if (monthlySelect) {
    const value = optionValueByPredicate(monthlySelect, (text) => /monthly/.test(text));
    return { name: monthlySelect.name, value };
  }

  const inputs = parseInputs(formHtml).filter((i) => ['radio', 'checkbox'].includes(i.type) && i.name);
  const monthlyInput = inputs.find((input) => /monthly|\bmonth\b/i.test(`${contextAt(formHtml, input.index)} ${input.value}`));
  if (!monthlyInput) throw new Error('Unable to discover frequency control for Monthly');
  return { name: monthlyInput.name, value: monthlyInput.value || 'on' };
}

function discoverMetricCheckboxes(formHtml) {
  const checkboxes = parseInputs(formHtml).filter((i) => i.type === 'checkbox' && i.name);
  if (!checkboxes.length) throw new Error('No checkboxes found on MAS MSB XML form');

  const findAfterAnchors = (anchorTerms, metricTerms) => {
    const normalizedHtml = normalizeText(formHtml);
    let anchorPos = -1;
    for (const term of anchorTerms) {
      const idx = normalizedHtml.indexOf(term);
      if (idx !== -1 && (anchorPos === -1 || idx < anchorPos)) anchorPos = idx;
    }
    const ranked = checkboxes
      .map((box) => {
        const near = contextAt(formHtml, box.index, 500);
        const score = metricTerms.reduce((acc, term) => acc + (near.includes(term) ? 3 : 0), 0)
          + (near.includes('building and construction') ? 3 : 0)
          + (near.includes('loans to businesses') ? 1 : 0)
          + (anchorPos !== -1 && box.index >= anchorPos ? 1 : 0);
        return { box, score };
      })
      .filter((entry) => entry.score > 0)
      .sort((a, b) => b.score - a.score || a.box.index - b.box.index);

    if (!ranked.length) {
      throw new Error(`Unable to discover checkbox control near anchors: ${anchorTerms.join(', ')}`);
    }

    return ranked[0].box;
  };

  const granted = findAfterAnchors(['building and construction', 'limits granted'], ['limits granted', 's$m']);
  const utilised = findAfterAnchors(['building and construction', 'utilised'], ['utilised', '%']);
  return { granted, utilised };
}

function detectSubmitTrigger(formHtml) {
  const inputs = parseInputs(formHtml);
  const submitInput = inputs.find((input) => {
    if (!input.name) return false;
    if (input.type === 'submit' || input.type === 'image' || input.type === 'button') {
      return /view|generate|submit|retrieve|apply|run/i.test(`${input.value} ${input.name} ${input.id}`);
    }
    return false;
  });

  if (submitInput) {
    return { type: 'submit', name: submitInput.name, value: submitInput.value || 'Submit' };
  }

  const doPostBackRegex = /(?:href|onclick)\s*=\s*['"][^'"]*__doPostBack\('([^']*)','([^']*)'\)/i;
  const m = formHtml.match(doPostBackRegex);
  if (m) {
    return { type: 'postback', target: m[1], argument: m[2] || '' };
  }

  throw new Error('Unable to detect View/Generate submit trigger');
}

function updateJarFromResponse(res, jar) {
  const getSetCookie = res.headers?.getSetCookie;
  const setCookies = typeof getSetCookie === 'function' ? getSetCookie.call(res.headers) : [];
  if (!setCookies || !setCookies.length) return;
  for (const line of setCookies) {
    const [pair] = String(line || '').split(';');
    const eq = pair.indexOf('=');
    if (eq <= 0) continue;
    const name = pair.slice(0, eq).trim();
    const value = pair.slice(eq + 1).trim();
    if (!name) continue;
    jar.set(name, value);
  }
}

function cookieHeaderFromJar(jar) {
  return [...jar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
}

async function fetchWithCookies(url, options = {}, jar = new Map()) {
  const headers = new Headers(options.headers || {});
  const cookie = cookieHeaderFromJar(jar);
  if (cookie) headers.set('cookie', cookie);
  const res = await fetch(url, { ...options, headers });
  updateJarFromResponse(res, jar);
  return res;
}

async function fetchWithRetry(url, options, jar, maxTries = 5) {
  let attempt = 0;
  let lastError;
  while (attempt < maxTries) {
    attempt += 1;
    try {
      const res = await fetchWithCookies(url, options, jar);
      if (res.status === 429 || res.status >= 500) {
        if (attempt >= maxTries) return res;
        await sleep(350 * (2 ** (attempt - 1)));
        continue;
      }
      return res;
    } catch (err) {
      lastError = err;
      if (attempt >= maxTries) throw err;
      await sleep(350 * (2 ** (attempt - 1)));
    }
  }
  throw lastError || new Error('request retries exhausted');
}

const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };

function parseMonthHeader(rawLabel) {
  const cleaned = stripHtmlTags(rawLabel).replace(/\s+/g, ' ').trim();
  const m = cleaned.match(/^([A-Za-z]{3,})\s+(\d{4})\s*(?:\(\s*p\s*\))?\s*$/i);
  if (!m) return null;
  const month = MONTHS[m[1].slice(0, 3).toLowerCase()];
  if (!month) return null;
  return {
    rawLabel: cleaned,
    period: `${m[2]}-${String(month).padStart(2, '0')}`,
    prelim: /\(\s*p\s*\)/i.test(cleaned)
  };
}

function parseNumericCell(value) {
  const num = Number(String(value || '').replace(/,/g, '').trim());
  return Number.isFinite(num) ? num : null;
}

function extractTableRows(tableHtml) {
  const rows = [];
  const rowRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch;
  while ((rowMatch = rowRegex.exec(tableHtml)) !== null) {
    const cells = [];
    const cellRegex = /<t([hd])\b[^>]*>([\s\S]*?)<\/t\1>/gi;
    let cellMatch;
    while ((cellMatch = cellRegex.exec(rowMatch[1])) !== null) {
      cells.push({ kind: cellMatch[1].toLowerCase(), text: stripHtmlTags(cellMatch[2]) });
    }
    if (cells.length) rows.push(cells);
  }
  return rows;
}

function parseDataTable(html) {
  const tables = [...String(html || '').matchAll(/<table\b[^>]*>[\s\S]*?<\/table>/gi)].map((m) => m[0]);
  for (const table of tables) {
    if (!/building and construction/i.test(table) || !/limits granted/i.test(table)) continue;
    const rows = extractTableRows(table);
    if (!rows.length) continue;

    const buildingIndex = rows.findIndex((cells) => cells.some((cell) => normalizeText(cell.text) === 'building and construction'));
    if (buildingIndex === -1) continue;

    let monthHeaders = null;
    let monthIndexes = null;
    for (const row of rows) {
      const parsed = row
        .map((cell, idx) => ({ idx, parsed: parseMonthHeader(cell.text) }))
        .filter((x) => x.parsed);
      if (parsed.length > 0) {
        monthHeaders = parsed.map((x) => x.parsed);
        monthIndexes = parsed.map((x) => x.idx);
        break;
      }
    }
    if (!monthHeaders || !monthHeaders.length) continue;

    const wanted = {
      'limits granted (s$m)': null,
      'utilised (%)': null
    };

    for (let i = buildingIndex + 1; i < rows.length; i += 1) {
      const labelCell = rows[i].find((c) => normalizeText(c.text));
      if (!labelCell) continue;
      const label = normalizeText(labelCell.text);
      if (label in wanted) wanted[label] = rows[i];
      if (wanted['limits granted (s$m)'] && wanted['utilised (%)']) break;
    }

    if (!wanted['limits granted (s$m)'] || !wanted['utilised (%)']) continue;

    const toValues = (row, rowName) => {
      const values = monthIndexes
        .map((idx, i) => {
          const parsed = parseNumericCell(row[idx]?.text);
          if (parsed == null) return null;
          return { period: monthHeaders[i].period, prelim: monthHeaders[i].prelim, value: parsed };
        })
        .filter(Boolean);
      if (!values.length) throw new Error(`No numeric values parsed for ${rowName}`);
      if (values.length !== monthHeaders.length) {
        throw new Error(`Cell count mismatch for ${rowName}: parsed=${values.length}, headers=${monthHeaders.length}`);
      }
      return values;
    };

    return {
      monthHeaders,
      grantedValues: toValues(wanted['limits granted (s$m)'], 'Limits Granted (S$M)'),
      utilisedValues: toValues(wanted['utilised (%)'], 'Utilised (%)')
    };
  }
  throw new Error('no data table found');
}

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        cur += ch;
      }
    } else if (ch === ',') {
      out.push(cur);
      cur = '';
    } else if (ch === '"') {
      inQuotes = true;
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function parseCsv(content) {
  return String(content || '')
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.trim().length > 0)
    .map(parseCsvLine);
}

function parseFromCsv(csvText) {
  const rows = parseCsv(csvText);
  if (!rows.length) throw new Error('CSV export empty');
  const headerRow = rows.find((r) => r.some((cell) => parseMonthHeader(cell)));
  if (!headerRow) throw new Error('CSV export missing month header row');

  const monthCols = headerRow
    .map((cell, idx) => ({ idx, header: parseMonthHeader(cell) }))
    .filter((x) => x.header);
  if (!monthCols.length) throw new Error('CSV export month headers not found');

  const findRow = (target) => rows.find((r) => normalizeText(r[0] || '').includes(target));
  const grantedRow = findRow('limits granted (s$m)');
  const utilisedRow = findRow('utilised (%)');
  if (!grantedRow || !utilisedRow) throw new Error('CSV export missing target rows');

  const toValues = (row, rowName) => {
    const parsed = monthCols
      .map(({ idx, header }) => {
        const value = parseNumericCell(row[idx]);
        if (value == null) return null;
        return { period: header.period, prelim: header.prelim, value };
      })
      .filter(Boolean);
    if (!parsed.length) throw new Error(`CSV row has no numeric values: ${rowName}`);
    if (parsed.length !== monthCols.length) throw new Error(`CSV cell/header mismatch: ${rowName}`);
    return parsed;
  };

  return {
    monthHeaders: monthCols.map((x) => x.header),
    grantedValues: toValues(grantedRow, 'Limits Granted (S$M)'),
    utilisedValues: toValues(utilisedRow, 'Utilised (%)')
  };
}

function discoverExportCandidate(postHtml, postUrl) {
  const links = [...String(postHtml || '').matchAll(/<a\b([^>]*)>([\s\S]*?)<\/a>/gi)].map((m) => ({ attrs: parseAttributes(m[1]), text: stripHtmlTags(m[2]) }));
  const link = links.find((l) => /download|export|excel|csv/i.test(`${l.text} ${l.attrs.href || ''}`));
  if (link && link.attrs.href && !/^javascript:/i.test(link.attrs.href)) {
    return { type: 'url', url: new URL(link.attrs.href, postUrl).toString(), text: link.text || link.attrs.href };
  }

  const postbackFromLink = links.find((l) => /download|export|excel|csv/i.test(l.text) && /__doPostBack\(/i.test(l.attrs.href || ''));
  if (postbackFromLink) {
    const m = (postbackFromLink.attrs.href || '').match(/__doPostBack\('([^']*)','([^']*)'\)/i);
    if (m) return { type: 'postback', target: m[1], argument: m[2] || '', text: postbackFromLink.text };
  }

  const inputs = parseInputs(postHtml);
  const exportInput = inputs.find((input) => /download|export|excel|csv/i.test(`${input.name} ${input.id} ${input.value}`));
  if (exportInput && exportInput.name) {
    return { type: 'submit', name: exportInput.name, value: exportInput.value || 'Export' };
  }

  return null;
}

function assertNotMaintenance(phase, html) {
  if (String(html || '').includes(MAINTENANCE_TEXT)) {
    throw new MasMsbMaintenanceError(phase);
  }
}

async function writeDebugArtifact(postHtml) {
  const artifactPath = path.join(process.cwd(), 'artifacts', 'mas_i6_last_response.html');
  await fs.mkdir(path.dirname(artifactPath), { recursive: true });
  await fs.writeFile(artifactPath, postHtml, 'utf8');
  return artifactPath;
}

function titleSnippet(html) {
  const title = String(html || '').match(/<title\b[^>]*>([\s\S]*?)<\/title>/i)?.[1] || '';
  return stripHtmlTags(title).slice(0, 200);
}

function buildSubmission(formHtml, verifyMode = false) {
  const hiddenFields = parseHiddenFields(formHtml);
  const formAction = extractFormAction(formHtml, MAS_MSB_XML_I6_URL);
  const { startMonth, endMonth, startYear, endYear } = pickYearAndMonthControls(formHtml);
  const frequency = pickFrequencyControl(formHtml);
  const metrics = discoverMetricCheckboxes(formHtml);
  const trigger = detectSubmitTrigger(formHtml);

  const now = new Date();
  const thisYear = now.getUTCFullYear();
  const thisMonth = now.getUTCMonth() + 1;

  const startYearValue = optionValueByPredicate(startYear, (_, value) => Number(value) === 2021)
    || optionValueByPredicate(startYear, (_, value) => Number(value) >= 2021)
    || startYear.options[0]?.value;
  const endYearValue = optionValueByPredicate(endYear, (_, value) => Number(value) === thisYear)
    || optionValueByPredicate(endYear, (_, value) => Number(value) <= thisYear, endYear.options[endYear.options.length - 1]?.value);
  const startMonthValue = optionValueByPredicate(startMonth, (text, value) => /^jan/.test(text) || Number(value) === 1);
  const endMonthValue = optionValueByPredicate(endMonth, (text, value) => Number(value) === thisMonth || text.startsWith(Object.keys(MONTHS)[thisMonth - 1]));

  if (!startYearValue || !endYearValue || !startMonthValue || !endMonthValue) {
    throw new Error('Unable to map desired date range onto discovered controls');
  }

  const payload = {
    ...hiddenFields,
    [startYear.name]: startYearValue,
    [endYear.name]: endYearValue,
    [startMonth.name]: startMonthValue,
    [endMonth.name]: endMonthValue,
    [frequency.name]: frequency.value,
    [metrics.granted.name]: metrics.granted.value || 'on',
    [metrics.utilised.name]: metrics.utilised.value || 'on'
  };

  if (trigger.type === 'submit') {
    payload[trigger.name] = trigger.value;
  } else {
    payload.__EVENTTARGET = trigger.target;
    payload.__EVENTARGUMENT = trigger.argument;
  }

  if (verifyMode) {
    if (trigger.type === 'submit') {
      console.log(`[verify-mas-msb-xml-i6] trigger=submit name=${trigger.name} value=${trigger.value}`);
    } else {
      console.log(`[verify-mas-msb-xml-i6] trigger=__doPostBack target=${trigger.target} argument=${trigger.argument}`);
    }
  }

  return { formAction, payload };
}

async function tryExportFallback(postHtml, postUrl, formPayload, jar, commonHeaders, verifyMode = false) {
  const candidate = discoverExportCandidate(postHtml, postUrl);
  if (!candidate) {
    throw new Error('no data table found');
  }

  if (verifyMode) {
    const descriptor = candidate.url || candidate.text || candidate.target || candidate.name;
    console.log(`[verify-mas-msb-xml-i6] export_fallback=${candidate.type} via=${descriptor}`);
  }

  if (candidate.type === 'url') {
    const exportRes = await fetchWithRetry(candidate.url, { headers: commonHeaders }, jar);
    if (!exportRes.ok) throw new Error(`MAS MSB XML export URL failed: HTTP ${exportRes.status}`);
    const ctype = exportRes.headers.get('content-type') || '';
    if (/csv/i.test(ctype) || /text\//i.test(ctype)) {
      const csv = await exportRes.text();
      return parseFromCsv(csv);
    }
    throw new Error(`Export fallback returned non-CSV content-type: ${ctype || 'unknown'}`);
  }

  if (candidate.type === 'postback' || candidate.type === 'submit') {
    const exportPayload = {
      ...formPayload,
      ...(candidate.type === 'postback'
        ? { __EVENTTARGET: candidate.target, __EVENTARGUMENT: candidate.argument }
        : { [candidate.name]: candidate.value })
    };

    const exportRes = await fetchWithRetry(postUrl, {
      method: 'POST',
      headers: {
        ...commonHeaders,
        'content-type': 'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams(exportPayload)
    }, jar);
    if (!exportRes.ok) throw new Error(`MAS MSB XML export postback failed: HTTP ${exportRes.status}`);
    const ctype = exportRes.headers.get('content-type') || '';
    if (/csv/i.test(ctype) || /text\//i.test(ctype)) {
      const csv = await exportRes.text();
      return parseFromCsv(csv);
    }
    throw new Error(`Export fallback postback returned non-CSV content-type: ${ctype || 'unknown'}`);
  }

  throw new Error('no data table found');
}

async function fetchMasMsbXmlI6Monthly({ verifyMode = false } = {}) {
  const jar = new Map();
  const commonHeaders = { 'user-agent': 'macro-indicator-bot/1.0' };
  const getRes = await fetchWithRetry(MAS_MSB_XML_I6_URL, { headers: commonHeaders }, jar);
  if (!getRes.ok) throw new Error(`MAS MSB XML GET failed: HTTP ${getRes.status}`);
  const landingHtml = await getRes.text();
  assertNotMaintenance('GET', landingHtml);

  const formHtml = extractFormHtml(landingHtml);
  const { formAction, payload } = buildSubmission(formHtml, verifyMode);

  const postRes = await fetchWithRetry(formAction, {
    method: 'POST',
    headers: {
      ...commonHeaders,
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(payload)
  }, jar);

  if (!postRes.ok) throw new Error(`MAS MSB XML POST failed: HTTP ${postRes.status}`);
  const postHtml = await postRes.text();
  assertNotMaintenance('POST', postHtml);

  try {
    return parseDataTable(postHtml);
  } catch (err) {
    if (!/no data table found/i.test(String(err?.message || ''))) throw err;

    if (verifyMode) {
      const artifactPath = await writeDebugArtifact(postHtml);
      console.log(`[verify-mas-msb-xml-i6] post_response_length=${postHtml.length}`);
      console.log(`[verify-mas-msb-xml-i6] contains_building_and_construction=${/building and construction/i.test(postHtml)}`);
      console.log(`[verify-mas-msb-xml-i6] contains_download_or_export=${/download|export/i.test(postHtml)}`);
      console.log(`[verify-mas-msb-xml-i6] title_snippet=${titleSnippet(postHtml)}`);
      console.log(`[verify-mas-msb-xml-i6] saved_debug_artifact=${artifactPath}`);
    }

    return tryExportFallback(postHtml, formAction, payload, jar, commonHeaders, verifyMode);
  }
}

module.exports = {
  MAS_MSB_XML_I6_URL,
  MasMsbMaintenanceError,
  fetchMasMsbXmlI6Monthly,
  fetchWithCookies,
  parseMonthHeader,
  parseDataTable
};
