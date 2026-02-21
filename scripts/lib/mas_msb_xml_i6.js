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
    if (!attrs.name) continue;
    const type = String(attrs.type || 'text').toLowerCase();
    inputs.push({
      name: attrs.name,
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

function contextAt(html, index, radius = 240) {
  const start = Math.max(0, index - radius);
  const end = Math.min(html.length, index + radius);
  return normalizeText(html.slice(start, end));
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

function optionValueByPredicate(select, predicate, fallback = null) {
  const found = select.options.find((opt) => predicate(normalizeText(opt.text), String(opt.value).trim(), opt));
  if (found) return found.value;
  return fallback;
}

function pickFrequencyControl(formHtml) {
  const selects = parseSelects(formHtml);
  const monthlySelect = selects.find((s) => s.options.some((o) => /monthly/i.test(o.text)));
  if (monthlySelect) {
    const value = optionValueByPredicate(monthlySelect, (text) => /monthly/.test(text));
    return { name: monthlySelect.name, value };
  }

  const inputs = parseInputs(formHtml).filter((i) => ['radio', 'checkbox'].includes(i.type));
  const monthlyInput = inputs.find((input) => /monthly|\bmonth\b/i.test(`${contextAt(formHtml, input.index)} ${input.value}`));
  if (!monthlyInput) throw new Error('Unable to discover frequency control for Monthly');
  return { name: monthlyInput.name, value: monthlyInput.value || 'on' };
}

function discoverMetricCheckboxes(formHtml) {
  const labelsByFor = new Map();
  const labelRegex = /<label\b([^>]*)>([\s\S]*?)<\/label>/gi;
  let lm;
  while ((lm = labelRegex.exec(formHtml)) !== null) {
    const attrs = parseAttributes(lm[1]);
    if (!attrs.for) continue;
    labelsByFor.set(attrs.for, stripHtmlTags(lm[2]));
  }

  const checkboxes = parseInputs(formHtml).filter((i) => i.type === 'checkbox');
  const findMetric = (metricText) => {
    const wanted = normalizeText(metricText);
    const candidates = checkboxes
      .map((box) => {
        const explicit = labelsByFor.get(box.id) || '';
        const near = contextAt(formHtml, box.index, 420);
        const score =
          (normalizeText(explicit).includes(wanted) ? 5 : 0)
          + (near.includes(wanted) ? 4 : 0)
          + (near.includes('building and construction') ? 2 : 0)
          + (near.includes('loans to businesses') ? 1 : 0);
        return { box, score };
      })
      .filter((c) => c.score > 0)
      .sort((a, b) => b.score - a.score);

    if (!candidates.length) throw new Error(`Unable to discover checkbox control for ${metricText}`);
    return candidates[0].box;
  };

  return {
    granted: findMetric('limits granted (s$m)'),
    utilised: findMetric('utilised (%)')
  };
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

    const toValues = (row) => monthIndexes
      .map((idx, i) => {
        const parsed = parseNumericCell(row[idx]?.text);
        if (parsed == null) return null;
        return { period: monthHeaders[i].period, prelim: monthHeaders[i].prelim, value: parsed };
      })
      .filter(Boolean);

    return {
      monthHeaders,
      grantedValues: toValues(wanted['limits granted (s$m)']),
      utilisedValues: toValues(wanted['utilised (%)'])
    };
  }
  throw new Error('no data table found');
}

async function fetchWithRetry(url, options, maxTries = 5) {
  let attempt = 0;
  let lastError;
  while (attempt < maxTries) {
    attempt += 1;
    try {
      const res = await fetch(url, options);
      if (res.status === 429 || res.status >= 500) {
        if (attempt >= maxTries) return res;
        await sleep(300 * (2 ** (attempt - 1)));
        continue;
      }
      return res;
    } catch (err) {
      lastError = err;
      if (attempt >= maxTries) throw err;
      await sleep(300 * (2 ** (attempt - 1)));
    }
  }
  throw lastError || new Error('request retries exhausted');
}

function discoverSubmissionControls(formHtml) {
  const hiddenFields = parseHiddenFields(formHtml);
  const { startMonth, endMonth, startYear, endYear } = pickYearAndMonthControls(formHtml);
  const frequency = pickFrequencyControl(formHtml);
  const metrics = discoverMetricCheckboxes(formHtml);

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

  const submitInput = parseInputs(formHtml).find((input) => input.type === 'submit' || /retrieve|view|submit/i.test(input.value));
  if (submitInput) {
    payload[submitInput.name] = submitInput.value || 'Submit';
  }

  return payload;
}

function assertNotMaintenance(phase, html) {
  if (String(html || '').includes(MAINTENANCE_TEXT)) {
    throw new MasMsbMaintenanceError(phase);
  }
}

async function fetchMasMsbXmlI6Monthly() {
  const commonHeaders = { 'user-agent': 'macro-indicator-bot/1.0' };
  const getRes = await fetchWithRetry(MAS_MSB_XML_I6_URL, { headers: commonHeaders });
  if (!getRes.ok) throw new Error(`MAS MSB XML GET failed: HTTP ${getRes.status}`);
  const landingHtml = await getRes.text();
  assertNotMaintenance('GET', landingHtml);

  const formHtml = extractFormHtml(landingHtml);
  const payload = discoverSubmissionControls(formHtml);

  const postRes = await fetchWithRetry(MAS_MSB_XML_I6_URL, {
    method: 'POST',
    headers: {
      ...commonHeaders,
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(payload)
  });
  if (!postRes.ok) throw new Error(`MAS MSB XML POST failed: HTTP ${postRes.status}`);
  const postHtml = await postRes.text();
  assertNotMaintenance('POST', postHtml);

  return parseDataTable(postHtml);
}

module.exports = {
  MAS_MSB_XML_I6_URL,
  MasMsbMaintenanceError,
  fetchMasMsbXmlI6Monthly,
  parseMonthHeader,
  parseDataTable
};
