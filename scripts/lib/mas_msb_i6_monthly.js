const MAS_MSB_I6_PAGE_URL = 'https://www.mas.gov.sg/statistics/monthly-statistical-bulletin/i-6-commercial-banks-loan-limits-granted-to-non-bank-customers-by-industry';

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

function normalizeLabel(text) {
  return stripHtmlTags(text).toLowerCase().replace(/\s+/g, ' ').trim();
}

function parseMonthHeader(rawLabel) {
  const cleaned = stripHtmlTags(rawLabel);
  const m = cleaned.match(/^([A-Za-z]{3,})\s+(\d{4})(\s*\((P)\))?$/i);
  if (!m) return null;
  const month = MONTHS[m[1].slice(0, 3).toLowerCase()];
  if (!month) return null;
  return {
    rawLabel: cleaned,
    period: `${m[2]}-${String(month).padStart(2, '0')}`,
    prelim: Boolean(m[4])
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
      cells.push({
        kind: cellMatch[1].toLowerCase(),
        text: stripHtmlTags(cellMatch[2])
      });
    }
    if (cells.length) rows.push(cells);
  }
  return rows;
}

function extractMsbI6MonthlyFromHtml(html) {
  const tables = [...String(html || '').matchAll(/<table\b[^>]*>[\s\S]*?<\/table>/gi)].map((m) => m[0]);
  if (!tables.length) throw new Error('No tables found in MAS MSB I.6 page HTML');

  for (const table of tables) {
    const rows = extractTableRows(table);
    if (!rows.length) continue;

    const buildingIndex = rows.findIndex((cells) => cells.some((cell) => normalizeLabel(cell.text) === 'building and construction'));
    if (buildingIndex === -1) continue;

    let monthHeaders = null;
    let monthIndexes = null;
    for (let i = 0; i < rows.length; i += 1) {
      const parsed = rows[i]
        .map((cell, idx) => ({ idx, parsed: parseMonthHeader(cell.text) }))
        .filter((entry) => entry.parsed);
      if (parsed.length) {
        monthHeaders = parsed.map((x) => x.parsed);
        monthIndexes = parsed.map((x) => x.idx);
        break;
      }
    }

    if (!monthHeaders || !monthHeaders.length) {
      throw new Error('Parse error: monthHeadersCount is 0 for Building and Construction table');
    }

    const targetLabels = {
      'limits granted (s$m)': 'granted',
      'utilised (%)': 'utilised'
    };

    const matched = { granted: [], utilised: [] };
    for (let i = buildingIndex + 1; i < rows.length; i += 1) {
      const cells = rows[i];
      const firstNonEmpty = cells.find((cell) => normalizeLabel(cell.text));
      if (!firstNonEmpty) continue;
      const normalized = normalizeLabel(firstNonEmpty.text);
      const target = targetLabels[normalized];
      if (target) matched[target].push({ rowIndex: i, cells });
    }

    if (matched.granted.length !== 1 || matched.utilised.length !== 1) {
      throw new Error(
        `Parse error: expected exactly one granted and one utilised row, got granted=${matched.granted.length}, utilised=${matched.utilised.length}`
      );
    }

    const toSeriesValues = (entry, rowName) => {
      const values = monthIndexes.map((idx) => {
        if (idx >= entry.cells.length) return null;
        return parseNumericCell(entry.cells[idx].text);
      });

      if (values.length !== monthHeaders.length) {
        throw new Error(`Parse error: extracted ${values.length} cells for ${rowName}, expected ${monthHeaders.length}`);
      }

      return monthHeaders
        .map((header, i) => {
          if (values[i] == null) return null;
          return { period: header.period, prelim: header.prelim, value: values[i] };
        })
        .filter(Boolean);
    };

    const grantedValues = toSeriesValues(matched.granted[0], 'Limits Granted (S$M)');
    const utilisedValues = toSeriesValues(matched.utilised[0], 'Utilised (%)');

    return {
      monthHeaders,
      grantedValues,
      utilisedValues
    };
  }

  throw new Error('Parse error: unable to find Building and Construction table section');
}

async function fetchMasMsbI6Monthly(headers = {}) {
  const res = await fetch(MAS_MSB_I6_PAGE_URL, { headers });
  if (!res.ok) throw new Error(`MAS MSB I.6 page not reachable: HTTP ${res.status}`);
  const html = await res.text();
  return extractMsbI6MonthlyFromHtml(html);
}

module.exports = {
  MAS_MSB_I6_PAGE_URL,
  extractMsbI6MonthlyFromHtml,
  fetchMasMsbI6Monthly
};
