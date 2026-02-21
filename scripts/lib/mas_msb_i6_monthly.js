const fs = require('fs/promises');
const path = require('path');
const { MAS_MSB_I6_PAGE_URL, downloadMasI6Csv } = require('./mas_i6_download_csv');

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

const END_OF_PERIOD_HEADER = 'End of Period';
const GRANTED_HEADER = 'Building and Construction - Limits Granted (S$M)';
const UTILISED_HEADER = 'Building and Construction - Utilised (%)';

function sanitizeText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function parseCsv(csvText) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < csvText.length; i += 1) {
    const ch = csvText[i];

    if (inQuotes) {
      if (ch === '"') {
        if (csvText[i + 1] === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }

    if (ch === ',') {
      row.push(field);
      field = '';
      continue;
    }

    if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      continue;
    }

    if (ch !== '\r') {
      field += ch;
    }
  }

  row.push(field);
  if (row.length > 1 || sanitizeText(row[0])) {
    rows.push(row);
  }

  return rows;
}

function parseNumberCell(raw) {
  const cleaned = sanitizeText(raw);
  if (!cleaned || cleaned === '-' || /^na$/i.test(cleaned) || /^n\/a$/i.test(cleaned)) return null;
  const parsed = Number(cleaned.replace(/,/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseEndOfPeriod(raw) {
  const cleaned = sanitizeText(raw);
  if (!cleaned) return null;
  const prelim = /\(P\)/i.test(cleaned);
  const base = cleaned.replace(/\s*\(P\)\s*/gi, '').trim();

  let match = base.match(/^([A-Za-z]{3})\s+(\d{4})$/);
  if (!match) match = base.match(/^([A-Za-z]{3})-(\d{2}|\d{4})$/);
  if (!match) match = base.match(/^([A-Za-z]{3})\s*-(\d{2}|\d{4})$/);
  if (!match) return null;

  const monthNum = MONTHS[match[1].toLowerCase()];
  if (!monthNum) return null;

  let year = Number(match[2]);
  if (!Number.isFinite(year)) return null;
  if (year < 100) year += 2000;

  return {
    period: `${year}-${String(monthNum).padStart(2, '0')}`,
    prelim
  };
}

function findHeaderIndex(rows) {
  return rows.findIndex((row) => row.some((cell) => sanitizeText(cell) === END_OF_PERIOD_HEADER));
}

function detectHeaders(headerRow) {
  return headerRow.map((cell) => sanitizeText(cell));
}

function parseMasI6Csv(csvText) {
  const rows = parseCsv(csvText);
  if (!rows.length) throw new Error('CSV parser returned 0 rows');

  const headerIndex = findHeaderIndex(rows);
  if (headerIndex === -1) throw new Error('Could not find header row containing "End of Period"');

  const headers = detectHeaders(rows[headerIndex]);
  const periodCol = headers.findIndex((h) => h === END_OF_PERIOD_HEADER);
  const grantedCol = headers.findIndex((h) => h === GRANTED_HEADER);
  const utilisedCol = headers.findIndex((h) => h === UTILISED_HEADER);

  if (periodCol === -1 || grantedCol === -1 || utilisedCol === -1) {
    const first30 = headers.slice(0, 30).join(' | ');
    throw new Error(
      `Required MAS I.6 columns not found. Need: "${GRANTED_HEADER}" and "${UTILISED_HEADER}". First headers: ${first30}`
    );
  }

  const grantedValues = [];
  const utilisedValues = [];
  const parsedPeriods = [];

  for (let i = headerIndex + 1; i < rows.length; i += 1) {
    const row = rows[i];
    const periodInfo = parseEndOfPeriod(row[periodCol]);
    if (!periodInfo) continue;

    const grantedNum = parseNumberCell(row[grantedCol]);
    const utilisedNum = parseNumberCell(row[utilisedCol]);
    if (grantedNum != null) grantedValues.push({ period: periodInfo.period, prelim: periodInfo.prelim, value: grantedNum });
    if (utilisedNum != null) utilisedValues.push({ period: periodInfo.period, prelim: periodInfo.prelim, value: utilisedNum });
    parsedPeriods.push(periodInfo);
  }

  if (!grantedValues.length) throw new Error(`No numeric values parsed for column: ${GRANTED_HEADER}`);
  if (!utilisedValues.length) throw new Error(`No numeric values parsed for column: ${UTILISED_HEADER}`);

  grantedValues.sort((a, b) => a.period.localeCompare(b.period));
  utilisedValues.sort((a, b) => a.period.localeCompare(b.period));

  const newestPeriods = [...parsedPeriods]
    .sort((a, b) => a.period.localeCompare(b.period))
    .slice(-3)
    .reverse();

  return {
    grantedValues,
    utilisedValues,
    extractedRowCount: parsedPeriods.length,
    newestPeriods,
    detectedHeaders: headers
  };
}

async function dumpArtifactsOnFailure({ downloadPath, csvText, parseError, detectedHeaders, verifyMode }) {
  if (!verifyMode) return;
  const artifactsDir = path.join(process.cwd(), 'artifacts');
  await fs.mkdir(artifactsDir, { recursive: true });

  if (downloadPath) {
    try {
      const artifactPath = path.join(artifactsDir, 'mas_i6_failed.csv');
      await fs.copyFile(downloadPath, artifactPath);
      console.log(`[verify-mas-msb-i6] saved_failed_csv=${artifactPath}`);
    } catch (err) {
      console.log(`[verify-mas-msb-i6] failed_to_save_csv_artifact=${err.message}`);
    }
  }

  if (csvText) {
    const first5 = csvText
      .split(/\r?\n/)
      .slice(0, 5)
      .map((line) => sanitizeText(line).slice(0, 240));
    console.log(`[verify-mas-msb-i6] csv_first_5_lines=${first5.join(' || ')}`);
  }

  if (Array.isArray(detectedHeaders) && detectedHeaders.length) {
    console.log(`[verify-mas-msb-i6] detected_headers=${detectedHeaders.slice(0, 30).join(' | ')}`);
  }

  if (parseError) {
    console.log(`[verify-mas-msb-i6] parse_error=${parseError.message}`);
  }
}

async function fetchMasMsbI6Monthly({ verifyMode = false } = {}) {
  const outputPath = verifyMode ? path.join(process.cwd(), 'artifacts', 'mas_i6.csv') : '/tmp/mas_i6.csv';
  let csvText = '';
  let detectedHeaders = [];

  try {
    const download = await downloadMasI6Csv({ outputPath, verifyMode });
    csvText = await fs.readFile(download.outputPath, 'utf8');
    const parsed = parseMasI6Csv(csvText);

    if (verifyMode) {
      const grantedLatest = parsed.grantedValues[parsed.grantedValues.length - 1];
      const utilisedLatest = parsed.utilisedValues[parsed.utilisedValues.length - 1];
      console.log(`[verify-mas-msb-i6] extracted_row_count=${parsed.extractedRowCount}`);
      console.log(`[verify-mas-msb-i6] newest_3_periods=${parsed.newestPeriods.map((p) => `${p.period}, prelim=${p.prelim}`).join(' | ')}`);
      console.log(`[verify-mas-msb-i6] granted_latest=${grantedLatest.period} value=${grantedLatest.value} prelim=${grantedLatest.prelim}`);
      console.log(`[verify-mas-msb-i6] utilised_latest=${utilisedLatest.period} value=${utilisedLatest.value} prelim=${utilisedLatest.prelim}`);
      console.log(`[verify-mas-msb-i6] csv_downloaded_size_bytes=${download.sizeBytes}`);
    }

    return parsed;
  } catch (err) {
    try {
      if (!detectedHeaders.length && csvText) {
        const rows = parseCsv(csvText);
        const headerIndex = findHeaderIndex(rows);
        if (headerIndex !== -1) detectedHeaders = detectHeaders(rows[headerIndex]);
      }
    } catch (_) {
      // ignore secondary parse diagnostics failures
    }
    await dumpArtifactsOnFailure({ downloadPath: outputPath, csvText, parseError: err, detectedHeaders, verifyMode });
    throw err;
  }
}

module.exports = {
  MAS_MSB_I6_PAGE_URL,
  fetchMasMsbI6Monthly,
  parseMasI6Csv,
  parseEndOfPeriod
};
