const fs = require('node:fs');
const path = require('node:path');

const csvPath = path.join(__dirname, '..', 'data', 'listed developer list.csv');

function parseCsv(input) {
  const lines = input.trimEnd().split(/\r?\n/);
  return lines.map((line) => line.split(',').map((cell) => cell.replace(/^"|"$/g, '').trim()));
}

function toCsv(rows) {
  return rows
    .map((row) => row.map((cell) => {
      const text = cell == null ? '' : String(cell);
      if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
      return text;
    }).join(','))
    .join('\n') + '\n';
}

const raw = fs.readFileSync(csvPath, 'utf8');
const rows = parseCsv(raw);
const header = rows[0];

const tickerIndex = header.indexOf('sgx_ticker');
if (tickerIndex < 0) {
  throw new Error('sgx_ticker column missing in listed developer list.csv');
}

let symbolIndex = header.indexOf('stockanalysis_symbol');
if (symbolIndex < 0) {
  header.push('stockanalysis_symbol');
  symbolIndex = header.length - 1;
}

let urlIndex = header.indexOf('stockanalysis_ratios_url');
if (urlIndex < 0) {
  header.push('stockanalysis_ratios_url');
  urlIndex = header.length - 1;
}

for (let i = 1; i < rows.length; i += 1) {
  const row = rows[i];
  while (row.length < header.length) row.push('');
  const symbol = (row[tickerIndex] || '').trim().toUpperCase();
  row[symbolIndex] = symbol;
  row[urlIndex] = symbol ? `https://stockanalysis.com/quote/sgx/${symbol}/financials/ratios/` : '';
}

fs.writeFileSync(csvPath, toCsv(rows), 'utf8');
console.log(`Updated ${rows.length - 1} developers in ${csvPath}`);
