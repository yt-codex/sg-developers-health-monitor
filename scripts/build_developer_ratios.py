#!/usr/bin/env python3
import csv
import json
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / 'data' / 'listed developer list.csv'
OUTPUT_JSON = ROOT / 'data' / 'processed' / 'developer_ratios_history.json'
CACHE_DIR = ROOT / 'data' / 'cache' / 'stockanalysis'

METRICS = {
    'marketCap': {'label': 'Market Capitalization', 'aliases': ['Market Capitalization'], 'unit': 'millions SGD'},
    'netDebtToEbitda': {'label': 'Net Debt / EBITDA Ratio', 'aliases': ['Net Debt / EBITDA Ratio', 'Net Debt / EBITDA']},
    'debtToEquity': {'label': 'Debt / Equity Ratio', 'aliases': ['Debt / Equity Ratio', 'Debt / Equity']},
    'netDebtToEquity': {'label': 'Net Debt / Equity Ratio', 'aliases': ['Net Debt / Equity Ratio', 'Net Debt / Equity']},
    'debtToEbitda': {'label': 'Debt / EBITDA Ratio', 'aliases': ['Debt / EBITDA Ratio', 'Debt / EBITDA']},
    'quickRatio': {'label': 'Quick Ratio', 'aliases': ['Quick Ratio']},
    'currentRatio': {'label': 'Current Ratio', 'aliases': ['Current Ratio']},
    'roic': {'label': 'ROIC', 'aliases': ['ROIC', 'Return on Invested Capital (ROIC)']},
    'roe': {'label': 'ROE', 'aliases': ['ROE', 'Return on Equity (ROE)']},
    'payoutRatio': {'label': 'Payout Ratio', 'aliases': ['Payout Ratio']},
    'assetTurnover': {'label': 'Asset Turnover', 'aliases': ['Asset Turnover', 'Asset Turnover Ratio']},
}

MISSING_RE = re.compile(r'^(?:-|--|n/?a|na|none|null)?$', re.I)


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def normalize_label(value: str) -> str:
    text = (value or '').lower()
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^a-z0-9/% ]', '', text)
    text = re.sub(r' ratio$', '', text)
    return text.strip()


ALIAS_LOOKUP = {normalize_label(alias): key for key, meta in METRICS.items() for alias in meta['aliases']}


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.curr_table = []
        self.curr_row = []
        self.curr_cell = []

    def handle_starttag(self, tag, attrs):
        t = tag.lower()
        if t == 'table':
            self.in_table = True
            self.curr_table = []
        elif self.in_table and t == 'tr':
            self.in_row = True
            self.curr_row = []
        elif self.in_row and t in ('th', 'td'):
            self.in_cell = True
            self.curr_cell = []

    def handle_data(self, data):
        if self.in_cell:
            self.curr_cell.append(data)

    def handle_endtag(self, tag):
        t = tag.lower()
        if t in ('th', 'td') and self.in_cell:
            cell = re.sub(r'\s+', ' ', ''.join(self.curr_cell)).strip()
            self.curr_row.append(cell)
            self.in_cell = False
        elif t == 'tr' and self.in_row:
            if self.curr_row:
                self.curr_table.append(self.curr_row)
            self.in_row = False
        elif t == 'table' and self.in_table:
            if self.curr_table:
                self.tables.append(self.curr_table)
            self.in_table = False


def parse_numeric(raw):
    if raw is None:
        return None
    t = str(raw).replace('\xa0', ' ').strip()
    if not t or MISSING_RE.match(t):
        return None
    cleaned = t.replace(',', '').replace('×', '').replace(' ', '')
    cleaned = re.sub(r'^[A-Za-z$]+', '', cleaned)
    cleaned = cleaned.rstrip('%')
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_period_label(label):
    txt = (label or '').strip()
    if not txt:
        return None
    if txt.lower() == 'current':
        return 'Current'
    m = re.match(r'^(?:FY\s*)?(20\d{2})$', txt, re.I)
    if m:
        return f"FY {m.group(1)}"
    return txt


def empty_metrics():
    return {
        key: {'label': meta['label'], 'values': {}, 'rawValues': {}, 'unit': meta.get('unit')}
        for key, meta in METRICS.items()
    }


def parse_ratios_from_html(html):
    parser = TableParser()
    parser.feed(html)

    for table in parser.tables:
        header_idx = next((i for i, row in enumerate(table) if row and row[0].lower().startswith('ratio')), -1)
        if header_idx < 0:
            header_idx = next((i for i, row in enumerate(table) if any('current' in c.lower() for c in row)), -1)
        if header_idx < 0:
            continue

        headers = table[header_idx]
        periods = [parse_period_label(x) for x in headers[1:]]
        periods = [p for p in periods if p]
        if not periods:
            continue

        period_ending = {}
        metrics = empty_metrics()

        for row in table[header_idx + 1:]:
            if not row:
                continue
            head = row[0]
            if head.lower() == 'period ending':
                for i, period in enumerate(periods):
                    period_ending[period] = row[i + 1] if i + 1 < len(row) else None
                continue

            key = ALIAS_LOOKUP.get(normalize_label(head))
            if not key:
                continue

            for i, period in enumerate(periods):
                raw = row[i + 1] if i + 1 < len(row) else None
                metrics[key]['rawValues'][period] = raw
                metrics[key]['values'][period] = parse_numeric(raw)

        period_rows = [{'label': p, 'periodEnding': period_ending.get(p)} for p in periods]
        return {'periods': period_rows, 'metrics': metrics}

    raise ValueError('Unable to locate StockAnalysis ratios table')


def fetch_html(url, retries=3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0 (SGDevelopersHealthMonitor/1.0)'})
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except (HTTPError, URLError, TimeoutError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.5 * (2 ** (attempt - 1)))
    raise last_err


def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with INPUT_CSV.open(newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    out = {'updatedAt': now_iso(), 'source': 'stockanalysis', 'developers': []}

    for row in rows:
        ticker = (row.get('stockanalysis_symbol') or row.get('sgx_ticker') or '').strip().upper()
        url = (row.get('stockanalysis_ratios_url') or '').strip()
        record = {
            'ticker': ticker,
            'name': row.get('company_name', ''),
            'stockanalysis_ratios_url': url,
            'periods': [],
            'metrics': empty_metrics(),
            'lastFetchedAt': now_iso(),
            'fetchStatus': 'error',
            'fetchError': None,
        }

        try:
            if not url:
                raise ValueError('Missing stockanalysis_ratios_url')
            html = fetch_html(url)
            (CACHE_DIR / f'{ticker}.html').write_text(html, encoding='utf-8')
            parsed = parse_ratios_from_html(html)
            record['periods'] = parsed['periods']
            record['metrics'] = parsed['metrics']
            captured = sum(1 for m in parsed['metrics'].values() if m['values'])
            record['fetchStatus'] = 'ok' if captured == len(METRICS) else 'partial'
            (CACHE_DIR / f'{ticker}.json').write_text(json.dumps(parsed, indent=2), encoding='utf-8')
            if ticker == '9CI':
                labels = ', '.join([p['label'] for p in parsed['periods']])
                print(f'[debug 9CI] periods={labels}; metricsCaptured={captured}')
            print(f'[{ticker}] {record["fetchStatus"]}')
        except Exception as exc:
            record['fetchStatus'] = 'error'
            record['fetchError'] = str(exc)
            print(f'[{ticker}] error: {exc}')

        out['developers'].append(record)

    out['updatedAt'] = now_iso()
    OUTPUT_JSON.write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(f'Wrote {OUTPUT_JSON}')


if __name__ == '__main__':
    main()
