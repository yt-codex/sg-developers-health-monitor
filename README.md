# Singapore Property Developers Vulnerability Monitor

Static multi-page dashboard for GitHub Pages that monitors vulnerability signals for Singapore-listed property developers.

## Repository layout

```text
.
├── assets/
│   ├── css/styles.css
│   └── js/
│       ├── app.js
│       ├── macro.js
│       ├── news.js
│       └── developers.js
├── data/
│   ├── site_meta.json
│   ├── macro_indicators.json
│   ├── developer_news.json
│   ├── risk_rules.json
│   └── listed_developers.json
├── scripts/
│   ├── update_macro_indicators.js
│   └── lib/
│       ├── datagov.js
│       ├── mas_api.js
│       └── singstat_tablebuilder.js
├── .github/workflows/
│   ├── update_data.yml
│   └── update-macro.yml
├── index.html
├── macro.html
├── news.html
├── developers.html
└── package.json
```

## Pages
- `index.html` – overview and navigation
- `macro.html` – macro indicator tiles, sparkline trends, and risk temperature
- `news.html` – feed with regex risk tags and filters
- `developers.html` – listed developers health score table with driver explanations

## Setup

### Prerequisites
- Node.js 20+ (for macro updater scripts)
- Python 3 (optional, for local static file server)
- `DATA_GOV_SG_API_KEY` for authenticated `data.gov.sg` dataset calls

### Install

```bash
npm install
```

## Run locally
Because pages load local JSON via `fetch`, serve the repo over HTTP:

```bash
python -m http.server 8000
```

Then open `http://localhost:8000/index.html`.

## Data update commands

```bash
npm run verify:macro-sources
npm run update:macro
npm test
```

- `verify:macro-sources` checks upstream macro data sources and logs diagnostics.
- `update:macro` refreshes `data/macro_indicators.json`.
- `test` runs parser + integration tests, including live source contract checks for SingStat TableBuilder `M700071` and `M183741` (network-dependent; auto-skipped if unreachable).


## Macro source details (SingStat TableBuilder)
- Indicators `sora_overnight`, `sgs_2y`, and `sgs_10y` are sourced from **SingStat TableBuilder `M700071`** via JSON endpoint `https://tablebuilder.singstat.gov.sg/api/table/tabledata/M700071`.
- Indicator `unit_labour_cost_construction` is sourced from **SingStat TableBuilder `M183741`** via JSON endpoint `https://tablebuilder.singstat.gov.sg/api/table/tabledata/M183741`.
- Parsing expects TableBuilder's pivoted JSON shape (time periods as columns, “Data Series” labels as rows), then transforms to tidy long records `{date, series_name, value}`.
- Frequency is treated as **monthly (M)** for these SingStat series.
- Date labels are parsed from `YYYY Mon` to ISO date using the **first day of month** convention (e.g., `2025 Dec -> 2025-12-01`).
- Missing/blank/NA values are treated as missing and dropped; numeric points are coerced to numbers, sorted in ascending date order, and deduplicated by `(series_name, date)` with the latest parsed observation retained.

## Environment variables
- `DATA_GOV_SG_API_KEY` (required for authenticated `data.gov.sg` access)
- `GITHUB_ACTIONS` (set by GitHub Actions automatically; used by updater behavior)
- `SINGSTAT_TABLEBUILDER_API_BASE` (optional override for SingStat API base URL)

For local development, either export in shell:

```bash
export DATA_GOV_SG_API_KEY="..."
```

or create `.env` in repo root:

```bash
DATA_GOV_SG_API_KEY=your_key_here
```

## GitHub Actions automation
- `.github/workflows/update_data.yml`: refreshes metadata timestamps daily/manual.
- `.github/workflows/update-macro.yml`: verifies and updates macro indicators daily/manual.

Set repository secret:
- Name: `DATA_GOV_SG_API_KEY`
- Path: `Settings → Secrets and variables → Actions`

## Data files
- `data/site_meta.json` – global timestamp shown on pages
- `data/macro_indicators.json` – indicator config, series, thresholds, explanations
- `data/developer_news.json` – pre-fetched news items
- `data/risk_rules.json` – regex tagging rules and severity hierarchy
- `data/listed_developers.json` – developer metrics and scoring model
