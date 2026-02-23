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
│   ├── macro_stress_signals.json
│   ├── developer_news.json
│   ├── news_all.json
│   ├── news_latest_90d.json
│   ├── meta.json
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
npm run update:news
npm test
npm run smoke
```

- `verify:macro-sources` checks upstream macro data sources and logs diagnostics.
- `update:macro` refreshes `data/macro_indicators.json` and derives `data/macro_stress_signals.json`.
- `test` runs parser + integration tests, including live source contract checks for SingStat TableBuilder `M700071` and `M183741` (network-dependent; auto-skipped if unreachable).
- `smoke` runs a local dashboard smoke check to assert required runtime files and `macro.html` hooks (`#macro-grid`, `#macro-risk`, `#category-filter`) are present.

## Repo hygiene checks
Run this lightweight verification before and after cleanup changes:

```bash
npm run smoke
```

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
- `.github/workflows/update_news.yml`: ingests CNA/BT/ST RSS feeds daily at 02:00 SGT (18:00 UTC), and supports optional Google News ingestion via manual workflow dispatch inputs (`enable_google`, `google_mode`, `backfill_days`).

Set repository secret:
- Name: `DATA_GOV_SG_API_KEY`
- Path: `Settings → Secrets and variables → Actions`

## Data files
- `data/site_meta.json` – global timestamp shown on pages
- `data/macro_indicators.json` – indicator config, series, thresholds, explanations
- `data/macro_stress_signals.json` – derived stress statuses for sector performance, labour cost, and interest rates
- `data/developer_news.json` – deprecated placeholder (news now sourced from RSS pipeline outputs)
- `data/news_all.json` – append-only historical RSS store (all items retained)
- `data/news_latest_90d.json` – derived subset limited to the last 90 days for frontend loading
- `data/meta.json` – latest news pipeline run metadata and per-feed status
- `data/risk_rules.json` – regex tagging rules and severity hierarchy
- `data/listed_developers.json` – developer metrics and scoring model


## News updater (RSS ingestion)
Run the developer news pipeline locally:

```bash
npm run update:news
```

This script reads feeds from CNA, BT, and ST, deduplicates by canonicalized link hash, appends only new records to `data/news_all.json`, derives `data/news_latest_90d.json`, and updates `data/meta.json`.


### Google News backfill and delta modes
The news updater supports Google News RSS Search as an additional source for older backfill and optional delta catch-up, while reusing the same relevance filters used for CNA/BT/ST.

- Query config: `config/google_news_queries.json` (`queries` array).
- Endpoint format: `https://news.google.com/rss/search?q=<query>&hl=en-SG&gl=SG&ceid=SG:en`
- Relevance gate: unchanged; all Google items must pass existing rules (`developer_match` OR SG+property/developer-topic) and still respect hard negatives.
- Rejections: rejected Google items are appended to `data/rejected_items.log` with rejection reason.

Run Google backfill manually (example 365-day backfill):

```bash
node scripts/update_news.js --source=google --mode=backfill --days=365
```

Run Google delta mode (example 7-day catch-up, optional query limit):

```bash
node scripts/update_news.js --source=google --mode=delta --days=7 --max_queries=10
```

Backfill mode attempts iterative windows using Google query `when:Xd` chunks (7/30/90/180/N days). Google support for `when:` can vary, so partial backfill is possible.

For Google items, the pipeline stores additional fields:
- `aggregator: "google_news"`
- `original_link` (Google RSS link)
- `resolved_link` (best-effort destination URL)
- `link` (resolved if available; otherwise original)
- `publisher` (best-effort title suffix extraction)
- `query` (triggering Google query string)

Google URL resolution priority:
1. `<source url="...">` from RSS item, if present
2. Decoding from Google URL, if possible
3. One HTTP GET with redirects; use final URL
4. Fallback to the original Google URL

Deduplication priority:
1. `resolved_link`
2. `<source url>`
3. canonicalized `link`
4. hash of `(normalized title + publisher + date)` as fallback

### Editing severity/tag rules
- Edit `config/tag_rules.json` to update severity regex patterns and tooltip definitions without code changes.
- Edit `config/developers.json` to add or revise known developer aliases used for entity extraction.
