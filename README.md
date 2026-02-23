# Singapore Property Developers Vulnerability Monitor

Static multi-page dashboard for GitHub Pages that monitors vulnerability signals for Singapore-listed property developers.

## Repository layout (high level)

```text
.
├── assets/
│   ├── css/styles.css
│   └── js/
│       ├── app.js
│       ├── macro.js
│       ├── news.js
│       └── developers.js
├── config/
│   ├── developers.json
│   ├── google_news_queries.json
│   ├── news_pipeline.json
│   ├── relevance_rules.json
│   └── tag_rules.json
├── data/
│   ├── listed_developers.json
│   ├── macro_indicators.json
│   ├── macro_stress_signals.json
│   ├── news_all.json
│   ├── news_latest_90d.json
│   ├── meta.json
│   └── site_meta.json
├── scripts/
│   ├── update_macro_indicators.js
│   ├── update_news.js
│   ├── validate_news_data.js
│   ├── smoke_check.js
│   └── lib/
├── tests/
├── index.html
├── macro.html
├── news.html
├── developers.html
└── package.json
```

## Pages
- `index.html` – overview and navigation
- `macro.html` – macro indicator tiles, sparkline trends, and risk temperature
- `news.html` – developer news feed with filters, tooltips, and pagination
- `developers.html` – listed developers health score table with driver explanations

## Setup

### Prerequisites
- Node.js 20+ (for updater scripts)
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

## Data update and validation commands

```bash
npm run verify:macro-sources
npm run update:macro
npm run update:news
npm run validate:news
npm run smoke
npm test
```

- `update:news` refreshes developer news from configured RSS feeds, applies relevance + severity tagging, appends only new items to `data/news_all.json`, and derives `data/news_latest_90d.json`.
- `validate:news` verifies `data/news_all.json` and `data/news_latest_90d.json` parse correctly and include required fields.
- `smoke` runs local smoke checks for required runtime files and key macro page hooks.

## News ingestion logic
- Main script: `scripts/update_news.js`.
- Feed/query/tagging config lives under `config/`:
  - `news_pipeline.json` – RSS feed URLs + frontend news constants
  - `google_news_queries.json` – Google query list
  - `relevance_rules.json` – SG developer relevance and hard negatives
  - `tag_rules.json` – severity/tag regex rules
  - `developers.json` – developer aliases for matching
- Persistence behavior:
  - append-only store: `data/news_all.json`
  - derived 90-day store: `data/news_latest_90d.json`
  - run metadata: `data/meta.json`
  - rejection audit log: `data/rejected_items.log`

## Run the news updater locally

```bash
npm run update:news
```

Google mode examples:

```bash
node scripts/update_news.js --source=google --mode=backfill --days=365
node scripts/update_news.js --source=google --mode=delta --days=7 --max_queries=10
```

## Compatibility notes (do not edit casually)
- Keep data schema fields used by frontend renderers (`id`, `title`, `link`, `source`, `pubDate`, `severity`, etc.) stable.
- Keep page selectors and IDs referenced by scripts stable, especially:
  - News: `#news-list`, `#news-pagination`, `#news-filters`, `#severity-filter`, `#developer-filter`, `#source-filter`, `#date-range`, `#search-text`
  - Macro: `#macro-grid`, `#macro-risk`, `#category-filter`
- Legacy news severity values such as `critical` are normalized at runtime for backward compatibility.

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
