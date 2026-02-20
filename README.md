# Singapore Property Developers Vulnerability Monitor

Static multi-page dashboard for GitHub Pages that monitors vulnerability signals for Singapore-listed property developers.

## Pages
- `index.html` – overview and navigation
- `macro.html` – macro indicator tiles, sparkline trends, and risk temperature
- `news.html` – RSS-style feed with regex risk tags and filters
- `developers.html` – listed developers health score table with driver explanations

## Run locally
Because each page loads local JSON via `fetch`, use a local static server:

```bash
python -m http.server 8000
```

Then open `http://localhost:8000/index.html`.

## Deploy to GitHub Pages
1. Push repository to GitHub.
2. In **Settings → Pages**, choose **Deploy from a branch**.
3. Select branch (`main` or your publishing branch) and root (`/`).
4. Visit `https://<user>.github.io/<repo>/index.html`.

All paths are relative (`./...`) so it works from subpaths.

## Data layer
All data is local JSON under `/data`:
- `site_meta.json` – global timestamp shown on every page
- `macro_indicators.json` – indicator configuration, series, thresholds, and explanations
- `developer_news.json` – pre-fetched news items
- `risk_rules.json` – regex tagging rules and severity hierarchy
- `listed_developers.json` – developer metrics and scoring model

## Edit risk rules
Open `data/risk_rules.json` and edit the ordered `rules` array:
- `severity`: `Critical`, `Warning`, `Watch`, `Info`
- `regex`: pattern string used by `new RegExp(...)`
- `flags`: usually `i` for case-insensitive matching
- `appliesTo`: fields (`title`, `summary`, `raw`)
- `label` + `rationale`: shown as matched chips/tooltips in UI

Rules are evaluated in order; all matching rules are shown and the highest severity becomes the primary badge.

## Update developer metrics and scoring
1. Edit `data/listed_developers.json`.
2. Update `scoringModel.weights` to tune factor importance.
3. Update `scoringModel.bands.status` to adjust Green/Amber/Red cutoffs.
4. For each developer, revise `metrics` + narrative `drivers` + `notes` + `lastUpdated`.

The table computes score transparently in browser JavaScript unless `precomputedHealthScore` is set.

## Optional automation (GitHub Actions)
Workflow: `.github/workflows/update_data.yml`
- Runs daily and on manual dispatch.
- Updates data timestamps and commits changes.

You can extend it to fetch public RSS/macro sources and normalize into `/data/*.json` before commit.
