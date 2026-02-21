# Hygiene Report

## Scope and method
Conservative hygiene pass focused on deleting only items with strong evidence of non-use. Evidence was collected with repository-wide `rg` scans, entrypoint/workflow inspection, and runnable checks.

## Deleted items

### 1) Unused helper function: `parsePeriodToDate` in `scripts/update_macro_indicators.js`
- **Signal A (reference scan):** `rg -n "parsePeriodToDate" .` returned only the function definition in `scripts/update_macro_indicators.js` and no call sites.
- **Signal B (entrypoint/runtime path review):** The only Node runtime script entrypoint is `scripts/update_macro_indicators.js` via `package.json` scripts and `.github/workflows/update-macro.yml`; inspection showed no dynamic lookup/eval that could invoke this function indirectly.
- **Action:** Function deleted.

### 2) Root placeholder file: `.gitkeep`
- **Signal A (reference scan):** `rg -n "\.gitkeep|gitkeep" .` found no references.
- **Signal B (filesystem usage):** The file was empty (`wc -c .gitkeep` => `0`) and there were no empty directories requiring a keepfile (`find . -type d -empty -not -path './.git/*'` returned none).
- **Action:** File deleted.

## Kept-but-suspicious candidates

### 1) `scripts/lib/mas_api.js` extensive diagnostics/non-JSON artifact handling
- **Why suspicious:** Large helper module with verbose diagnostics and retry logic.
- **Why kept:** It is directly imported by `scripts/update_macro_indicators.js` and used by the `verify:macro-sources` / `update:macro` paths. Dynamic and fallback paths make conservative deletion unsafe.
- **Follow-up check:** If desired, add focused unit tests for helper branches, then prune only branches proven unreachable.

### 2) Workflow dependency cleanup in `.github/workflows/update-macro.yml`
- **Resolution:** Removed Playwright environment variable, cache step, and browser install step after confirming no repository code depends on Playwright.
- **Result:** Macro update workflow now runs only Node setup, dependency install, and macro verify/update commands.

## Dependency changes
- **No dependency removals performed.**
- `package.json` has no declared dependencies/devDependencies to prune; only script commands are defined.
- `npm install` confirmed no dependency graph beyond the root package.

## Verification run
- `npm install` → success.
- `npm run verify:macro-sources` → fails without `DATA_GOV_SG_API_KEY` (expected environment prerequisite, not a code regression).
- `node --check` on all JS runtime/browser files → success.

## Commit plan used
- **Commit 1 (deps):** Not created (no dependency changes were provably available).
- **Commit 2 (unused code/files):** Remove dead function and root placeholder file.
- **Commit 3 (docs/report):** Update `README.md` and add `HYGIENE_REPORT.md`.
