const fs = require('node:fs');
const path = require('node:path');

const requiredFiles = [
  'macro.html',
  path.join('data', 'macro_indicators.json'),
  path.join('data', 'macro_stress_signals.json')
];

const requiredMacroHooks = ['id="macro-grid"', 'id="macro-risk"', 'id="category-filter"'];

function assertExists(relativePath) {
  const absolutePath = path.join(process.cwd(), relativePath);
  if (!fs.existsSync(absolutePath)) {
    throw new Error(`Missing required file: ${relativePath}`);
  }
}

function assertMacroHooks() {
  const macroHtml = fs.readFileSync(path.join(process.cwd(), 'macro.html'), 'utf8');
  for (const hook of requiredMacroHooks) {
    if (!macroHtml.includes(hook)) {
      throw new Error(`macro.html is missing expected hook: ${hook}`);
    }
  }
}

function run() {
  for (const filePath of requiredFiles) {
    assertExists(filePath);
  }
  assertMacroHooks();
  console.log('Smoke checks passed: required files and macro hooks are present.');
}

run();
