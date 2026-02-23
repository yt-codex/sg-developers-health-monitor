const path = require('node:path');
const fs = require('node:fs');

const REQUIRED_FIELDS = ['id', 'title', 'link', 'source', 'pubDate', 'severity'];

function readJson(relativePath) {
  const absolutePath = path.join(process.cwd(), relativePath);
  const content = fs.readFileSync(absolutePath, 'utf8');
  return JSON.parse(content);
}

function assertNewsShape(items, label) {
  if (!Array.isArray(items)) {
    throw new Error(`${label} must contain an items array`);
  }

  items.forEach((item, idx) => {
    for (const field of REQUIRED_FIELDS) {
      if (!item[field]) {
        throw new Error(`${label} item[${idx}] is missing required field: ${field}`);
      }
    }
  });
}

function run() {
  const allNews = readJson(path.join('data', 'news_all.json'));
  const latest90d = readJson(path.join('data', 'news_latest_90d.json'));

  assertNewsShape(allNews.items, 'data/news_all.json');
  assertNewsShape(latest90d.items, 'data/news_latest_90d.json');

  console.log(`Validated news JSON: all=${allNews.items.length}, latest_90d=${latest90d.items.length}`);
}

run();
