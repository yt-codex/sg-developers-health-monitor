const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const {
  buildProbeInputs,
  UPSTREAM_WORKFLOW_NAMES
} = require('../ops/build_probe_inputs');

const FIXED_NOW = new Date('2026-02-28T00:00:00.000Z');

function mkTmpRoot() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ops-probe-inputs-'));
}

function writeJson(rootDir, relativePath, value) {
  const absolutePath = path.join(rootDir, relativePath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
  fs.writeFileSync(absolutePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function baselineWorkflowRuns(now = FIXED_NOW) {
  const completed = new Date(now.getTime() - 2 * 60 * 60 * 1000).toISOString();
  return Object.fromEntries(
    UPSTREAM_WORKFLOW_NAMES.map((name, idx) => [
      name,
      {
        id: idx + 1,
        name,
        status: 'completed',
        conclusion: 'success',
        completed_at: completed,
        html_url: `https://github.com/example/repo/actions/runs/${idx + 1}`
      }
    ])
  );
}

function baseFixture() {
  const newsItems = [
    {
      id: 'n-1',
      title: 'Sample news item',
      link: 'https://example.com/news/1',
      source: 'CNA',
      pubDate: '2026-02-27T09:00:00.000Z',
      severity: 'info'
    },
    {
      id: 'n-2',
      title: 'Second sample news item',
      link: 'https://example.com/news/2',
      source: 'BT',
      pubDate: '2026-02-27T12:00:00.000Z',
      severity: 'watch'
    }
  ];

  return {
    'data/meta.json': {
      last_updated_sgt: '2026-02-27T20:00:00+08:00',
      status: 'success',
      counts: {
        existing_total: 10,
        new_items: 2,
        rejected_items: 0,
        total_all: 12,
        latest_90d: 12
      },
      feeds: [
        {
          source: 'CNA',
          url: 'https://example.com/cna-rss',
          status: 'ok',
          items_fetched: 8
        },
        {
          source: 'BT',
          url: 'https://example.com/bt-rss',
          status: 'ok',
          items_fetched: 6
        }
      ]
    },
    'data/news_all.json': { items: newsItems },
    'data/news_latest_90d.json': { items: newsItems },
    'data/macro_indicators.json': {
      meta: { asOf: '2026-02-27' },
      macro_indicators: {
        last_updated_utc: '2026-02-27T11:00:00.000Z',
        update_run: {
          ok_count: 10,
          failed_count: 0,
          failed_items: []
        },
        series: {
          sgs_10y: {
            status: 'ok',
            latest_value: 2.45,
            latest_period: '2026-01'
          },
          sora_overnight: {
            status: 'ok',
            latest_value: 1.95,
            latest_period: '2026-01'
          }
        }
      }
    },
    'data/processed/developer_ratios_history.json': {
      updatedAt: '2026-02-27T10:30:00.000Z',
      source: 'stockanalysis',
      scoringModel: {},
      developers: [
        {
          ticker: 'AAA',
          name: 'AAA Dev',
          fetchStatus: 'ok',
          fetchError: null
        },
        {
          ticker: 'BBB',
          name: 'BBB Dev',
          fetchStatus: 'ok',
          fetchError: null
        }
      ]
    },
    'data/processed/developer_health_diagnostics.json': {
      generatedAt: '2026-02-27T10:40:00.000Z',
      totalDevelopers: 2,
      statusCounts: {
        Green: 2,
        Amber: 0,
        Red: 0,
        'Pending data': 0
      }
    }
  };
}

function writeFixture(rootDir, fixture, { omit = [] } = {}) {
  for (const [relativePath, value] of Object.entries(fixture)) {
    if (omit.includes(relativePath)) continue;
    writeJson(rootDir, relativePath, value);
  }
}

function buildEnv() {
  return {
    GITHUB_REPOSITORY: 'example/repo',
    GITHUB_SHA: 'abc123def456',
    GITHUB_RUN_ID: '123456',
    GITHUB_SERVER_URL: 'https://github.com',
    GITHUB_WORKFLOW: 'Ops probe aggregator',
    GITHUB_JOB: 'emit-probe'
  };
}

function getCheck(payload, name) {
  return (payload.key_checks || []).find((check) => check.name === name);
}

test('all feeds down => FAIL', async () => {
  const rootDir = mkTmpRoot();
  const fixture = baseFixture();
  fixture['data/meta.json'].feeds = [
    { source: 'CNA', url: 'https://example.com/cna-rss', status: 'error', items_fetched: 0 },
    { source: 'BT', url: 'https://example.com/bt-rss', status: 'error', items_fetched: 0 }
  ];
  writeFixture(rootDir, fixture);

  const payload = await buildProbeInputs({
    rootDir,
    now: FIXED_NOW,
    env: buildEnv(),
    workflowRunsByName: baselineWorkflowRuns(FIXED_NOW)
  });

  assert.equal(payload.status, 'FAIL');
  assert.equal(payload.row_counts.news_feeds_ok, 0);
  assert.equal(payload.row_counts.news_feeds_error, 2);
  assert.equal(getCheck(payload, 'news.feed_health')?.status, 'FAIL');
});

test('macro partial degradation regression => WARN', async () => {
  const rootDir = mkTmpRoot();
  const fixture = baseFixture();
  fixture['data/macro_indicators.json'].macro_indicators.update_run = {
    ok_count: 9,
    failed_count: 1,
    failed_items: [
      {
        name: 'dataset_x',
        source: 'data.gov.sg',
        dataset_ref: 'dataset_x',
        error_summary: 'no matching series'
      }
    ]
  };
  fixture['data/macro_indicators.json'].macro_indicators.series.optional_series = {
    status: 'failed',
    error_summary: 'no matching series'
  };
  writeFixture(rootDir, fixture);

  const payload = await buildProbeInputs({
    rootDir,
    now: FIXED_NOW,
    env: buildEnv(),
    workflowRunsByName: baselineWorkflowRuns(FIXED_NOW),
    previousProbe: {
      row_counts: {
        macro_failed_count: 0,
        macro_failed_series_count: 0
      }
    }
  });

  assert.equal(payload.status, 'WARN');
  assert.equal(getCheck(payload, 'macro.partial_degradation')?.status, 'WARN');
});

test('developer parser degradation => WARN and FAIL depending on ok coverage', async (t) => {
  await t.test('partial/error with at least one ok => WARN', async () => {
    const rootDir = mkTmpRoot();
    const fixture = baseFixture();
    fixture['data/processed/developer_ratios_history.json'].developers = [
      { ticker: 'AAA', name: 'AAA Dev', fetchStatus: 'ok', fetchError: null },
      {
        ticker: 'BBB',
        name: 'BBB Dev',
        fetchStatus: 'partial',
        fetchError: 'parse error: missing ratio table'
      }
    ];
    writeFixture(rootDir, fixture);

    const payload = await buildProbeInputs({
      rootDir,
      now: FIXED_NOW,
      env: buildEnv(),
      workflowRunsByName: baselineWorkflowRuns(FIXED_NOW)
    });

    assert.equal(payload.status, 'WARN');
    assert.equal(getCheck(payload, 'developer.fetch_health')?.status, 'WARN');
  });

  await t.test('zero ok developers => FAIL', async () => {
    const rootDir = mkTmpRoot();
    const fixture = baseFixture();
    fixture['data/processed/developer_ratios_history.json'].developers = [
      {
        ticker: 'AAA',
        name: 'AAA Dev',
        fetchStatus: 'error',
        fetchError: 'interstitial block page'
      },
      {
        ticker: 'BBB',
        name: 'BBB Dev',
        fetchStatus: 'error',
        fetchError: 'fetch timeout'
      }
    ];
    writeFixture(rootDir, fixture);

    const payload = await buildProbeInputs({
      rootDir,
      now: FIXED_NOW,
      env: buildEnv(),
      workflowRunsByName: baselineWorkflowRuns(FIXED_NOW)
    });

    assert.equal(payload.status, 'FAIL');
    assert.equal(getCheck(payload, 'developer.fetch_health')?.status, 'FAIL');
  });
});

test('missing required files => FAIL', async () => {
  const rootDir = mkTmpRoot();
  const fixture = deepClone(baseFixture());
  writeFixture(rootDir, fixture, { omit: ['data/macro_indicators.json'] });

  const payload = await buildProbeInputs({
    rootDir,
    now: FIXED_NOW,
    env: buildEnv(),
    workflowRunsByName: baselineWorkflowRuns(FIXED_NOW)
  });

  assert.equal(payload.status, 'FAIL');
  assert.equal(getCheck(payload, 'required_file.data_macro_indicators.json')?.status, 'FAIL');
});

test('healthy data => OK', async () => {
  const rootDir = mkTmpRoot();
  writeFixture(rootDir, baseFixture());

  const payload = await buildProbeInputs({
    rootDir,
    now: FIXED_NOW,
    env: buildEnv(),
    workflowRunsByName: baselineWorkflowRuns(FIXED_NOW)
  });

  assert.equal(payload.status, 'OK');
  assert.equal(getCheck(payload, 'news.feed_health')?.status, 'OK');
  assert.equal(getCheck(payload, 'macro.ok_count')?.status, 'OK');
  assert.equal(getCheck(payload, 'developer.fetch_health')?.status, 'OK');
});
