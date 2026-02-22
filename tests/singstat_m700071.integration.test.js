const test = require('node:test');
const assert = require('node:assert/strict');
const {
  fetchSingStatRequiredSeries,
  fetchUnitLabourCostConstructionSeries,
  fetchConstructionGdpSeries
} = require('../scripts/lib/singstat_tablebuilder');

function isMonotonicAscending(items) {
  for (let i = 1; i < items.length; i += 1) {
    if (items[i - 1].date > items[i].date) return false;
  }
  return true;
}

function assertRecentDate(dateString, maxLagDays) {
  const then = new Date(`${dateString}T00:00:00Z`);
  const now = new Date();
  const lagDays = (now.getTime() - then.getTime()) / (24 * 60 * 60 * 1000);
  assert.ok(lagDays <= maxLagDays, `latest date ${dateString} is too old (${Math.round(lagDays)} days)`);
}

function shouldSkipLive(err) {
  const text = String(err?.message || err || '');
  return /ENETUNREACH|EAI_AGAIN|ECONNRESET|timed out|CONNECT tunnel failed|fetch failed/i.test(text);
}

test('SingStat M700071 includes SORA + SGS 2Y + SGS 10Y with numeric ascending monthly data', async (t) => {
  let bundle;
  try {
    bundle = await fetchSingStatRequiredSeries({ tableId: 'M700071' });
  } catch (err) {
    if (shouldSkipLive(err)) t.skip(`live SingStat unavailable: ${err.message}`);
    throw err;
  }

  for (const key of ['SORA', 'SGS_2Y', 'SGS_10Y']) {
    assert.ok(bundle[key], `missing ${key} in SingStat result`);
    assert.ok(Array.isArray(bundle[key].rows), `${key} rows is not an array`);
    assert.ok(bundle[key].rows.length > 0, `${key} rows is empty`);

    const rows = bundle[key].rows;
    assert.ok(isMonotonicAscending(rows), `${key} dates are not monotonic ascending`);

    for (const row of rows) {
      assert.match(row.date, /^\d{4}-\d{2}-\d{2}$/, `${key} row date is not ISO date: ${row.date}`);
      assert.equal(typeof row.value, 'number', `${key} row value is not numeric for ${row.date}`);
      assert.ok(Number.isFinite(row.value), `${key} row value is not finite for ${row.date}`);
    }

    assertRecentDate(rows[rows.length - 1].date, 120);
  }
});

test('SingStat M183741 includes Unit labour cost of construction with numeric ascending monthly data', async (t) => {
  let bundle;
  try {
    bundle = await fetchUnitLabourCostConstructionSeries({ tableId: 'M183741' });
  } catch (err) {
    if (shouldSkipLive(err)) t.skip(`live SingStat unavailable: ${err.message}`);
    throw err;
  }

  const rows = bundle.UNIT_LABOUR_COST_CONSTRUCTION?.rows;
  assert.ok(Array.isArray(rows) && rows.length > 0, 'unit labour cost rows are missing/empty');
  assert.ok(isMonotonicAscending(rows), 'unit labour cost dates are not monotonic ascending');

  for (const row of rows) {
    assert.match(row.date, /^\d{4}-\d{2}-\d{2}$/);
    assert.equal(typeof row.value, 'number');
    assert.ok(Number.isFinite(row.value));
  }

  assertRecentDate(rows[rows.length - 1].date, 200);
});


test('SingStat M015792 includes Construction GDP (seasonally adjusted) with numeric ascending quarterly data', async (t) => {
  let bundle;
  try {
    bundle = await fetchConstructionGdpSeries({ tableId: 'M015792' });
  } catch (err) {
    if (shouldSkipLive(err)) t.skip(`live SingStat unavailable: ${err.message}`);
    throw err;
  }

  const rows = bundle.CONSTRUCTION_GDP_SA?.rows;
  assert.ok(Array.isArray(rows) && rows.length > 0, 'construction GDP rows are missing/empty');
  assert.ok(isMonotonicAscending(rows), 'construction GDP dates are not monotonic ascending');

  for (const row of rows) {
    assert.match(row.date, /^\d{4}-\d{2}-\d{2}$/);
    assert.equal(typeof row.value, 'number');
    assert.ok(Number.isFinite(row.value));
  }

  assertRecentDate(rows[rows.length - 1].date, 200);
});
