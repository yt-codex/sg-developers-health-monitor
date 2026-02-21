const test = require('node:test');
const assert = require('node:assert/strict');
const { fetchSingStatRequiredSeries } = require('../scripts/lib/singstat_tablebuilder');

function isMonotonicAscending(items) {
  for (let i = 1; i < items.length; i += 1) {
    if (items[i - 1].date > items[i].date) return false;
  }
  return true;
}

test('SingStat TS/M700071 includes SORA + SGS 2Y + SGS 10Y with numeric ascending monthly data', async () => {
  const bundle = await fetchSingStatRequiredSeries({ tableId: 'TS/M700071' });

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

    const recent = rows.slice(-24);
    assert.ok(recent.length >= 12, `${key} has fewer than 12 recent monthly points`);
  }
});
