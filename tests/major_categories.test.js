const test = require('node:test');
const assert = require('node:assert/strict');
const macroData = require('../data/macro_indicators.json');
const {
  ALLOWED_MAJOR_CATEGORIES,
  EXPECTED_SERIES_IDS_49,
  SERIES_MAJOR_CATEGORY_MAP,
  getMajorCategory
} = require('../scripts/lib/major_categories');

test('major category map covers exactly the authoritative 49 indicator IDs', () => {
  assert.equal(EXPECTED_SERIES_IDS_49.length, 49);
  const mappedIds = Object.keys(SERIES_MAJOR_CATEGORY_MAP).sort();
  const expectedIds = [...EXPECTED_SERIES_IDS_49].sort();
  assert.deepEqual(mappedIds, expectedIds);
});

test('major category values are limited to allowed enum', () => {
  for (const seriesId of EXPECTED_SERIES_IDS_49) {
    const category = getMajorCategory(seriesId);
    assert.ok(category, `Expected category for ${seriesId}`);
    assert.ok(
      ALLOWED_MAJOR_CATEGORIES.includes(category),
      `Invalid category ${category} for ${seriesId}`
    );
  }
});

test('data output includes major_category for all authoritative 49 indicators', () => {
  const series = macroData.macro_indicators.series;
  for (const seriesId of EXPECTED_SERIES_IDS_49) {
    assert.ok(series[seriesId], `Series ${seriesId} missing from data output`);
    assert.equal(series[seriesId].major_category, getMajorCategory(seriesId));
    assert.ok(ALLOWED_MAJOR_CATEGORIES.includes(series[seriesId].major_category));
  }
});
