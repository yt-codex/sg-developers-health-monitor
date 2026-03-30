const test = require('node:test');
const assert = require('node:assert/strict');

const {
  parseTableBuilderHierarchyRows,
  indexTableBuilderHierarchyRows,
  hierarchyRowToSeriesValues,
  hierarchyRowLatest
} = require('../scripts/lib/singstat_tablebuilder');

test('hierarchy parser preserves series numbers and quarterly period formatting', () => {
  const payload = {
    Data: {
      row: [
        {
          seriesNo: '2',
          rowText: 'Total Non-Landed Properties',
          uoM: 'Number Of Units',
          columns: [
            { key: '2025 3Q', value: '46787' },
            { key: '2025 4Q', value: '47358' }
          ]
        },
        {
          seriesNo: '2.1',
          rowText: 'Under Construction',
          uoM: 'Number Of Units',
          columns: [
            { key: '2025 3Q', value: '28020' },
            { key: '2025 4Q', value: '28903' }
          ]
        }
      ]
    }
  };

  const rows = parseTableBuilderHierarchyRows(payload);
  const indexed = indexTableBuilderHierarchyRows(rows);
  const total = indexed.bySeriesNo.get('2');
  const underConstruction = indexed.bySeriesNo.get('2.1');

  assert.equal(total.rowText, 'Total Non-Landed Properties');
  assert.equal(underConstruction.rowText, 'Under Construction');
  assert.deepEqual(hierarchyRowToSeriesValues(total), [
    { period: '20253Q', value: 46787 },
    { period: '20254Q', value: 47358 }
  ]);
  assert.deepEqual(hierarchyRowLatest(underConstruction), { period: '20254Q', value: 28903 });
});

test('hierarchy index groups duplicate rowText labels without clobbering seriesNo lookup', () => {
  const payload = {
    Data: {
      row: [
        { seriesNo: '1', rowText: 'Total Office Space', columns: [{ key: '2025 4Q', value: '867' }] },
        { seriesNo: '1.1', rowText: 'Under Construction', columns: [{ key: '2025 4Q', value: '531' }] },
        { seriesNo: '4', rowText: 'Total Business Park Space', columns: [{ key: '2025 4Q', value: '25' }] },
        { seriesNo: '4.1', rowText: 'Under Construction', columns: [{ key: '2025 4Q', value: '25' }] }
      ]
    }
  };

  const indexed = indexTableBuilderHierarchyRows(parseTableBuilderHierarchyRows(payload));

  assert.equal(indexed.bySeriesNo.get('1.1').rowText, 'Under Construction');
  assert.equal(indexed.bySeriesNo.get('4.1').rowText, 'Under Construction');
  assert.equal((indexed.byRowText.get('Under Construction') || []).length, 2);
});
