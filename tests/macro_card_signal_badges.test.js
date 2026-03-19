const test = require('node:test');
const assert = require('node:assert/strict');

const { buildCardSignalBadgesBySeriesId } = require('../assets/js/macro.js');

test('maps active macro signals to their driver cards only', () => {
  const badges = buildCardSignalBadgesBySeriesId({
    signals: {
      sector_performance: {
        status: 'Stress',
        series_id: 'construction_gdp',
        tooltip: 'Sector performance rule'
      },
      labour_cost: {
        status: 'Stress',
        series_id: 'unit_labour_cost_construction',
        tooltip: 'Labour cost rule'
      },
      interest_rate: {
        status: 'Normal',
        series_id: 'sora_overnight',
        tooltip: 'Interest rate rule'
      },
      materials_price: {
        status: 'Watch',
        details: {
          triggered_by: 'construction_material_cement_in_bulk_ordinary_portland_cement'
        },
        tooltip: 'Materials price rule'
      }
    }
  });

  assert.deepEqual(Object.keys(badges).sort(), [
    'construction_gdp',
    'construction_material_cement_in_bulk_ordinary_portland_cement',
    'unit_labour_cost_construction'
  ]);
  assert.deepEqual(badges.construction_gdp, [
    { label: 'Sector performance', status: 'Stress', tooltip: 'Sector performance rule' }
  ]);
  assert.deepEqual(badges.unit_labour_cost_construction, [
    { label: 'Labour cost', status: 'Stress', tooltip: 'Labour cost rule' }
  ]);
  assert.deepEqual(badges.construction_material_cement_in_bulk_ordinary_portland_cement, [
    { label: 'Material price', status: 'Watch', tooltip: 'Materials price rule' }
  ]);
  assert.equal(badges.sora_overnight, undefined);
});

test('supports legacy material_price key when mapping watch badges', () => {
  const badges = buildCardSignalBadgesBySeriesId({
    signals: {
      material_price: {
        status: 'Watch',
        details: {
          triggered_by: 'construction_material_ready_mixed_concrete'
        },
        tooltip: 'Legacy materials price rule'
      }
    }
  });

  assert.deepEqual(badges, {
    construction_material_ready_mixed_concrete: [
      { label: 'Material price', status: 'Watch', tooltip: 'Legacy materials price rule' }
    ]
  });
});
