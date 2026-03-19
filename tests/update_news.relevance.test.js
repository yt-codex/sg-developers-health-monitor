const test = require('node:test');
const assert = require('node:assert/strict');

const developerConfig = require('../config/developers.json');
const relevanceRules = require('../config/relevance_rules.json');
const {
  cleanupExistingNews,
  evaluateRelevance
} = require('../scripts/update_news');

test('evaluateRelevance rejects unrelated Singapore launch and market headlines', () => {
  const cases = [
    'Singapore Exchange to launch Asian government bond futures amid geopolitical turmoil CEO Loh Boon Chye did not specify when the contracts would launch or which markets they would cover.',
    'Singapore to launch AI tool to flag high-risk patients for cardiovascular disease risk screening Doctors will be encouraged to use the new tool, which can predict a patient’s risk of developing diabetes or high cholesterol.',
    'There will be a Pokemon truck with Switch 2 game stations and sofas around Singapore till June To celebrate the launch of the new Pokemon Pokopia game on the Nintendo Switch 2, a travelling Pokemon truck will make its way across Singapore from Mar 5 to Jun 4.',
    'Pizza Hut Singapore launches Hut’s Sliders and revamps My Box meals for solo dining Solo dining is reimagined in Pizza Hut Singapore’s new launch.'
  ];

  for (const text of cases) {
    const relevance = evaluateRelevance(text.toLowerCase(), developerConfig, relevanceRules);
    assert.equal(relevance.pass, false);
  }
});

test('evaluateRelevance accepts Singapore property sentiment coverage', () => {
  const text = [
    'Global uncertainty weighs on Singapore property players in Q4: NUS poll',
    'Slowdown in economy is flagged as top risk amid robust housing demand at premium price points.'
  ].join(' ').toLowerCase();

  const relevance = evaluateRelevance(text, developerConfig, relevanceRules);
  assert.equal(relevance.pass, true);
  assert.equal(relevance.relevance_reason, 'sg_property_topic');
  assert.ok(relevance.relevance_terms.includes('property players'));
});

test('evaluateRelevance accepts Hoi Hup and Sunway MCL launch coverage via developer match', () => {
  const text = [
    'Hoi Hup-Sunway MCL to preview Pinery Residences mixed-use project in Tampines West from $2,340 psf',
    'The project is set to preview on March 14, with the launch scheduled for a fortnight later on March 28.'
  ].join(' ').toLowerCase();

  const relevance = evaluateRelevance(text, developerConfig, relevanceRules);
  assert.equal(relevance.pass, true);
  assert.equal(relevance.relevance_reason, 'developer_match');
  assert.ok(relevance.relevance_terms.includes('hoi hup') || relevance.relevance_terms.includes('sunway mcl'));
});

test('cleanupExistingNews reevaluates non-google items and removes direct-feed false positives', async () => {
  const items = [
    {
      id: 'ai-story',
      title: 'Singapore to launch AI tool to flag high-risk patients for cardiovascular disease risk screening',
      link: 'https://www.channelnewsasia.com/singapore/ai-diabetes-high-cholesterol-health-screening-woodlands-5972401',
      source: 'CNA',
      pubDate: '2026-03-05T03:07:00.000Z',
      severity: 'info',
      snippet: 'Doctors will be encouraged to use the new tool, which can predict a patient’s risk of developing diabetes or high cholesterol.'
    }
  ];

  const { cleaned, rejectedLogs } = await cleanupExistingNews(
    items,
    developerConfig,
    relevanceRules,
    [],
    ['warning', 'watch', 'info'],
    { allowed_publishers: [], aliases: {} }
  );

  assert.equal(cleaned.length, 0);
  assert.equal(rejectedLogs.length, 1);
  assert.equal(rejectedLogs[0].reason, 'cleanup_missing_property_developer_topic');
});

test('cleanupExistingNews preserves stored google items that already carry an allowlisted publisher', async () => {
  const items = [
    {
      id: 'pinery-google',
      title: 'Hoi Hup-Sunway MCL to preview Pinery Residences mixed-use project in Tampines West from $2,340 psf',
      original_link: 'https://news.google.com/rss/articles/ABC?oc=5',
      resolved_link: 'https://news.google.com/rss/articles/ABC?oc=5&hl=en-SG',
      resolved_link_status: 'resolved',
      source_url: 'https://www.edgeprop.sg',
      link: 'https://news.google.com/rss/articles/ABC?oc=5&hl=en-SG',
      source: 'google_news',
      aggregator: 'google_news',
      publisher: 'EdgeProp Singapore',
      pubDate: '2026-03-11T09:59:55.000Z',
      pubDate_sgt: '2026-03-11T17:59:55.000+08:00',
      developer: 'Multiple',
      severity: 'info',
      tags: [],
      snippet: 'Hoi Hup-Sunway MCL to preview Pinery Residences mixed-use project in Tampines West from $2,340 psf EdgeProp.sg',
      matched_terms: [],
      fetched_at_sgt: '2026-03-19T21:59:28.381+08:00',
      relevance_reason: 'developer_match',
      relevance_terms: ['hoi hup', 'sunway mcl']
    }
  ];

  const { cleaned, rejectedLogs } = await cleanupExistingNews(
    items,
    developerConfig,
    relevanceRules,
    [],
    ['warning', 'watch', 'info'],
    require('../config/google_news_publishers_allowlist.json')
  );

  assert.equal(rejectedLogs.length, 0);
  assert.equal(cleaned.length, 1);
  assert.equal(cleaned[0].publisher, 'EdgeProp Singapore');
});
