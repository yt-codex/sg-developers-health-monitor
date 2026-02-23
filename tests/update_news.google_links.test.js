const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildGoogleDedupKeys,
  buildTitleDateDedupKey,
  decodeGoogleLinkCandidate,
  isHomepageLikeUrl,
  isLikelyArticleUrl,
  normalizeTitleForDedup,
  resolveGoogleLink
} = require('../scripts/update_news');

test('decodeGoogleLinkCandidate extracts direct URL from url query parameter', () => {
  const candidate =
    'https://news.google.com/rss/articles/CBMi?url=https%3A%2F%2Fexample.com%2Fnews%2Fstory-123&utm_source=foo';
  const decoded = decodeGoogleLinkCandidate(candidate);
  assert.equal(decoded, 'https://example.com/news/story-123');
});

test('isLikelyArticleUrl rejects homepage-like paths and accepts article paths', () => {
  assert.equal(isLikelyArticleUrl('https://www.theedgesingapore.com/'), false);
  assert.equal(isLikelyArticleUrl('https://www.channelnewsasia.com/news'), false);
  assert.equal(isLikelyArticleUrl('https://www.straitstimes.com/business/property/cdl-wins-bid-12345'), true);
});



test('isHomepageLikeUrl treats Google rss/articles links as article candidates', () => {
  assert.equal(isHomepageLikeUrl('https://news.google.com/rss/articles/CBMiX2h0dHBzOi8vd3d3LnN0cmFpdHN0aW1lcy5jb20vYnVzaW5lc3MvcHJvcGVydHkvY2RsLXdpbnMtYmlkLTk4NzY1?oc=5'), false);
  assert.equal(isHomepageLikeUrl('https://news.google.com/home?hl=en-SG&gl=SG&ceid=SG:en'), true);
});

test('resolveGoogleLink falls back to original Google article link when source_url is homepage-like', async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => ({ url: 'https://www.theedgesingapore.com/' });

  try {
    const result = await resolveGoogleLink({
      link: 'https://news.google.com/rss/articles/CBMiXw?oc=5',
      source_url: 'https://www.theedgesingapore.com/'
    });
    assert.equal(result.resolved_link_status, 'fallback_google');
    assert.equal(result.resolved_link, 'https://news.google.com/rss/articles/CBMiXw?oc=5');
  } finally {
    global.fetch = originalFetch;
  }
});

test('resolveGoogleLink falls back to source_url when it is article-like', async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error('network unavailable in unit test');
  };

  try {
    const articleSourceResult = await resolveGoogleLink({
      link: 'https://news.google.com/rss/articles/CBMiYQ?oc=5',
      source_url: 'https://www.straitstimes.com/business/property/cdl-wins-bid-12345'
    });
    assert.equal(articleSourceResult.resolved_link_status, 'resolved');
    assert.equal(
      articleSourceResult.resolved_link,
      'https://www.straitstimes.com/business/property/cdl-wins-bid-12345'
    );
  } finally {
    global.fetch = originalFetch;
  }
});


test('normalizeTitleForDedup normalizes punctuation and ignorable articles', () => {
  assert.equal(
    normalizeTitleForDedup('CapitaLand Investment dragged into the red by China asset losses: What are the hits and misses?'),
    'capitaland investment dragged into red by china asset losses what are hits and misses'
  );
  assert.equal(
    normalizeTitleForDedup('Capitaland Investment dragged into red by China asset losses - What are hits and misses'),
    'capitaland investment dragged into red by china asset losses what are hits and misses'
  );
});

test('buildTitleDateDedupKey produces same key for near-identical titles on same date', () => {
  const date = '2026-02-23T07:46:00.000Z';
  const key1 = buildTitleDateDedupKey(
    'CapitaLand Investment dragged into the red by China asset losses: What are the hits and misses?',
    date
  );
  const key2 = buildTitleDateDedupKey(
    'Capitaland Investment dragged into red by China asset losses: What are the hits and misses?',
    date
  );
  assert.equal(key1, key2);
});

test('buildGoogleDedupKeys includes title-date dedup key for cross-source deduping', () => {
  const title = 'Capitaland Investment dragged into red by China asset losses: What are the hits and misses?';
  const pubDate = '2026-02-23T07:46:00.000Z';
  const titleDateKey = buildTitleDateDedupKey(title, pubDate);
  const keys = buildGoogleDedupKeys({
    title,
    pubDate,
    publisher: 'The Straits Times',
    resolved_link: 'https://news.google.com/rss/articles/ABC?oc=5',
    original_link: 'https://news.google.com/rss/articles/ABC?oc=5&hl=en-US'
  });
  assert.ok(keys.includes(titleDateKey));
});
