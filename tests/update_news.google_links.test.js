const test = require('node:test');
const assert = require('node:assert/strict');

const {
  decodeGoogleLinkCandidate,
  isHomepageLikeUrl,
  isLikelyArticleUrl,
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
