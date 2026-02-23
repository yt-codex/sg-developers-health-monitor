const test = require('node:test');
const assert = require('node:assert/strict');

const {
  decodeGoogleLinkCandidate,
  isLikelyArticleUrl,
  resolveGoogleLink
} = require('../scripts/update_news');

test('decodeGoogleLinkCandidate extracts direct URL from url query parameter', () => {
  const candidate =
    'https://news.google.com/rss/articles/CBMi?url=https%3A%2F%2Fexample.com%2Fnews%2Fstory-123&utm_source=foo';
  const decoded = decodeGoogleLinkCandidate(candidate);
  assert.equal(decoded, 'https://example.com/news/story-123');
});

test('isLikelyArticleUrl rejects domain roots and accepts deep article paths', () => {
  assert.equal(isLikelyArticleUrl('https://www.theedgesingapore.com/'), false);
  assert.equal(isLikelyArticleUrl('https://www.era.com.sg/'), false);
  assert.equal(isLikelyArticleUrl('https://www.straitstimes.com/business/property/cdl-wins-bid'), true);
});

test('resolveGoogleLink falls back to source_url only when it looks like a specific article URL', async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error('network unavailable in unit test');
  };

  try {
    const genericSourceResult = await resolveGoogleLink({
      link: 'https://news.google.com/rss/articles/CBMiXw?oc=5',
      source_url: 'https://www.theedgesingapore.com/'
    });
    assert.equal(genericSourceResult.resolution, 'fallback');
    assert.equal(genericSourceResult.resolved_link, 'https://news.google.com/rss/articles/CBMiXw?oc=5');

    const articleSourceResult = await resolveGoogleLink({
      link: 'https://news.google.com/rss/articles/CBMiYQ?oc=5',
      source_url: 'https://www.straitstimes.com/business/property/cdl-wins-bid'
    });
    assert.equal(articleSourceResult.resolution, 'source_url');
    assert.equal(
      articleSourceResult.resolved_link,
      'https://www.straitstimes.com/business/property/cdl-wins-bid'
    );
  } finally {
    global.fetch = originalFetch;
  }
});
