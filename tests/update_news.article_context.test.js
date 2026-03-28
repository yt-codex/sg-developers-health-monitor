const test = require('node:test');
const assert = require('node:assert/strict');

const developerConfig = require('../config/developers.json');
const relevanceRules = require('../config/relevance_rules.json');
const {
  buildArticleAnalysisContext,
  evaluateRelevance,
  extractArticleContextFromHtml,
  resolveDevelopersFromContext
} = require('../scripts/update_news');

test('extractArticleContextFromHtml captures Business Times article paragraphs', () => {
  const html = `
    <html>
      <head>
        <meta property="og:description" content="First parcel released in new Dover-Medway estate for a 625-unit project draws six bids">
      </head>
      <body>
        <p data-testid="article-paragraph-component">
          The top bid of S$1,556 per sq ft per plot ratio was placed by a consortium that includes Forsea Holdings, Qingjian Realty and Jianan Capital.
        </p>
        <p data-testid="article-paragraph-component">
          The one-north site could yield about 625 homes and drew six bids at tender close.
        </p>
      </body>
    </html>
  `;

  const context = extractArticleContextFromHtml(html, {
    publisher: 'The Business Times',
    url: 'https://www.businesstimes.com.sg/property/test-story'
  });

  assert.ok(context);
  assert.equal(context.method, 'publisher_paragraphs');
  assert.equal(context.paragraphs.length, 2);
  assert.match(context.text, /Forsea Holdings/);
  assert.match(context.text, /one-north site could yield about 625 homes/i);
});

test('extractArticleContextFromHtml captures Straits Times article paragraphs', () => {
  const html = `
    <html>
      <body>
        <p data-testid="article-paragraph-annotation-test-id">
          GuocoLand sold more than 90 per cent of the River Modern units on launch day, with buyers drawn to the project location.
        </p>
        <p data-testid="article-paragraph-annotation-test-id">
          Analysts said the take-up rate reflects sustained demand for new homes in Singapore.
        </p>
      </body>
    </html>
  `;

  const context = extractArticleContextFromHtml(html, {
    publisher: 'The Straits Times',
    url: 'https://www.straitstimes.com/property/test-story'
  });

  assert.ok(context);
  assert.equal(context.method, 'publisher_paragraphs');
  assert.equal(context.paragraphs.length, 2);
  assert.match(context.text, /GuocoLand sold more than 90 per cent/i);
});

test('resolveDevelopersFromContext normalizes subsidiary phrasing to the parent developer', () => {
  const text =
    'Alpha Residential Pte Ltd, a unit of Qingjian Realty, submitted the top bid for the one-north residential site in Singapore.';

  const resolution = resolveDevelopersFromContext(text, developerConfig);

  assert.deepEqual(resolution.developers, ['Qingjian Realty']);
  assert.equal(resolution.primary_developer, 'Qingjian Realty');
  assert.ok(resolution.raw_entities.includes('Alpha Residential Pte Ltd'));
  assert.equal(resolution.relationships.length, 1);
  assert.equal(resolution.relationships[0].parent, 'Qingjian Realty');
});

test('article context upgrades a generic land-bid headline into a developer match', () => {
  const articleContext = {
    url: 'https://www.businesstimes.com.sg/property/test-story',
    publisher: 'The Business Times',
    method: 'publisher_paragraphs',
    description: 'First parcel released in new Dover-Medway estate for a 625-unit project draws six bids',
    paragraphs: [
      'The top bid was placed by a consortium that includes Forsea Holdings, Qingjian Realty and Jianan Capital.',
      'The one-north parcel in Singapore could yield about 625 homes.'
    ]
  };

  const analysis = buildArticleAnalysisContext(
    {
      title: 'Top bid sets benchmark for residential land in one-north area',
      snippet: 'The tender drew six bids.',
      query: ''
    },
    articleContext,
    developerConfig
  );
  const relevance = evaluateRelevance(analysis.combined, developerConfig, relevanceRules, {
    contextText: analysis.context_text,
    developerMatches: analysis.developer_resolution.developer_matches
  });

  assert.equal(relevance.pass, true);
  assert.equal(relevance.relevance_reason, 'developer_match');
  assert.equal(analysis.developer_resolution.primary_developer, 'Multiple');
  assert.deepEqual(analysis.developer_resolution.developers, ['Forsea', 'Qingjian Realty']);
});
