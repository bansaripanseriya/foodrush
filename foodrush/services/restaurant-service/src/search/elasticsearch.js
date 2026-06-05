// services/restaurant-service/src/search/elasticsearch.js
const { Client } = require('@elastic/elasticsearch');

const esClient = new Client({ node: process.env.ELASTICSEARCH_URL });

const RESTAURANT_INDEX = 'restaurants';

// Create the index with proper mapping
async function createRestaurantIndex() {
  const exists = await esClient.indices.exists({ index: RESTAURANT_INDEX });
  if (exists) return;

  await esClient.indices.create({
    index: RESTAURANT_INDEX,
    body: {
      settings: {
        number_of_shards: 1,
        number_of_replicas: 0,
        analysis: {
          analyzer: {
            // Custom analyzer for restaurant/food names
            food_analyzer: {
              type: 'custom',
              tokenizer: 'standard',
              filter: ['lowercase', 'asciifolding', 'food_synonyms'],
            },
          },
          filter: {
            food_synonyms: {
              type: 'synonym',
              synonyms: [
                'biryani, briyani, biriyani',
                'pizza, pie',
                'burger, hamburger',
              ],
            },
          },
        },
      },
      mappings: {
        properties: {
          restaurantId:  { type: 'keyword' },
          name:          { type: 'text', analyzer: 'food_analyzer', fields: { keyword: { type: 'keyword' } } },
          cuisine:       { type: 'keyword' },
          description:   { type: 'text', analyzer: 'food_analyzer' },
          rating:        { type: 'float' },
          priceRange:    { type: 'integer' },  // 1=₹, 2=₹₹, 3=₹₹₹
          isOpen:        { type: 'boolean' },
          deliveryTime:  { type: 'integer' },  // minutes
          minOrder:      { type: 'float' },
          location: {
            type: 'geo_point',              // 🗺️ enables geo queries
          },
          menuItems: {
            type: 'nested',                 // search within array of menu items
            properties: {
              name:        { type: 'text', analyzer: 'food_analyzer' },
              description: { type: 'text' },
              price:       { type: 'float' },
              category:    { type: 'keyword' },
              tags:        { type: 'keyword' },
            },
          },
          tags:          { type: 'keyword' },
          updatedAt:     { type: 'date' },
        },
      },
    },
  });

  console.log(`[ES] Index '${RESTAURANT_INDEX}' created`);
}

// ─── SEARCH: the core query ────────────────────────────────
async function searchRestaurants({ query, lat, lng, radiusKm = 5, cuisine, maxPrice, page = 0, size = 20 }) {
  const must = [];
  const filter = [
    // Always filter to open restaurants
    { term: { isOpen: true } },
    // Geo-distance filter — only restaurants within radiusKm
    {
      geo_distance: {
        distance: `${radiusKm}km`,
        location: { lat, lon: lng },
      },
    },
  ];

  if (query) {
    // Multi-match across name, description, AND nested menu items
    must.push({
      bool: {
        should: [
          { multi_match: { query, fields: ['name^3', 'description', 'tags'], type: 'best_fields', fuzziness: 'AUTO' } },
          {
            nested: {
              path: 'menuItems',
              query: { multi_match: { query, fields: ['menuItems.name^2', 'menuItems.description', 'menuItems.tags'] } },
              inner_hits: { size: 3, _source: ['menuItems.name', 'menuItems.price'] },
            },
          },
        ],
      },
    });
  }

  if (cuisine)  filter.push({ term: { cuisine } });
  if (maxPrice) filter.push({ range: { priceRange: { lte: maxPrice } } });

  const response = await esClient.search({
    index: RESTAURANT_INDEX,
    body: {
      from: page * size,
      size,
      query: { bool: { must: must.length ? must : [{ match_all: {} }], filter } },
      sort: [
        // Sort by: relevance score first, then by distance, then by rating
        '_score',
        { _geo_distance: { location: { lat, lon: lng }, order: 'asc', unit: 'km' } },
        { rating: { order: 'desc' } },
      ],
    },
  });

  return {
    total: response.hits.total.value,
    restaurants: response.hits.hits.map(hit => ({
      ...hit._source,
      score: hit._score,
      matchedItems: hit.inner_hits?.menuItems?.hits?.hits?.map(i => i._source) || [],
    })),
  };
}

module.exports = { createRestaurantIndex, searchRestaurants, esClient, RESTAURANT_INDEX };