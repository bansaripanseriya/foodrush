// services/order-service/tests/order.test.js
const request = require('supertest');
const { Pool } = require('pg');
const app = require('../src/app');

// Integration test — needs a real test database
describe('Order Service', () => {
  let pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: process.env.TEST_DATABASE_URL });
    await pool.query('DELETE FROM orders');  // clean slate
  });

  afterAll(() => pool.end());

  describe('POST /api/orders', () => {
    it('creates an order and returns it', async () => {
      const res = await request(app)
        .post('/api/orders')
        .set('x-user-id', 'test-user-1')  // gateway injects this
        .send({
          restaurantId: 'restaurant-123',
          items: [{ menuItemId: 'item-1', name: 'Biryani', price: 250, quantity: 2 }],
          deliveryAddress: { lat: 28.6139, lng: 77.2090, text: 'New Delhi' },
        });

      expect(res.status).toBe(201);
      expect(res.body.status).toBe('pending');
      expect(res.body.total).toBe(500);
    });
  });

  describe('State machine', () => {
    it('rejects invalid state transitions', async () => {
      // Create an order first
      const createRes = await request(app)
        .post('/api/orders')
        .set('x-user-id', 'test-user-1')
        .send({ /* ... */ });

      // Can't go from pending → delivered
      const updateRes = await request(app)
        .patch(`/api/orders/${createRes.body.id}/status`)
        .send({ status: 'delivered' });

      expect(updateRes.status).toBe(400);
      expect(updateRes.body.error).toContain('Cannot transition');
    });
  });
});