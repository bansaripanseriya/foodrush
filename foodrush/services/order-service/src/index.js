const express = require('express');
const { Pool } = require('pg');
const { kafkaClient } = require('@foodrush/shared/kafka/client');
const { TOPICS } = require('@foodrush/shared/kafka/topics');

const app = express();
app.use(express.json());

// PostgreSQL connection pool
const pool = new Pool({connectionString : process.env.DATABASE_URL});

// Kafka client
const kafkaClient = new kafkaClient('order-service', process.env.KAFKA_BROKERS);
let producer;

(async () => {producer = await kafkaClient.createProducer(); })();

// Order state machine - valid transitions
const STATE_TRANSITIONS = {
    pending: ['confirmed', 'cancelled'],
    confirmed: ['preparing', 'cancelled'],
    preparing: ['ready', 'cancelled'],
    ready: ['picked_up', 'cancelled'],
    picked_up: ['delivered', 'cancelled'],
    delivered: [],
    cancelled: [],
};

// ---- CREATE ORDER ----
app.post('/api/orders', async (req, res) => {
    const {userId, restaurantId, items, deliveryAddress} = req.body;

    // Calculate total
    const total = items.reduce((sum, item) => sum + (item.price * item.quantity), 0);

    const client = await pool.connect();
    try{
        await client.query('BEGIN');

        // Insert order
        const oredrResult = await client.query(
            `INSERT INTO orders (user_id, restaurant_id, status, total, delivery_address, created_at)
            VALUES ($1, $2, 'pending', $3, $4, NOW()) RETURNING *`,
            [userId, restaurantId, total, JSON.stringify(deliveryAddress)]
        );

        await client.query('COMMIT');

        // Publish to Kafka - triggers restaurant notification, payment, delivery assignment
        await producer.send({
            topic: TOPICS.ORDER_CREATED,
            messages: [{
                key: order.id.toString(),
                value: JSON.stringify({
                    'orderId': order.id,
                    userId, 
                    restaurantId, 
                    items, total, 
                    deliveryAddress,
                    createdAt: order.created_at,
                }),
                headers: {
                    source: 'order-service',
                }
            }]
        });

        res.status(201).json(order);
    } catch (error) {
        console.error('Error creating order:', error);
        await client.query('ROLLBACK');
        res.status(500).json({error: 'Internal server error'});
    } finally {
        if(client) { await client.release(); }
    }
});

// ---- UPDATE ORDER STATUS ----
app.patch('/api/orders/:orderId/status', async (req, res) => {
    const { orderId } = req.params;
    const { status, updatedBy } = req.body;

    const orderResult = await pool.query(
        'SELECT * FROM orders WHERE id = $1',
        [orderId]
    );

    const order = ordereResult[0];

    if(!order) { return res.status(404).json({error: 'Order not found'}); }

    const allowedNext = STATE_TRANSITIONS[order.status] || [];
    if(!allowedNext.includes(status)) {
        return res.status(400).json({
            error: `Cannot transition from ${order.status} to ${status}`,
            allowed: allowedNext,
        })
    }

    const client = await pool.connect();
    try{
        await client.query('BEGIN');

        await pool.query(
        'UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2',
        [status, orderId]
    );

    // Every status change is an event - deliver and notification service react
    await producer.send({
        topic: TOPICS.ORDER_STATUS_UPDATED,
        messages: [{
            key: orderId.toString(),
            value: JSON.stringify({
                orderId,
                previousStatus: order.status,
                status,
                updatedBy,
            }),
            headers: {
                source: 'order-service',
            }
        }],
    });

    res.json({orderId, status});
    } catch (error) {
        console.error('Error updating order status:', error);
        await pool.query('ROLLBACK');
        res.status(500).json({error: 'Internal server error'});
    } finally {
        if(client) { await client.release(); }
    }
    
});

app.listen(3003, () => console.log('Order service is running on port 3003'));