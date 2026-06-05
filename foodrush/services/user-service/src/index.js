const express = require('express');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { kafkaClient } = require('@foodrush/shared/kafka/client');
const { TOPICS } = require('@foodrush/shared/kafka/topics');
const redis = require('redis');

const app = express();
app.use(express.json());

// PostgreSQL connection pool
const pool = new Pool({connectionString : process.env.DATABASE_URL});

// Redis client for refresh tokens
const redisClient = redis.createClient({url: process.env.REDIS_URL});
await redisClient.connect();

// Kafka client
const kafkaClient = new kafkaClient('user-service', process.env.KAFKA_BROKERS);
let producer;

(async() => {
    producer = await kafkaClient.createProducer();
})();

//  ---- REGISTER USER ----
app.post('/api/users/register', async(req, res) => {
    const {email, password, name, phone} = req.body;

    try{
        // Check duplicate email
        const existing = await pool.query('SELECT id FROM users WHERE email = $1', [email]);
        if(existing.rows.length > 0) {
            return res.status(409).json({error: 'Email already exists'});
        }
        
        const passwordHash = await bcrypt.hash(password, 12);

        const result = await pool.query(
            `INSERT INTO users (email, password_hash, name, phone, created_at)
            VALUES ($1, $2, $3, $4, NOW()) RETURNING id, email, name`,
            [email, passwordHash, name, phone]
        );

        const user = result.rows[0];

        // Publish event so notification-service sends welcome email
        await producer.send({
            topic: 'user.registered',
            messages: [{
                key: user.id.toString(),
                value: JSON.stringify({
                    'userId': user.id, email, name
                }),
                headers: {
                    source: 'user-service',
                }
            }],
        });

        const {accessToken, refreshToken} = generateTokens(user);
        await redisClient.setEx(`refresh: ${user.id}`, JWT_REFRESH_EXPIRES_IN = 7 * 24 * 60 * 60, refreshToken);

        res.status(201).json({user, accessToken, refreshToken});
    } catch (error) {
        console.error('Error registering user:', error);
        res.status(500).json({error: 'Internal server error'});
    }
});

//  ---- LOGIN USER ----
app.post('/api/users/login', async(req, res) => {
    const {email, password} = req.body;

    try{
        const result = await pool.query(
            'SELECT id, email, name, password_hash FROM users WHERE email = $1', [email]
        );

        const user = result.rows[0];
        if(!user || !(await bcrypt.compare(password, user.password_hash))) {
            return res.status(401).json({error: 'Invalid credentials'});
        }

        const {accessToken, refreshToken} = generateTokens(user);
        await redisClient.setEx(`refresh: ${user.id}`, JWT_REFRESH_EXPIRES_IN = 7 * 24 * 60 * 60, refreshToken);

        res.status(200).json({user: {id: user.id, email: user.email, name: user.name}, accessToken, refreshToken});
    } catch (error) {
        console.error('Error logging in user:', error);
        res.status(500).json({error: 'Internal server error'});
    }
});

function generateTokens(user) {
    const accessToken = jwt.sign(
        { userId: user.id, email: user.email, role: user.role },
        process.env.JWT_SECRET,
        { expiresIn: '15m' }
    );
    const refreshToken = jwt.sign(
        { userId: user.id, type: 'refresh' },
        process.env.JWT_SECRET,
        { expiresIn: '7d' }
    );
    return { accessToken, refreshToken };
}

// ---- LOG OUT USER ----
app.post('/api/users/logout', async(req, res) => {
    const {refreshToken} = req.body;
    if(!refreshToken) {
        return res.status(400).json({error: 'Refresh token is required'});
    }
    
    try{
        const decoded = jwt.verify(refreshToken, process.env.JWT_SECRET);
        await redisClient.del(`refresh: ${decoded.userId}`);
        res.status(200).json({message: 'Logged out successfully'});
    } catch (error) {
        console.error('Error logging out user:', error);
        res.status(500).json({error: 'Internal server error'});
    }
});

// ---- REFRESH TOKEN ----
app.post('/api/users/refresh', async(req, res) => {
    const {refreshToken} = req.body;
    if(!refreshToken) {
        return res.status(400).json({error: 'Refresh token is required'});
    }
    
});

app.listen(3001, () => console.log('User service is running on port 3001'));