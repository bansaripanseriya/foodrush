const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const redis = require('redis');
const { createClient } = require('redis');
const RedisStore = require('rate-limit-redis').default;

const app = express();
const redisClient = createClient({ url: process.env.REDIS_URL });
redisClient.connect();

// Rate limiting: 100 req/15min per IP - storde in Redis so it works across gateway instances
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100,
    standardHeaders: true,
    store: new RedisStore({ sendCommand: (...args) => redisClient.sendCommand(args) }),
    message: {error: 'Too many requests, please slow down'},
});

// Auth middleware - validates JWT and attacges user to request
function authenticate(req, res, next) {
    const token = req.headers.authorization?.split(' ')[1];
    if(!token) return res.status(401).json({error: 'Authentication required'});

    try{
        req.user = jwt.verify(token, process.env.JWT_SECRET);
        req.headers['x-user-id'] = req.user.userId;
        req.headers['x-user-role'] = req.user.role;
        next();
    } catch(err) {
        console.error('Error authenticating user:', err);
        return res.status(401).json({error: 'Invalid token'});
    }
}

// Public routes (no auth needed)
app.use('/api/users/register', limiter, createProxyMiddleware({ target: process.env.USER_SERVICE_URL, changeOrigin: true }));
app.use('/api/users/login',    limiter, createProxyMiddleware({ target: process.env.USER_SERVICE_URL, changeOrigin: true }));
app.use('/api/search',         limiter, createProxyMiddleware({ target: process.env.RESTAURANT_SERVICE_URL, changeOrigin: true }));

// Protected routes
app.use('/api/users',         limiter, authenticate, createProxyMiddleware({ target: process.env.USER_SERVICE_URL, changeOrigin: true }));
app.use('/api/restaurants',   limiter, authenticate, createProxyMiddleware({ target: process.env.RESTAURANT_SERVICE_URL, changeOrigin: true }));
app.use('/api/orders',        limiter, authenticate, createProxyMiddleware({ target: process.env.ORDER_SERVICE_URL, changeOrigin: true }));
app.use('/api/delivery',      limiter, authenticate, createProxyMiddleware({ target: process.env.DELIVERY_SERVICE_URL, changeOrigin: true }));

app.listen(3000, () => console.log('API Gateway on :3000'));