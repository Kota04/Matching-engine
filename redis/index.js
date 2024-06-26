const { createClient } = require('redis');

const PORT_REDIS = process.env.PORT_REDIS || 6379;

const cache = createClient({ url: `redis://localhost:${PORT_REDIS}` });

// Handle connection events
cache.on('error', (err) => {
  console.error('Redis client error:', err);
});

cache.on('connect', () => {
  console.log('Connected to Redis');
});

// Connect to Redis
async function connectRedis() {
  await cache.connect();
}

// Redis methods using native Promise-based API
async function getAsync(key) {
  return await cache.get(key);
}

async function setAsync(key, value) {
  return await cache.set(key, value);
}

async function deleteAsync(key) {
  return await cache.del(key);
}

module.exports = { cache, connectRedis, getAsync, setAsync, deleteAsync };
