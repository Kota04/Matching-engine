const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const client = require('../redis/index');
const router = express.Router();
const redisClient = client.duplicate();

router.post('/bid', async (req, res) => {
    const data = req.body;
    const key = `${data.channel_id}_lock`; // Redis key for the lock
    // Validate input data
    if (!data.channel_id || !data.user_id || typeof data.price !== 'number' || typeof data.quantity !== 'number' || typeof data.type !== 'number') {
        return res.status(400).json({ error: 'Invalid input data' });
    }
    try {
        // Ensure Redis client is connected
        if (!redisClient.isOpen) await redisClient.connect();
        
        let val = await redisClient.get(key);
        while(val!=null){
            val = await redisClient.get(key);
            await new Promise(resolve => setTimeout(resolve, 10));
        }

        await redisClient.set(key, 'locked'); // Acquire the lock
        const node = new Node(0, data.user_id, data.channel_id, data.price, data.quantity, data.type);

        // Get current buy/sell arrays from Redis
        const value = await redisClient.get(node.channel_id);
        let buy = [], sell = [];

        if (value) {
            [buy, sell] = JSON.parse(value);
        }
        // Process the order based on its type
        if (node.type === 0) {
            await processBuyOrder(node, buy, sell);
        } else {
            await processSellOrder(node, buy, sell);
        }
        // Save updated buy/sell arrays back to Redis
        res.status(201).json(node);
    } catch (error) {
        console.error('Error processing bid:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
    finally{
        // Release the lock
        await redisClient.del(key);
        redisClient.quit();
    }
});

async function processBuyOrder(node, buy, sell) {
    if (sell.length === 0 || sell[0].price > node.price) {
       buy=insertSorted(buy, node, (a, b) => b.price - a.price, 10);
       await redisClient.set(node.channel_id, JSON.stringify([buy, sell]));
    } else {
        {
            await matchOrders(node, sell, buy, 0);
    }
    
}
}

async function processSellOrder(node, buy, sell) {
    if (buy.length === 0 || buy[0].price < node.price) {
        sell=insertSorted(sell, node, (a, b) => a.price - b.price, 10);
        await redisClient.set(node.channel_id, JSON.stringify([buy, sell]));
    } else {
        await matchOrders(node, buy, sell, 1);
    }
}

function insertSorted(array, item, comparator, limit) {
    let index = 0;
    while (index < array.length && comparator(array[index], item) < 0) {
        index++;
    }
    array.splice(index, 0, item);
    if (array.length > limit) {
        array.pop();
    }
    return array;
}

async function matchOrders(node, oppositeArray, sameArray, tradeType) {
    while (oppositeArray.length > 0 && (tradeType === 0 ? oppositeArray[0].price <= node.price : oppositeArray[0].price >= node.price) && node.quantity > 0) {
        const oppositeNode = oppositeArray[0];

        if (node.quantity >= oppositeNode.quantity) {
            node.quantity -= oppositeNode.quantity;
            await insertTrade(node, oppositeNode, oppositeNode.quantity, oppositeNode.price, tradeType);
            oppositeNode.quantity = 0;
            await updateBidStatus(oppositeNode.id, 1, 0); // update status to completed
            oppositeArray.shift();
        } else {
            oppositeNode.quantity -= node.quantity;
            await insertTrade(node, oppositeNode, node.quantity, oppositeNode.price, tradeType);
            node.quantity = 0;
            await updateBidStatus(oppositeNode.id, 0, oppositeNode.quantity); // update status to partially completed
        }
    }
    
    // updating the current node
    if (node.quantity > 0) {
        const insertQuery = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time) 
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) 
        RETURNING id`;
        const values = [node.user_id, node.channel_id, node.quantity, node.price, node.type];
        const res = await db.query(insertQuery, values);
        node.id = res.rows[0].id;
        sameArray = insertSorted(sameArray, node, (a, b) => tradeType === 0 ? b.price - a.price : a.price - b.price, 10);
    }
    // repopulating the opposite arrays
    if(tradeType === 0) 
        {
            const query = 'SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 1 ORDER BY price_per_unit ASC ORDER BY time ASC LIMIT 10';
            const values = [oppositeNode.channel_id];
            const res = await db.query(query, values);
            oppositeArray = res.rows.map(row => new Node(row.id, row.user_id, row.channel_id, row.price, row.quantity, row.type));
            await redisClient.set(node.channel_id, JSON.stringify([sameArray, oppositeArray])); // updating the redis cache
        }
    else
    {
        const query = 'SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 0 ORDER BY price_per_unit DESC ORDER BY time ASC LIMIT 10';
        const values = [oppositeNode.channel_id];
        const res = await db.query(query, values);
        oppositeArray = res.rows.map(row => new Node(row.id, row.user_id, row.channel_id, row.price, row.quantity, row.type));
        await redisClient.set(node.channel_id, JSON.stringify([oppositeArray, sameArray])); // updating the redis cache
    }
   
}

async function updateBidStatus(bidId, status, units) {
    const query = `UPDATE bids SET units = $1, status = $2 WHERE id = $3`;
    const values = [units, status, bidId];
    await db.query(query, values);
}

async function insertTrade(buyNode, sellNode, quantity, price, tradeType) {
    const query = `
        INSERT INTO trades (bid_id, buyer_id, seller_id, channel_id, units, price_per_unit, time)
        VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);
    `;
    const values = tradeType === 0
        ? [sellNode.id, buyNode.user_id, sellNode.user_id, sellNode.channel_id, quantity, price]
        : [buyNode.id, buyNode.user_id, sellNode.user_id, buyNode.channel_id, quantity, price];

    await db.query(query, values);
}

module.exports = router;
