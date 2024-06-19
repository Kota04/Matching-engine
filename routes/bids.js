const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const client = require('../redis/index');

const router = express.Router();
const redisClient = client.duplicate();

router.post('/bid', async (req, res) => {
    const data = req.body;
    const query = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time) 
                   VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) 
                   RETURNING id`;
    const values = [data.user_id, data.channel_id, data.quantity, data.price, data.type];

    try {
        // Ensure Redis client is connected
        if (!redisClient.isOpen) await redisClient.connect();

        // Database insert operation
        const result = await db.query(query, values);
        const id = result.rows[0].id;

        // Create Node instance
        const node = new Node(id, data.user_id, data.channel_id, data.price, data.quantity, data.type);
        res.status(201).json(node);

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
        await redisClient.set(node.channel_id, JSON.stringify([buy, sell]));
    } catch (error) {
        console.error('Error processing bid:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

async function processBuyOrder(node, buy, sell) {
    if (sell.length === 0 || sell[0].price > node.price) {
        insertSorted(buy, node, (a, b) => b.price - a.price, 10);
    } else {
        await matchOrders(node, sell, buy, 0);
    }
}

async function processSellOrder(node, buy, sell) {
    if (buy.length === 0 || buy[0].price < node.price) {
        insertSorted(sell, node, (a, b) => a.price - b.price, 10);
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
}

async function matchOrders(node, oppositeArray, sameArray, tradeType) {
    while (oppositeArray.length > 0 && (tradeType === 0 ? oppositeArray[0].price <= node.price : oppositeArray[0].price >= node.price) && node.quantity > 0) {
        const oppositeNode = oppositeArray[0];

        if (node.quantity >= oppositeNode.quantity) {
            node.quantity -= oppositeNode.quantity;
            oppositeNode.quantity = 0;
            await updateBidStatus(oppositeNode.id, 1, 0); // update status to completed
            // call the trade function
            oppositeArray.shift();
        } else {
            oppositeNode.quantity -= node.quantity;
            node.quantity = 0;
            oppositeArray[0].quantity = oppositeNode.quantity;
            await updateBidStatus(oppositeNode.id, 0, oppositeNode.quantity); // update status to partially completed
            // call the trade function
        }
    }

    // updating the current node
    if (node.quantity > 0) {
        insertSorted(sameArray, node, (a, b) => tradeType === 0 ? b.price - a.price : a.price - b.price, 10);
        await updateBidStatus(node.id, 0, node.quantity);
    } else {
        await updateBidStatus(node.id, 1, 0);
    }
}

async function updateBidStatus(bidId, status, units) {
    const query = `UPDATE bids SET units = $1, status = $2 WHERE id = $3`;
    const values = [units, status, bidId];
    await db.query(query, values);
}

async function trade(buyer_id, seller_id,channel_id ,units, price) {
    // add the query for inserting the data into trades table
}
module.exports = router;
