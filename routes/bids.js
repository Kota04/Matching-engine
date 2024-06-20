const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const redisClient = require('../redis/index');
const router = express.Router();
const binarySearch = require('binary-search');
const _ = require('lodash');

router.post('/bid', async (req, res) => {
    const data = _.get(req, 'body');

    const key = `${_.get(data, 'channel_id')}_lock`; // Redis key for the lock
    
    try {
        // Ensure Redis client is connected
        await redisClient.connect();
        
        let val = await redisClient.get(key);
        while(val != null) {
            val = await redisClient.get(key);
            await new Promise(resolve => setTimeout(resolve, 10));
        }

        await redisClient.set(key, 'locked'); // Acquire the lock
        const node = new Node(
            0, 
            _.get(data, 'user_id'),
            _.get(data, 'channel_id'),
            _.get(data, 'price_per_unit'), 
            _.get(data, 'units'), 
            _.get(data, 'trade_type'));

        // Get current buy/sell arrays from Redis
        const value = await redisClient.get(_.get(data, 'channel_id'));
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
    } finally {
        // Release the lock
        await redisClient.del(key);
        redisClient.quit();
    }
});

async function processBuyOrder(node, buy, sell) {
    if (_.size(sell) === 0 || _.get(sell, '[0].price') > _.get(node, 'price')) {
        buy = insertSorted(buy, node, (a, b) => _.get(b, 'price') - _.get(a, 'price'), 10);
        await redisClient.set(_.get(node, 'channel_id'), JSON.stringify([buy, sell]));
    } else {
        await matchOrders(node, sell, buy, 0);    
    }
}

async function processSellOrder(node, buy, sell) {
    if (_.size(buy) === 0 || _.get(buy, '[0].price') < _.get(node, 'price')) {
        sell = insertSorted(sell, node, (a, b) => _.get(a, 'price') - _.get(b, 'price'), 10);
        await redisClient.set(_.get(node, 'channel_id'), JSON.stringify([buy, sell]));
    } else {
        await matchOrders(node, buy, sell, 1);
    }
}

function insertSorted(array, item, comparator, limit) {
    let index = binarySearch(array, item, comparator);
    if (index < 0) {
        index = ~index;
    }
    array.splice(index, 0, item);
    if (_.size(array) > limit) {
        array.pop();
    }
    return array;
}

async function matchOrders(node, oppositeArray, sameArray, tradeType) {
    while (_.size(oppositeArray) > 0 && 
           (tradeType === 0 ? _.get(oppositeArray, '[0].price') <= _.get(node, 'price') : _.get(oppositeArray, '[0].price') >= _.get(node, 'price')) && 
           _.get(node, 'quantity') > 0) {
        
        let oppositeNode = _.get(oppositeArray, '[0]');

        if (_.get(node, 'quantity') >= _.get(oppositeNode, 'quantity')) {
            _.update(node, 'quantity', n => n - _.get(oppositeNode, 'quantity'));
            await insertTrade(node, oppositeNode, _.get(oppositeNode, 'quantity'), _.get(oppositeNode, 'price'), tradeType);
            _.set(oppositeNode, 'quantity', 0); // Using lodash to set quantity to 0
            await updateBidStatus(oppositeNode.id, 1, 0); // update status to completed
            oppositeArray.shift();
        } else {
            _.update(oppositeNode, 'quantity', n => n - _.get(node, 'quantity'));
            await insertTrade(node, oppositeNode, node.quantity, _.get(oppositeNode, 'price'), tradeType);
            _.set(node, 'quantity', 0); // Using lodash to set quantity to 0
            await updateBidStatus(oppositeNode.id, 0, _.get(oppositeNode, 'quantity')); // update status to partially completed
        }
    }
    
    // updating the current node
    if (_.get(node, 'quantity') > 0) {
        const insertQuery = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time) 
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) 
        RETURNING id`;
        const values = [_.get(node, 'user_id'), _.get(node, 'channel_id'), _.get(node, 'quantity'), _.get(node, 'price'), _.get(node, 'type')];
        const res = await db.query(insertQuery, values);
        _.set(node, 'id', _.get(res, 'rows[0].id'));
        sameArray = insertSorted(sameArray, node, (a, b) => tradeType === 0 ? _.get(b, 'price') - _.get(a, 'price') : _.get(a, 'price') - _.get(b, 'price'), 10);
    }

    const query = tradeType === 0
        ? 'SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 1 ORDER BY price_per_unit ASC, time ASC LIMIT 10'
        : 'SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 0 ORDER BY price_per_unit DESC, time ASC LIMIT 10';
    const values = [_.get(node, 'channel_id')];
    const res = await db.query(query, values);
    oppositeArray = _.map(_.get(res, 'rows'), row => new Node(row.id, row.user_id, row.channel_id, row.price, row.quantity, row.type));

    await redisClient.set(_.get(node, 'channel_id'), JSON.stringify(tradeType === 0 ? [sameArray, oppositeArray] : [oppositeArray, sameArray]));
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
        ? [_.get(sellNode, 'id'), _.get(buyNode, 'user_id'), _.get(sellNode, 'user_id'), _.get(sellNode, 'channel_id'), quantity, _.get(sellNode, 'price')]
        : [_.get(buyNode, 'id'), _.get(sellNode, 'user_id'), _.get(buyNode, 'user_id'), _.get(buyNode, 'channel_id'), quantity, _.get(buyNode, 'price')];

    await db.query(query, values);
}

module.exports = router;
