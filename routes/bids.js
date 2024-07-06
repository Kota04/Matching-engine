const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const { cache, getAsync, setAsync, deleteAsync } = require('../redis/index');
const _ = require('lodash');

const router = express.Router();

router.use(express.json());  // Middleware to parse JSON bodies

// Function to handle POST requests to /bids
router.post("/", async (req, res) => {
  const data = _.get(req, 'body');
  const channelId = _.get(data, 'channel_id');
  const userId = _.get(data, 'user_id');
  const key = `${channelId}_lock`;

  try {
    // Critical section begins
    let val = await getAsync(key);
    while (val === 'Locked') {
      await new Promise(r => setTimeout(r, 5));
      val = await getAsync(key);
    }

    await setAsync(key, 'Locked');

    const currNode = new Node(
      0, userId, channelId, 
      _.get(data, 'price_per_unit'), 
      _.get(data, 'units'), 
      _.get(data, 'trade_type')
    );

    let [buy, sell] = await getOrderQueues(currNode.channel_id);

    let result;
    if (_.get(currNode, 'trade_type') === false) {
      result = await processBuyOrder(currNode, sell);
    } else {
      result = await processSellOrder(currNode, buy);
    }

    if (result !== -1) {
      _.set(currNode, 'units', result);
    }

    if (result >= 0) {
      if (_.get(currNode, 'trade_type') === false) {
        sell = await refillQueue(currNode, 'sell');
      } else {
        buy = await refillQueue(currNode, 'buy');
      }
    }

    if (result !== 0) {
      const node1 = await insertBid(currNode);
      if (_.get(currNode, 'trade_type') === false) {
        buy = insertSorted(buy, node1, (a, b) => _.get(b, 'price') - _.get(a, 'price'), 10);
      } else {
        sell = insertSorted(sell, node1, (a, b) => _.get(b, 'price') - _.get(a, 'price'), 10);
      }
    }

    await setAsync(channelId, JSON.stringify([buy, sell]));
    // Critical section ends
    await deleteAsync(key);
    return res.status(200).send();

  } catch (err) {
    console.error(err);
    await deleteAsync(key);
    return res.status(500).send(err);
  }
});

const getOrderQueues = async (channelId) => {
  const queueData = await getAsync(channelId);
  return queueData ? JSON.parse(queueData) : [[], []];
};

const processBuyOrder = async (currNode, sellArray) => {
  if (_.isEmpty(sellArray) || _.get(currNode, 'price_per_unit') < _.get(sellArray, '[0].price_per_unit')) {
    return -1;
  } else {
    return await matchOrder(currNode, sellArray);
  }
};

const processSellOrder = async (currNode, buyArray) => {
  if (_.isEmpty(buyArray) || _.get(currNode, 'price_per_unit') > _.get(buyArray, '[0].price_per_unit')) {
    return -1;
  } else {
    return await matchOrder(currNode, buyArray);
  }
};

const matchOrder = async (currNode, memQueue) => {
  while (_.size(memQueue) > 0 && 
         (_.get(currNode, 'trade_type') === false 
            ? _.get(memQueue, '[0].price_per_unit') <= _.get(currNode, 'price_per_unit') 
            : _.get(memQueue, '[0].price_per_unit') >= _.get(currNode, 'price_per_unit')) && 
         _.get(currNode, 'units') > 0) {
           
    if (_.get(currNode, 'units') >= _.get(memQueue, '[0].units')) {
      _.update(currNode, 'units', units => units - _.get(memQueue, '[0].units'));
      await insertTrade(currNode, memQueue[0], _.get(memQueue, '[0].units'));
      await updateBidStatus(_.get(memQueue, '[0].id'), true, 0);
      memQueue.shift();
    } else {
      _.update(memQueue, '[0].units', units => units - _.get(currNode, 'units'));
      await insertTrade(currNode, memQueue[0], _.get(currNode, 'units'));
      await updateBidStatus(_.get(memQueue, '[0].id'), false, _.get(memQueue, '[0].units'));
      return 0;
    }
  }
  return _.get(currNode, 'units');
};

const updateBidStatus = async (bidId, status, units) => {
  const query = 'UPDATE bids SET status = $1, units = $2 WHERE id = $3';
  const values = [status, units, bidId];
  return await db.query(query, values);
};

const insertTrade = async (currNode, inMemoryNode, units) => {
  const buyerId = _.get(currNode, 'trade_type') === false ? _.get(currNode, 'user_id') : _.get(inMemoryNode, 'user_id');
  const sellerId = _.get(currNode, 'trade_type') === false ? _.get(inMemoryNode, 'user_id') : _.get(currNode, 'user_id');
  const bidId = _.get(inMemoryNode, 'id');
  const channelId = _.get(currNode, 'channel_id');

  const query = `
    INSERT INTO trades (bid_id, buyer_id, seller_id, channel_id, units, price_per_unit, time)
    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);`;
  const values = [bidId, buyerId, sellerId, channelId, units, _.get(currNode, 'trade_type') === false ? _.get(inMemoryNode, 'price_per_unit') : _.get(currNode, 'price_per_unit')];

  return await db.query(query, values);
};

const insertSorted = (array, value, comparator, limit) => {
  const index = _.sortedIndexBy(array, value, comparator);
  array.splice(index, 0, value);
  if (_.size(array) > limit) {
    array.pop();
  }
  return array;
};

const refillQueue = async (currNode, queueType) => {
  const selectQuery = queueType === 'sell'
    ? `SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type 
       FROM bids WHERE channel_id = $1 AND status = false AND trade_type = true 
       ORDER BY price_per_unit ASC, time ASC LIMIT 10`
    : `SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type 
       FROM bids WHERE channel_id = $1 AND status = false AND trade_type = false 
       ORDER BY price_per_unit DESC, time ASC LIMIT 10`;
  const values = [_.get(currNode, 'channel_id')];
  const res = await db.query(selectQuery, values);
  const nodes = _.map(_.get(res, 'rows'), row => new Node(_.get(row, 'id'), _.get(row, 'user_id'), _.get(row, 'channel_id'), _.get(row, 'price'), _.get(row, 'quantity'), _.get(row, 'type')));
  return nodes;
};

const insertBid = async (currNode) => {
  const userId = _.get(currNode, 'user_id');
  const channelId = _.get(currNode, 'channel_id');

  const insertQuery = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time, status) 
                       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, false) 
                       RETURNING id`;
  const values = [userId, channelId, _.get(currNode, 'units'), _.get(currNode, 'price_per_unit'), _.get(currNode, 'trade_type')];
  const res = await db.query(insertQuery, values);
  _.set(currNode, 'id', _.get(res, 'rows[0].id'));

  return currNode;
};

module.exports = router;
