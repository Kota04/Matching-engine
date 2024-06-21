const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const { cache, getAsync, setAsync, deleteAsync } = require('../redis/index');
const binarySearch = require('binary-search');
const _ = require('lodash');

const router = express.Router();

// Function to handle POST requests to /bids
router.post("/buy", async (req, res) => {
  const data = _.get(req, 'body');
  const key = `${_.get(data, 'channel_id')}_lock`;
  
  try{
      let val = await getAsync(key);
      while(val)
      {
          await new Promise(r => setTimeout(r, 10));  
          val = await getAsync(key);
      }
      await setAsync(key, 'Locked');
      const currNode = new Node(0, _.get(data, 'user_id'), _.get(data, 'channel_id'), _.get(data, 'price_per_unit'), _.get(data, 'units'), _.get(data, 'trade_type'));
      let buy=[],sell=[];
      const queueData = await getAsync(_.get(currNode, 'channel_id'));
      if(queueData)
        {
          [buy,sell] = JSON.parse(queueData);
        }
      if(_.get(currNode, 'trade_type') == 0)
      {
          const result = processBuyOrder(currNode,sell);          
      }
      else
      {
          const result = processSellOrder(currNode,buy);
      }
      _.set(currNode,'units',result);
      if(result >= 0) // refilling the table only if atleast one is consumed by currNode
      {
          if(_.get(currNode, 'trade_type') == 0)
          {
            const selectQuery ='SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 1 ORDER BY price_per_unit ASC, time ASC LIMIT 10'
            const values = [_.get(currNode, 'channel_id')];
            const res = await db.query(selectQuery, values);
            sell =  _.map(_.get(res, 'rows'), row => new Node(_.get(row, 'id'), _.get(row, 'user_id'), _.get(row, 'channel_id'), _.get(row, 'price_per_unit'), _.get(row, 'units'), _.get(row, 'type')));
          }
          else
          {
            const selectQuery ='SELECT id, user_id, channel_id, price_per_unit AS price, units AS quantity, trade_type AS type FROM bids WHERE channel_id = $1 and status = 0 and trade_type = 0 ORDER BY price_per_unit DESC, time ASC LIMIT 10'
            const values = [_.get(currNode, 'channel_id')];
            const res = await db.query(selectQuery, values);
            buy =  _.map(_.get(res, 'rows'), row => new Node(_.get(row, 'id'), _.get(row, 'user_id'), _.get(row, 'channel_id'), _.get(row, 'price_per_unit'), _.get(row, 'units'), _.get(row, 'type')));
          }
      }
      if(result != 0)
      {
        const insertQuery = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time) 
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) 
        RETURNING id`;
        const values = [_.get(currNode, 'user_id'), _.get(currNode, 'channel_id'), _.get(currNode, 'units'), _.get(currNode, 'price_per_unit'), _.get(currNode, 'trade_type')];
        const res = await db.query(insertQuery, values);
        _.set(currNode, 'id', _.get(res, 'rows[0].id'));
        
        if(_.get(currNode, 'trade_type') == 0)
        {
           buy = insertSorted(buy,currNode,(a, b) => _.get(b, 'price') - _.get(a, 'price'),10);
       
       }
       else
       {
           sell = insertSorted(sell,currNode,(a, b) => _.get(b, 'price') - _.get(a, 'price'),10);
       }
      }
      await setAsync(_.get(currNode, 'channel_id'), JSON.stringify([buy,sell]));
      await deleteAsync(key);
      res.status(200).send();
      
      
  }
  catch(err)
  {
      console.log(err);
      res.status(500).send(err);
  }
  

});

const processBuyOrder = async (currNode,sellArray) => {
      if(_.size(sellArray) == 0 || _.get(currNode, 'price_per_unit') < _.get(sellArray, '[0].price_per_unit'))
      {
          return -1;
      }
      else
      {
          const value = await matchOrder(currNode,sellArray);
          return value;
      }
}

const processSellOrder = async (currNode,buyArray) => {
    if(_.size(buyArray) == 0 || _.get(currNode, 'price_per_unit') > _.get(buyArray, '[0].price_per_unit'))
      {
          return -1;
      }
      else
      {
          const value = await matchOrder(currNode,buyArray);
          return value;
      }
}

const matchOrder = async (currNode,memQueue) => {
    while(_.size(memQueue) > 0 && (_.get(currNode,'trade_type') == 0 ? _.get(memQueue, '[0].price_per_unit') > _.get(currNode, 'price_per_unit') : _.get(memQueue, '[0].price_per_unit') < _.get(currNode, 'price_per_unit')))
    {
          if(_.get(currNode,'units') >= _.get(memQueue, '[0].units'))
          {
              _.update(currNode,'units',units => units - _.get(memQueue, '[0].units'));
              await InsertTrade(currNode,memQueue[0],_.get(memQueue, '[0].units'));
              await updateBidStatus(_.get(memQueue, '[0].id'),1,_.get(memQueue, '[0].units'));
              memQueue.shift();
          }   
          else
          {
              _.update(memQueue, '[0].units',units => units - _.get(currNode, 'units'));
              await InsertTrade(memQueue[0],currNode,_.get(currNode, 'units'));
              await updateBidStatus(_.get(memQueue, '[0].id'),0,_.get(currNode, 'units'));
              return {array:memQueue,value:_.get(currNode, '[0].units')};
          }
    }
    return _.get(currNode, '[0].units');
}

const updateBidStatus = async (bidId,status,units) => {
    const query = 'UPDATE bids SET status = $1, units = $2 WHERE id = $3';
    const values = [status,units,bidId];
    await db.query(query,values);
}

const InsertTrade = async (currNode,inMenoryNode,units) => {
  const query = `
    INSERT INTO trades (bid_id, buyer_id, seller_id, channel_id, units, price_per_unit, time)
    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);`;
    const values = _.get(currNode,'trade_type') === 0
    ? [_.get(inMenoryNode,'id'),_.get(currNode,'user_id'),_.get(inMenoryNode,'user_id'),_.get(currNode,'channel_id'),units,_.get(inMenoryNode,'price_per_unit')]
    : [_.get(inMenoryNode,'id'),_.get(inMenoryNode,'user_id'),_.get(currNode,'user_id'),_.get(currNode,'channel_id'),units,_.get(currNode,'price_per_unit')];
    await db.query(query,values);
  
}

const insertSorted = (array,value,comparator,limit) =>{
    const index = _.sortedIndex(array,value,comparator);
    array.splice(index,0,value);
    if(_.size(array) > limit)
    {
        array.pop();
    }
    return array;
}

module.exports = router;
