// routes/bid.js
const express = require('express');
const db = require('../db');
const Node = require('../models/Node');
const client = require('../redis/index');

const router = express.Router();

router.post('/bid', async (req, res) => {
    const data = req.body;
    const query = `INSERT INTO bids (user_id, channel_id, units, price_per_unit, trade_type, time) 
                   VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) 
                   RETURNING id`;
    const values = [data.user_id, data.channel_id, data.quantity, data.price, data.type];
    
    const result = await db.query(query, values);
    const id = result.rows[0].id; // get the id of the inserted row 
    res.status(201).json(node);
   
    const node = new Node(id,data.user_id,data.channel_id,data.price,data.quantity,data.type); 
    
    await client.connect();

    const value = await client.get(node.channel_id);
    if(value === null)  // case where the first request is sent
    {                           
        const buy = [];
        const sell = [];
        if(node.type === 0)
        {
            buy.push(node);
        }
        else
        {
            sell.push(node);
        }
        await client.set(node.channel_id, JSON.stringify([buy,sell]));
        
    }
    else
    {
        const bids = JSON.parse(value);
        const buy = bids[0];
        const sell = bids[1];
        if(node.type === 0) 
        {
            if(sell[0].price>node.price) // no match occurs
            {
                let index=0;
                while(index<sell.length && buy[index].price>node.price)
                {
                    index++;
                }
                buy.splice(index,0,node);
                if(buy.length > 10) // change it to buffer size
                {
                    buy.pop();
                }
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
            } 
            else
            {
                while( sell.length > 0 && sell[0].price<=node.price && node.quantity > 0)
                {
                    if(node.quantity > sell[0].quantity)
                    {
                        node.quantity -= sell[0].quantity;
                        sell[0].quantity = 0;
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [sell[0].quantity, sell[0].id];  
                        await db.query(updateQuery, updateValues);
                        // add the query in trades table
                    }
                    else
                    {
                        sell[0].quantity -= node.quantity;
                        node.quantity = 0;
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [node.quantity, node.id];  
                        await db.query(updateQuery, updateValues);
                        // add the query in trades table
                    }
                    if(sell[0].quantity === 0)
                    {
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [sell[0].quantity, sell[0].id];  // update query for sell node
                        await db.query(updateQuery, updateValues);
                        sell.shift();
                    }
                    else
                    {
                        const updateQuery = `UPDATE bids SET units = $1 WHERE id = $2`;
                        const updateValues = [sell[0].quantity, sell[0].id];  // update query for sell node
                        await db.query(updateQuery, updateValues);
                    }
                } 
                if(node.quantity > 0)
                {
                    const updateQuery = `UPDATE bids SET units = $1 WHERE id = $2`;
                    const updateValues = [node.quantity, node.id];  // update query for current node
                    await db.query(updateQuery, updateValues);
                    buy.splice(0,0, node); // add the node to the front of the array
                }    
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
            }   
        }
        else // order is sell
        {
            if(buy[0].price<node.price) // no match occurs
            {
                // check the condition to check whether to add or not
                let index=0;
                while(index<buy.length && sell[index].price<node.price)
                {
                    index++;
                }
                sell.splice(index,0,node);
                if(sell.length > 10) // change it to buffer size
                {
                    sell.pop();
                }
                
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
            }
            else
            {
                while( buy.length > 0 && buy[0].price>=node.price && node.quantity > 0)
                {
                    if(node.quantity > buy[0].quantity)
                    {
                        node.quantity -= buy[0].quantity;
                        buy[0].quantity = 0;
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [buy[0].quantity, buy[0].id];  
                        await db.query(updateQuery, updateValues);
                        // add the query in trades table
                    }
                    else
                    {
                        buy[0].quantity -= node.quantity;
                        node.quantity = 0;
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [node.quantity, node.id];  
                        await db.query(updateQuery, updateValues);
                        // add the query in trades table
                    }
                    if(buy[0].quantity === 0)
                    {
                        const updateQuery = `UPDATE bids SET units = $1,status = 1 WHERE id = $2`;
                        const updateValues = [buy[0].quantity, buy[0].id];  // update query for buy node
                        await db.query(updateQuery, updateValues);
                        buy.shift();
                    }

                }
                if(node.quantity > 0)
                {
                    const updateQuery = `UPDATE bids SET units = $1 WHERE id = $2`;
                    const updateValues = [node.quantity, node.id];  
                    await db.query(updateQuery, updateValues);
                    sell.splice(0,0, node); 
                }
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
            }
        }
    }

});

module.exports = router;
