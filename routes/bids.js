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
    try
    {
        const result = await db.query(query, values);
        const id = result.rows[0].id; // returns the id of the inserted row
        // node to be created to store in the array
        res.status(201).json(node);
    }
    catch(err)
    {
        console.log(err);
        res.status(500).send('Error inserting bid');
    }
    const node = new Node(id,data.user_id,data.channel_id,data.price,data.quantity,data.type); 
    
    await client.connect();

    const value = await client.get(node.channel_id);
    if(value === null)
    {                           // case where the first request is sent
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
                // check the condition to check whether to add or not
                buy.push(node);
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
                        // update query for sell node and node
                        // add the query in trades table
                    }
                    else
                    {
                        sell[0].quantity -= node.quantity;
                        node.quantity = 0;
                        // update query for sell node 
                        // add the query in trades table
                        // update the status of node to successful
                    }
                    if(sell[0].quantity === 0)
                    {
                        sell.shift();
                    }
                } 
                if(node.quantity > 0)
                {
                    buy.push(node);
                    
                }    
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
                
            }   
        }
        else // order is sell
        {
            if(buy[0].price<node.price) // no match occurs
            {
                // check the condition to check whether to add or not
                sell.push(node);
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
                        // update query for buy node and node
                        // add the query in trades table
                    }
                    else
                    {
                        buy[0].quantity -= node.quantity;
                        node.quantity = 0;
                        // update query for buy node 
                        // add the query in trades table
                        // update the status of node to successful
                    }
                    if(buy[0].quantity === 0)
                    {
                        buy.shift();
                    }

                }
                if(node.quantity > 0)
                {
                    sell.push(node); // add to sell array in the start
                }
                await client.set(node.channel_id, JSON.stringify([buy,sell]));
            }
        }
    }

});

module.exports = router;
