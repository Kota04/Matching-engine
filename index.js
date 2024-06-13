const express = require('express');
const client = require('./db');

const app = express();
const port = 3000;

app.use(express.json());


class Node {
  constructor(id,price, quantity,type) {
      this.id = id;
      this.price = price;
      this.quantity = quantity;
      this.type = type;
      this.next = null;
  }
}

class LinkedList {
  constructor() {
      this.head = null;
      console.log("LinkedList created");
  }

  // Insert a new node in sorted order
  insertSell(node) {
      if (this.head === null || this.head.price >= node.price) {
          node.next = this.head;
          this.head = node;
          return;
      }
      let current = this.head;
      while (current.next !== null && current.next.price < node.price) {
          current = current.next;
      }
      
      node.next = current.next;
      current.next = node;
  }

  insertBuy(node) {
      if (this.head === null || this.head.price <= node.price) {
          node.next = this.head;
          this.head = node;
          return;
      }
      
      let current = this.head;
      while (current.next !== null && current.next.price > node.price) {
          current = current.next;
      }
      
      node.next = current.next;
      current.next = node;
  }

  matchForSell(buyOrderNode) {  // when the order is buy
      if (this.head === null || this.head.price > buyOrderNode.price) {
          return 0; // No match
      }      
      while (this.head !== null && buyOrderNode.quantity > 0 && this.head.price <= buyOrderNode.price) {
          if (this.head.quantity > buyOrderNode.quantity) {
              this.head.quantity -= buyOrderNode.quantity;
              const updateQuery = 'UPDATE "order" SET status = 2, dqua=$2 WHERE id = $1';
              const values = [this.head.id, this.head.quantity];
              client.query(updateQuery, values, (err, result) => {
                  if (err) {
                      console.log(err);
                  } else {
                    //  console.log('Order status updated');
                  }
              });
              buyOrderNode.quantity = 0; // reflect the database to order status as partially matched
              return 1; // Successfully matched
          } else {
              buyOrderNode.quantity -= this.head.quantity;
              // reflect the database to order status as successfully matched
              const updateQuery = 'UPDATE "order" SET status = 1, dqua=$2 WHERE id = $1';
              const values = [this.head.id, this.head.quantity];
              client.query(updateQuery, values, (err, result) => {
                  if (err) {
                      console.log(err);
                  } else {
                   //   console.log('Order status updated');
                  }
              });
              this.head = this.head.next; 
          }
      }
      if(buyOrderNode.quantity === 0){
          return 1; // Successfully matched
      }
      
      return buyOrderNode.quantity; // Partially matched
  }

  matchForBuy(sellOrderNode) {  // when the order is sell
      if (this.head === null || this.head.price < sellOrderNode.price) {
          return 0; // No match
      }
      
      while (this.head !== null && sellOrderNode.quantity > 0 && this.head.price >= sellOrderNode.price) {
          if (this.head.quantity > sellOrderNode.quantity) {
              this.head.quantity -= sellOrderNode.quantity;
              // reflect the database to order status as partially matched
              const updateQuery = 'UPDATE "order" SET status = 2, dqua=$2 WHERE id = $1';
              const values = [this.head.id, this.head.quantity];
              client.query(updateQuery, values, (err, result) => {
                  if (err) {
                      console.log(err);
                  } else {
                 //     console.log('Order status updated');
                  }
              });
              sellOrderNode.quantity = 0;
              return 1; // Successfully matched
          } else {
              sellOrderNode.quantity -= this.head.quantity;
              // reflect the database to order status as successfully matched
              const updateQuery = 'UPDATE "order" SET status = 1, dqua=$2 WHERE id = $1';
              const values = [this.head.id, this.head.quantity];
              client.query(updateQuery, values, (err, result) => {
                  if (err) {
                      console.log(err);
                  } else {
                //      console.log('Order status updated');
                  }
              });
              this.head = this.head.next; //
          }
          
      }
      if(sellOrderNode.quantity === 0){
          return 1; // Successfully matched
      }
      
      return sellOrderNode.quantity; // Partially matched
  }
}


const buy = new LinkedList();
const sell = new LinkedList();

app.post('/buy', async (req, res) => {
 
    const order = req.body;
    const node = new Node(order.id,order.price, order.quantity,order.type);
    console.log(node.id);    
    // Insert the order node into the database
     const insertQuery = 'INSERT INTO "order" (id, price, quantity, dqua, status, type) VALUES ($1, $2, $3, $4, $5, $6)';
     const values = [node.id, node.price, node.quantity, node.quantity, 0, node.type];
     client.query(insertQuery, values,(err, result) => {
          if (err) {
              console.log(err);
              res.status(500).send('Error inserting order');
          } else {
          //    res.status(201).send('Order inserted successfully');
          }
     });
    if (order.type === 0) { // buy
    
        const matchResult = sell.matchForSell(node);
        if (matchResult === 0) {
            buy.insertBuy(node);
          //  console.log("No matching sell orders found.");
        } else if (matchResult === 1) {
           // console.log("Buy order fully matched.");
            const updateQuery = 'UPDATE "order" SET status = 1,dqua=$2 WHERE id = $1';
            const values = [node.id,node.quantity];
            // Update the order status in the database
            client.query(updateQuery, values, (err, result) => {  
                if (err) {
                    console.log(err);
                } else {
               //     console.log('Order status updated');
                }
            });
        } else  {
            node.quantity = matchResult;
            console.log(node.quantity)
            buy.insertBuy(node);
          //  console.log("Buy order partially matched.");
            const updateQuery = 'UPDATE "order" SET status = 2,dqua=$2 WHERE id = $1';
            const values = [node.id, node.quantity];
            // Update the order status in the database
            client.query(updateQuery  , values, (err, result) => {  
                if (err) {
                    console.log(err);
                } else {
                 //   console.log('Order status updated');
                }
            });
        }
    } 
    else 
    {
        const matchResult2 = buy.matchForBuy(node); // sell
        if (matchResult2 === 0) { 
            sell.insertSell(node);
         //   console.log("No matching buy orders found.");
        } else if (matchResult2 === 1) {
       //   console.log("Buy order fully matched.");
          const updateQuery = 'UPDATE "order" SET status = 1,dqua=$2 WHERE id = $1';
          const values = [node.id,node.quantity];
          // Update the order status in the database
          client.query(updateQuery, values, (err, result) => {  
              if (err) {
                  console.log(err);
              } else {
               //   console.log('Order status updated');
              }
          });
        } else {    
            node.quantity = matchResult2;
          //  console.log(node.quantity)
            sell.insertSell(node);
          //  console.log("Sell order partially matched.");
            const updateQuery = 'UPDATE "order" SET status = 2,dqua=$2 WHERE id = $1';
            const values = [node.id, node.quantity];
            // Update the order status in the database
            client.query(updateQuery  , values, (err, result) => {  
                if (err) {
                    console.log(err);
                } else {
                   // console.log('Order status updated');
                }
            });
        }
    }
});



app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
