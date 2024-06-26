// models/Node.js
class Node {
    constructor(id, user_id, channel_id, price, quantity, type) {
      this.id = id;
      this.user_id = user_id;
      this.channel_id = channel_id;
      this.price_per_unit = price;
      this.units = quantity;
      this.trade_type = type;
    }
  }
  
  module.exports = Node;
  