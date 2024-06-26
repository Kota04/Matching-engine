const express = require('express');
const bodyParser = require('body-parser');
const bidRoute = require('./routes/bids');
const { connectRedis } = require('./redis/index');

const app = express();
const port = 3000;

app.use(bodyParser.json());

app.use('/bids', bidRoute);

app.listen(port, async () => {
  await connectRedis();
  console.log(`Server is running on port ${port}`);
});
