// server.js
const express = require('express');
const bodyParser = require('body-parser');
const bidRoute = require('./routes/bids');

const app = express();
const port = 3000;

app.use(bodyParser.json());

app.use('/', bidRoute);

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
