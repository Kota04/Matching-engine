const {Client} = require('pg')

const con = new Client({ 
    host: 'localhost',
    user: 'satya', 
    port: 5432,
    password: '6185',
    database: 'stocks'

})

con.connect((err) => {
    if(err){
        console.log('Error connecting to db', err)
    }else{
        console.log('Connected to db')
    }
})
module.exports = con;

