const  mongodb = require('mongodb');
const async = require('async');

const customers = require('./data/m3-customer-data.json');
const customerAddress = require('./data/m3-customer-address-data.json');
console.log('Task Begins');
let tasks = [];

const limit = parseInt(process.argv[2],10) || 1000
//let url = "mongodb://localhost:27017/edx-course-db"
let url = "mongodb://localhost:27017/edx-course-db";
//mongodb.MongoClient(url,{useNewUrlParser: true},(error, client)=>{
mongodb.MongoClient.connect(url,{useNewUrlParser:true}, (error,client)=>{
    if(error) return console.log(error);
    let db = client.db('edx-course-db');
   
    customers.forEach((customer, index, list) => {
        customers[index] = Object.assign(customer, customerAddress[index]);
        if(index % limit == 0){
            const start = index;
            const end = (start+limit > customers.length) ? customers.length-1 : start+limit;
            tasks.push((done)=>{
                console.log(`Processing ${start}-${end} out of ${customers.length}`)
                db.collection('customers_1').insert(customers.slice(start,end),(error,results) =>{
                    done(error, results);
                });
            });
            console.log()
        }
    })
    console.log(`${tasks.length} parallel task(s)`);
    const startTime = Date.now();
    async.parallel(tasks,(error,results) => {
        if(error) console.error(error);

        const endTime = Date.now();
        console.log(`Execution Time: ${endTime-startTime}`);

        client.close();
    });
});
console.log('Task Ends');
