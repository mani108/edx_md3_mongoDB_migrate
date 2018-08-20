const  mongodb = require('mongodb');
const async = require('async');

const customers = require('./data/m3-customer-data.json');
const customerAddress = require('./data/m3-customer-address-data.json');

let tasks = [];

tasks.push(function(Callback){
    let documents = [];
    console.log(`Processing Records :`);
    for(j=0;j<Object.keys(customers).length;j++){
        let document = {};
        Object.assign(document, customers[j], customerAddress[j]);
        documents.push(document);
    }
    writeToDb(documents, Callback);
});

const writeToDb = function(docs, cb){
    let url = "mongodb://localhost:27017/edx-course-db";
    mongodb.MongoClient.connect(url,{useNewUrlParser:true}, (err,client)=>{
        if(err)
        cb(err, null);

        let db = client.db('edx-course-db');
        db.collection('customers').insert(docs, function(err,res){
            client.close();
            if(err){
                cb(err, null);
            }
            console.log(`Processed Records`);
        });
    });
}

async.parallel(tasks, function(err, res){
    console.log(res);
});