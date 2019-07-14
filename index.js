
const BLOCK_LIST_FOLDER = './blocklist-ipsets/';
const BLOCK_LIST_PROPERTIES = ['Maintainer', 'Category'];
const fs = require('fs');
const readline = require('readline');
var Queue = require('better-queue');
var client = require('./bin/connection');
let ipToList = {};
let fileMetaData = {};


client.cluster.health({},function(err,resp,status) {  
    console.log("-- Client Health --",resp);
  });

var q = new Queue(postData, { concurrent: 1 });
q.on('empty', function (){
    console.log("Queue done");
})

function postData(body, done){
    client.bulk(body, function (err, resp) {
        if(resp.errors) {
           console.log(resp.errors)
           done();
        } else {
            done();
        }
    });
}

async function readFile(list, process) {

    let fileProperties = {}
    let file = `${BLOCK_LIST_FOLDER}${list}`

    const fileStream = fs.createReadStream(file);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        
        if(/^#/.test(line)){
            let property = parseFileMetaData(line);
            if(BLOCK_LIST_PROPERTIES.indexOf(property.key) > -1){
                fileProperties[property.key] = property.value;
            }
        } else { 
            if(line in ipToList){
                ipToList[line].push(list);
            } else {
                ipToList[line] = [list];
            }
        }
    }

    fileProperties.file = list;
    fileMetaData[list] = fileProperties;

}

function parseFileMetaData(line) {

    let key;
    let value;

    try {

        let lineArray = line.replace(/^#\s+/, '').split(/:/);
        
        if(lineArray.length === 2){
            key = lineArray[0].trim();
            value = lineArray[1].trim();
        }
    
    } catch(e){
        console.log(e);
    }

    return { key: key, value: value }
}

async function updateIPsets(){
    
    let files = fs.readdirSync(BLOCK_LIST_FOLDER);
    let ipsets = files.filter(f => {
            return /.+\.ipset$/.test(f);
    });
    for(list of ipsets){
        await readFile(list);
    }

}

function pushIPLists() {
    let batch = {
        body: []
    };

    let i = 0;

    for(ip in ipToList){

        if(i === 1000){
            q.push(batch);
            batch.body = [];
            i = 0;
        }

        batch.body.push(`{ "index": { "_index" : "ipreputation", "_type": "_doc", "_id" : "${ip}" } }`);
        batch.body.push(`{ "ip": "${ip}", "lists": ${JSON.stringify(ipToList[ip])} }`);
        i++;
    }

    // Push the remainder to the queue
    if(batch.body.length > 0){
        q.push(batch);
    }
}


function pushMetaData() {

    let batch = {
        body: []
    };

    let i = 0;

    for(file in fileMetaData){

        if(i === 1000){
            q.push(batch);
            batch.body = [];
            i = 0;
        }
        
        batch.body.push(`{ "index" : {"_id" : "${file}", "_type" : "_doc", "_index" : "ipreputation-metadata"} }`);
        batch.body.push(JSON.stringify(fileMetaData[file]));
        i++;
    }

    // Push the remainder to the queue
    if(batch.body.length > 0){
        q.push(batch);
    }
}


updateIPsets()
        .then(() => {
            pushIPLists();
            pushMetaData();
        })
