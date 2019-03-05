
const BLOCK_LIST_FOLDER = './blocklist-ipsets/';
const fs = require('fs');
const readline = require('readline');
var Queue = require('better-queue');
var client = require('./bin/connection');

let metaData = {}


client.cluster.health({},function(err,resp,status) {  
    console.log("-- Client Health --",resp);
  });





var q = new Queue(postData, { concurrent: 1 });
q.on('empty', function (){
    console.log("Queue done");
})

function postData(body, done){
    console.log("Adding one");
    client.bulk(body, function (err, resp) {
        if(resp.errors) {
           console.log(JSON.stringify(resp, null, '\t'));
           done();
        } else {
            console.log("Success")
            done();
        }
    });
}

async function readFile(list, process) {

    let file = `${BLOCK_LIST_FOLDER}${list}`

    const fileStream = fs.createReadStream(file);

    const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
    });

    for await (const line of rl) {
        if(!/^#/.test(line)){
            if(line in metaData){
                metaData[line].push(list);
            } else {
                metaData[line] = [list];
            }
        }
    }

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


    updateIPsets()
        .then(() => {

            let batch = {
                body: []
            };

            let i = 0;

            for(ip in metaData){

                if(i === 1000){
                    q.push(batch);
                    batch.body = [];
                    i = 0;
                }

                batch.body.push(`{ "index": { "_index" : "ipreputation", "_type": "_doc", "_id" : "${ip}" } }`);
                batch.body.push(`{ "ip": "${ip}", "lists": ["${metaData[ip].join('","')}"] }`);
                i++;

            }
        })




