var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client( {  
  hosts: [
    'siem-01.j.local:9200',
  ]
});

module.exports = client;