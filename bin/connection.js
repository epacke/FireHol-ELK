var elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client( {  
  hosts: [
    '',
  ]
});

module.exports = client;