/*jslint es6 */
"use strict";
const url = require('url');
const Web3 = require('web3');
const Influx = require('influx');
const GOLEM_TOKEN_START_BLOCK = 5385618;
const GOLEM_CONTRACT_ADDRESS = '0xA7dfb33234098c66FdE44907e918DAD70a3f211c'

// client to the influx db
const influx = new Influx.InfluxDB({
  host: process.env.INFLUXDB_HOST || 'localhost',
  port: process.env.INFLUXDB_PORT || 8086,
  database: 'golem_network_data',
  schema: [{
    measurement: 'transfers',
    fields: {
      from: Influx.FieldType.STRING,
      to: Influx.FieldType.STRING,
      value: Influx.FieldType.STRING,
      closure_time: Influx.FieldType.INTEGER,
      block_number: Influx.FieldType.INTEGER,
      block_timestamp: Influx.FieldType.INTEGER
    },
    tags: []
  }]
});

console.log(`Creating 'golem_network_data' InfluxDB database`);
influx.createDatabase('golem_network_data');

function writePoints(from, to, value, closureTime, blockNumber, blockTimestamp) {
  influx.writePoints([{
    measurement: 'transfers',
    fields: {
      from: from,
      to: to,
      value: value,
      closure_time: closureTime,
      block_number: blockNumber,
      block_timestamp: blockTimestamp
    },
    tags: []
  }]);
}

const PARITY_NODE = process.env.PARITY_URL || "http://localhost:8545";
var web3 = new Web3(new Web3.providers.HttpProvider(PARITY_NODE));

var batchTransferAbi = [{
  "anonymous": false,
  "inputs": [{
      "indexed": true,
      "name": "from",
      "type": "address"
    },
    {
      "indexed": true,
      "name": "to",
      "type": "address"
    },
    {
      "indexed": false,
      "name": "value",
      "type": "uint256"
    },
    {
      "indexed": false,
      "name": "closureTime",
      "type": "uint64"
    }
  ],
  "name": "BatchTransfer",
  "type": "event"
}];

var golemContract = new web3.eth.Contract(batchTransferAbi, GOLEM_CONTRACT_ADDRESS);

function getPastEvents(startBlockNumber) {
  console.log(`Get past events startinng from ${startBlockNumber} block number`);

  golemContract.getPastEvents('BatchTransfer', {
      fromBlock: startBlockNumber,
      toBlock: 'latest'
    })
    .then(function (events) {
      events.forEach(function (batchTranfer) {
        var {
          blockNumber,
          returnValues: {
            from,
            to,
            value,
            closureTime
          }
        } = batchTranfer

        web3.eth.getBlock(blockNumber) // slow
          .then(function(block){
            writePoints(from, to, value, closureTime, blockNumber, block.timestamp)
          })
      })
    })
}

function work() {
  console.log(`Selecting the MAX block number from the database`);

  influx.query("select MAX(block_number) from transfers")
  .then(blockNumberInfo => {
    const blockNumber = blockNumberInfo[0].max || GOLEM_TOKEN_START_BLOCK
    getPastEvents(blockNumber)
  })
}

// Execute the `getPastEvents` every 5 minutes
work();
setInterval(work, 5 * 60 * 1000);

//======================================================
module.exports = async function (request, response) {
  let req = url.parse(request.url, true);
  let q = req.query;

  switch (req.pathname) {
    case '/healthcheck':
      return send(response, 200, "ok");

    default:
      return send(response, 404, 'Not found');
  }
}