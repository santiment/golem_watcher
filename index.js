/*jslint es6 */
"use strict";
const { send } = require("micro");
const url = require("url");
const Web3 = require("web3");
const Influx = require("influx");
const GOLEM_TOKEN_START_BLOCK = 5385618;
const GOLEM_CONTRACT_ADDRESS = "0xA7dfb33234098c66FdE44907e918DAD70a3f211c";
const GOLEM_DATABASE_NAME = "golem_network_data";
const GOLEM_TOKEN_DECIMALS = 18;

// client to the influx db
const influx = new Influx.InfluxDB({
  host: process.env.INFLUXDB_HOST || "localhost",
  port: process.env.INFLUXDB_PORT || 8086,
  database: "golem_network_data",
  schema: [
    {
      measurement: "transfers",
      fields: {
        from: Influx.FieldType.STRING,
        to: Influx.FieldType.STRING,
        value: Influx.FieldType.FLOAT,
        closure_time: Influx.FieldType.INTEGER,
        block_number: Influx.FieldType.INTEGER
      },
      tags: ["transaction_index", "transaction_log_index"]
    }
  ]
});

const writePoints = (
  from,
  to,
  value,
  closureTime,
  blockNumber,
  blockTimestamp,
  transactionIndex,
  transactionLogIndex
) => {
  let date = new Date(blockTimestamp * 1000);

  influx
    .writePoints([
      {
        measurement: "transfers",
        timestamp: date, // node-influx will handle the necessary precision
        fields: {
          from: from,
          to: to,
          value: value / Math.pow(10, GOLEM_TOKEN_DECIMALS),
          closure_time: closureTime,
          block_number: blockNumber
        },
        tags: {
          transaction_index: transactionIndex,
          transaction_log_index: transactionLogIndex
        }
      }
    ])
    .catch(err => {
      console.error(
        `Error writing points for block number ${blockNumber},
        transaction index ${transactionIndex} and transaction log index ${transactionLogIndex}
        to InfluxDB! ${err.stack}`
      );
    });
};

const PARITY_NODE = process.env.PARITY_URL || "http://localhost:8545";
let web3 = new Web3(new Web3.providers.HttpProvider(PARITY_NODE));

let batchTransferAbi = [
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        name: "from",
        type: "address"
      },
      {
        indexed: true,
        name: "to",
        type: "address"
      },
      {
        indexed: false,
        name: "value",
        type: "uint256"
      },
      {
        indexed: false,
        name: "closureTime",
        type: "uint64"
      }
    ],
    name: "BatchTransfer",
    type: "event"
  }
];

let golemContract = new web3.eth.Contract(
  batchTransferAbi,
  GOLEM_CONTRACT_ADDRESS
);

const healthcheckInflux = () => {
  return influx.query("show measurements");
};

const healthcheckParity = () => {
  return web3.eth.getBlockNumber();
};

const getPastEvents = startBlockNumber => {
  console.log(
    `Getting past events startinng from ${startBlockNumber} block number`
  );

  let endBlockNumber = startBlockNumber + 20000;
  golemContract
    .getPastEvents("BatchTransfer", {
      fromBlock: startBlockNumber,
      toBlock: endBlockNumber
    })
    .then(events => {
      console.log(
        `Got ${events &&
          events.length} batch tarnsfer events for the block range [${startBlockNumber}, ${endBlockNumber}]}`
      );
      events.forEach(function(batchTranfer) {
        let {
          transactionIndex,
          transactionLogIndex,
          blockNumber,
          returnValues: { from, to, value, closureTime }
        } = batchTranfer;

        web3.eth
          .getBlock(blockNumber) // slow
          .then(block => {
            writePoints(
              from,
              to,
              value,
              closureTime,
              blockNumber,
              block.timestamp,
              transactionIndex,
              transactionLogIndex
            );
          })
          .catch(err => {
            console.error(
              `Error saving data with block number ${blockNumber} to InfluxDB! ${
                err.stack
              }`
            );
          });
      });
    })
    .catch(err => {
      console.error(
        `Error getting past events since starting block number ${startBlockNumber}! ${
          err.stack
        }`
      );
    });
};

const work = () => {
  console.log(`Selecting the MAX block number from the database`);

  influx
    .query("select MAX(block_number) from transfers")
    .then(blockNumberInfo => {
      const blockNumber =
        (blockNumberInfo[0] !== undefined && blockNumberInfo[0].max) ||
        GOLEM_TOKEN_START_BLOCK;
      getPastEvents(blockNumber);
    })
    .catch(err => {
      console.error(
        `Error fetching the MAX block number from the database! \nError stack:\n ${
          err.stack
        }`
      );
    });
};

const init = () => {
  influx
    .createDatabase(GOLEM_DATABASE_NAME)
    .then(success => {
      console.log(
        `Sucessfully created database ${GOLEM_DATABASE_NAME}. Starting the work.`
      );

      // Execute the `work` every 5 minutes
      work();
      setInterval(work, 5 * 60 * 1000);
    })
    .catch(err => {
      console.error(
        `Cannot create database ${GOLEM_DATABASE_NAME}. \nError stack:\n ${
          err.stack
        }`
      );
      console.log(
        `Will try to create the database and start the work in 5 minutes.`
      );
      setTimeout(init, 5 * 60 * 1000);
    });
};

init();

process.on("unhandledRejection", (reason, p) => {
  // Otherwise unhandled promises are not possible to trace with the information logged
  console.error(
    "Unhandled Rejection at: Promise",
    p,
    "reason:",
    reason,
    "error stack:",
    reason.stack
  );
  process.exit(1);
});

//======================================================

module.exports = async (request, response) => {
  const req = url.parse(request.url, true);
  const q = req.query;

  switch (req.pathname) {
    case "/healthcheck":
      return healthcheckInflux()
        .then(healthcheckParity())
        .then(result => send(response, 200, "ok"))
        .catch(err =>
          send(
            response,
            500,
            `Connection to influx or parity failed. \nError stack:\n${
              err.stack
            }`
          )
        );

    default:
      return send(response, 404, "Not found");
  }
};
