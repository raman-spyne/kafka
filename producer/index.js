const { Kafka } = require("kafkajs");
const crypto = require("crypto");
const env = require('dotenv');

// initializing .env file
env.config();

const server = new Kafka({
  brokers: process.env.BROKERLIST.split(','),
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.USERNAME,
    password: process.env.PASSWORD
  },
});

const producer = server.producer();

producer.connect().then(() => {
  console.log("connected...");

  setInterval(
    () =>
      producer.send({
        topic: "topic_01",
        messages: [
          {
            key: `${crypto.randomInt(6)}`,              // 0,1,2,3,4,5 are the six possible keys to decide among 6 partitions
            value: crypto.randomBytes(20).toString(),
          },
        ],
      }),
    2000
  );
});
