const { Kafka } = require("kafkajs");
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

const consumer = server.consumer({ groupId: "G1" });

(async function() {
    await consumer.connect();
    console.log('\n Consumer connected... \n')

    await consumer.subscribe({ topic: 'topic_01' });

    await consumer.run({
        eachMessage: async ({topic, message}) => {
            console.log({
                topic,
                key: message.key.toString(),
                value: message.value.toString(),
                headers: message.headers
            });
        }
    });
})()
