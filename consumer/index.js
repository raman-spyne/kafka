const { Kafka } = require("kafkajs");

const server = new Kafka({
  brokers: ["pkc-n00kk.us-east-1.aws.confluent.cloud:9092"],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: "6KSCVIWS4VJKBONU",
    password:
      "gYTlmC/lkMArJcvW+rBcjd+mD/A78dGEI3u8RMxKciAloGq4zvcz31gPca/DkgR1",
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
