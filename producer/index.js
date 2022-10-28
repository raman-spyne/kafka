const { Kafka } = require("kafkajs");
const crypto = require("crypto");

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
