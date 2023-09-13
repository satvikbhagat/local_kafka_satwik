console.log("consumer user schedule");
const { Kafka } = require("kafkajs");
require("dotenv").config();

// const kafka = new Kafka({
//   brokers: [process.env.BROKER1, process.env.BROKER2, process.env.BROKER3],
//   sasl: {
//     mechanism: "scram-sha-512", // scram-sha-256 or scram-sha-512,
//     username: process.env.USERNAME,
//     password: process.env.PASSWORD,
//   },
//   ssl: true,
// });

//local kafka
const kafka = new Kafka({
  clientId: "my-consumer",
  brokers: [process.env.BROKER],
});
console.log("INFRA CONSUMER GROUP ID", process.env.GROUP_RESOURCE_CREATE_1);

const consumer = kafka.consumer({
  groupId: process.env.GROUP_RESOURCE_CREATE_1,
});
let connected = false;

consumer.on("consumer.connect", (_) => {
  console.log("kafka consumer connected");
  connected = true;
});

consumer.on("consumer.disconnect", (_) => {
  console.log("kafka consumer disconnected");
  connected = false;
  SaamsControllerDataConsumer();
});

consumer.on("consumer.commit_offsets", (topics) => {
  console.log("Offset committed :", JSON.stringify(topics.payload.topics));
});

consumer.on("consumer.rebalancing", (memberId) => {
  console.log("Rebalancing Consumers");
  console.log("memberId: ", memberId);
});

module.exports.SaamsControllerDataConsumer = async () => {
  try {
    if (!connected) {
      try {
        await consumer.connect();
        console.log("Connected to Kafka consumer successfully");
      } catch (error) {
        console.error("Failed to connect to Kafka consumer:", error);
        return;
      }
      //await consumer.connect();
      await consumer.subscribe({
        topic: process.env.SAAMS_RESOURCE_TOPIC,
        fromBeginning: true,
      });

      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          //const reqBody = JSON.parse(message.value.toString());
          let messageValue;
          let commitMessage;
          let currentOffset = parseInt(message.offset) + 1;
          console.log("current offset++++++", currentOffset);

          try {
            messageValue = JSON.parse(message.value.toString());
          } catch (error) {
            console.log("error++++++++++++");
            // If parsing fails, assume the message is a string and log it.
            console.log({
              partition,
              offset: message.offset,
              value: message.value.toString(),
            });
            console.log("KAFKA MESSAGE CONSUMED");

            commitMessage = await consumer.commitOffsets([
              { topic, partition, offset: currentOffset.toString() },
            ]);
            return;
          }
          console.log({
            partition,
            offset: message.offset,
            value: messageValue,
          });
          console.log("KAFKA MESSAGE CONSUMED");
          await processMessage({ topic, partition, message });

          commitMessage = await consumer.commitOffsets([
            { topic, partition, offset: currentOffset.toString() },
          ]);
        },
      });
      console.log("kafka consumer ran");
    }
  } catch (error) {
    console.warn("Kafka consumer connect failed", error);
    if (connected) {
      console.log("Kafka Manually disconnecting consumer");
      await consumer.disconnect();
    } else {
      console.log("kafka waiting 10 sec");
      setInterval(() => {
        SaamsControllerDataConsumer();
      }, 10000);
    }
  }
};

const processMessage = async ({ topic, partition, message }) => {
  //const key = message.key.toString();
  const reqBody = JSON.parse(message.value.toString());
  let result = true;
  //console.log("key+++++++++", key);
  console.log("value+++++++++", reqBody);
  // const key = reqBody.messageData.msgType;
  // console.log("key++++", key);

  return result;
};
