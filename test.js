// const { kafkaClient } = require("./kafkaClient");
// const { processMessage } = require("./messageProcessor");
// const { Topics } = require("./topics");

// require("dotenv").config();

// const consumer = kafkaClient.consumer({
//   groupId: process.env.KAFKA_CONSUMER_GROUP_ID,
// });
// let connected = false;

// consumer.on("consumer.connect", (_) => {
//   console.log("kafka consumer connected");
//   connected = true;
// });

// consumer.on("consumer.disconnect", (_) => {
//   console.log("kafka consumer disconnected");
//   connected = false;
//   autoConnect();
// });

// consumer.on("consumer.commit_offsets", (topics) => {
//   console.log("Offset committed :", JSON.stringify(topics.payload.topics));
// })

// consumer.on("consumer.rebalancing", (memberId) => {
//   console.log("Rebalancing Consumers");
//   console.log("memberId: ", memberId);
// })

// // keep retrying every 10 seconds
// async function autoConnect() {
//   try {
//     if (!connected) {
//       await consumer.connect();
//       await consumer.subscribe({
//         topics: [Topics.ACAAS_TX_TOPIC],
//         fromBeginning: true,
//       });
//       console.log("kafka consumer subscribed");
//       await consumer.run({
//         autoCommit: false,
//         eachMessage: async ({ topic, partition, message }) => {
//           let parsedMessage;
//           if (message.key == null) {
//             console.warn("key is null");
//           }
//           let key = message.key ? message.key.toString("utf-8") : "unknown";
//           let currentOffset = parseInt(message.offset) + 1;
//           try {
//             parsedMessage = JSON.parse(message.value.toString("utf-8"));
//             console.log("Message: ", parsedMessage);
//           } catch (error) {
//             console.warn("kafka consumer json parse failed", message.value, error);
//             //TODO: commit
//             consumer.commitOffsets([{ topic: topic, partition: partition, offset: currentOffset.toString() }])
//             return;
//           }
//           await processMessage(topic, partition, message.offset, key, message.timestamp, parsedMessage);
//           //TODO: commit
//           consumer.commitOffsets([{ topic: topic, partition: partition, offset: currentOffset.toString() }])
//         },
//       });
//       console.log("kafka consumer ran");
//     }
//   } catch (error) {
//     console.warn("Kafka consumer connect failed", error);;
//     if (connected) {
//       console.log("Kafka Manually disconnecting consumer");
//       await consumer.disconnect();;
//     } else {
//       console.log("kafka waiting 10 sec");
//       setInterval(() => {
//         autoConnect();;
//       }, 10000);
//     }
//   }
// }

// module.exports.kafkaConsumer = {
//   autoConnect: autoConnect,
// };