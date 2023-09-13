console.log("consumer");
const { Kafka } = require("kafkajs");

/*const kafka = new Kafka({
    brokers: [
        process.env.BROKER1,
        process.env.BROKER2,
        process.env.BROKER3,
    ],
    sasl: {
        mechanism: "scram-sha-512", // scram-sha-256 or scram-sha-512,
        username: process.env.USERNAME,
        password: process.env.PASSWORD,
    },
    ssl: true,
});*/

//local kafka
const kafka = new Kafka({
  clientId: "my-consumer",
  brokers: ["sachin-HP-Laptop-15-da0xxx:9092"],
});

const consumer = kafka.consumer({
  groupId: process.env.GROUP_SAAMS,
  autoCommit: false,
});

module.exports.saamsDataConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: process.env.SAAMS_ACK_DATA,
    fromBeginning: true,
    maxInFlightRequests: 1,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let messageValue;
      let retries = 0;

      try {
        messageValue = JSON.parse(message.value.toString());
      } catch (error) {
        // If parsing fails, assume the message is a string and log it.
        console.log({
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
        console.log("KAFKA MESSAGE CONSUMED");
        return;
      }
      //const reqBody = JSON.parse(message.value.toString())
      console.log({
        partition,
        offset: message.offset,
        value: messageValue,
      });
      console.log("KAFKA MESSAGE CONSUMED");
      //console.log("message", reqBody);

      while (retries < 3) {
        const result = await processMessage({ topic, partition, message });
        if (result.success) {
          await consumer.commitOffsets([
            { topic, partition, offset: message.offset },
          ]);
          console.log("KAFKA MESSAGE COMMITTED");
          break;
        } else {
          console.log(
            `RETRYING message ${message.offset}, retry ${retries + 1}`
          );
          retries++;
          await new Promise((resolve) => setTimeout(resolve, 5000));
        }
      }
      if (retries == 3) {
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
        console.log("KAFKA MESSAGE COMMITTED");
      }
    },
  });

  const processMessage = async ({ topic, partition, message }) => {
    let result;

    const reqBody = JSON.parse(message.value.toString());
    const msgType = reqBody.msgType;

    switch (msgType) {
        case "add_modules":
            result = { success: true}
            
            break;

        case "add_modules1":
            result = { success: false}
            
            break;
    
        default:
            console.log("pass the valid")
            result = { success: true}
            break;
    }

    //result = { success: false };

    return result;
  };
};
