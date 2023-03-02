const { Kafka, Partitioners } = require("kafkajs");
const axios = require("axios");
const avro = require("avsc");
const registryUrl = "http://localhost:3000";

// Kafka connection configuration
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

// Creating Kafka producer and consumer instances
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({groupId: "test-group"});

// Create a custom serializer that encodes data using the Avro schema
const avroSerializer = async (topic, data) => {
  const response = await axios.get(
    `${registryUrl}/schemas/${topic}/latest/versions`
  );
  if (!response.data || !response.data.schema || response.data.schema.length === 0) {
    throw new Error(`Could not find Schema for ${topic}`);
  }
  const schemaId = response.data.schema.id;
  const schema = response.data.schema.topicSchema;
  const type = avro.parse(schema);
  const buffer = type.toBuffer(data);
  return { value: buffer, headers: { schemaId: schemaId } };
};

// Create a custom deserializer that decodes data using the Avro schema
const avroDeserializer = async (topic, message) => {
  const schemaId = message.headers["schemaId"].toString();
  const response = await axios.get(`${registryUrl}/schemas/${schemaId}`);
  if (!response.data || !response.data.schema || response.data.schema.length === 0) {
    throw new Error(`Could not find Schema for ${topic}`);
  }
  const schema = response.data.schema[0].topicSchema;
  const type = avro.parse(schema);
  const value = type.fromBuffer(message.value);
  return value;
};

async function main() {
  await producer.connect();

  const schema = {
    type: "record",
    name: "myrecord",
    fields: [
      { name: "field1", type: "string" },
      { name: "field2", type: "int" },
    ],
  };

  const message = { field1: "value1", field2: 123 };

  // Use the custom serializer to encode the message using the Avro schema
  const encodedMessage = await avroSerializer("my-topic", message);

  // Send the encoded message to the Kafka topic
  await producer.send({
    topic: "my-topic",
    messages: [encodedMessage],
  });

  await producer.disconnect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });


  // Use the custom deserializer to decode the message using the Avro schema
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decoded = await avroDeserializer(topic, message);
      console.log(decoded);
    },
  });

}

main();
