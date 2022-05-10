import { Kafka, Message } from 'kafkajs';

const clientId = 'my-app';
const kafka = new Kafka({
  clientId,
  brokers: ['127.0.0.1:9092'],
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: clientId });

async function produceMessages(topic: string, messages: Message[]) {
  await producer.connect();
  await producer.send({
    topic,
    messages,
  });

  await producer.disconnect();
}

async function consumeMessages(topic: string) {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
}

produceMessages('quickstart-events', [{ value: 'Hello KafkaJS user!' }]).catch((err) => {
  console.error('error in producer: ', err);
});

consumeMessages('quickstart-events').catch((err) => {
  console.error('error in consumer: ', err);
});
