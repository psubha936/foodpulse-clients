const assert = require("node:assert/strict");
const {
  connectKafkaProducer,
  createKafkaClient,
  startJsonConsumer,
} = require("../dist");

const TOPIC = "foodpulse.clients.integration.v1";

function withTimeout(promise, milliseconds) {
  let timer;

  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Timed out after ${milliseconds}ms`)),
      milliseconds,
    );
  });

  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function main() {
  const brokersValue = process.env.KAFKA_BROKERS;

  if (!brokersValue) {
    throw new Error("KAFKA_BROKERS is missing from .env");
  }

  const brokers = brokersValue
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);

  const client = createKafkaClient({
    clientId: "foodpulse-clients-test",
    brokers,
    options: {
      connectionTimeout: 10_000,
      requestTimeout: 15_000,
      retry: { retries: 3 },
    },
  });

  const admin = client.createAdmin();
  let producerConnection;
  let consumerConnection;

  const eventId = `event-${Date.now()}`;
  const groupId = `foodpulse-clients-test-${Date.now()}`;

  let resolveReceived;
  let rejectReceived;

  const received = new Promise((resolve, reject) => {
    resolveReceived = resolve;
    rejectReceived = reject;
  });

  console.log("Kafka integration test starting");
  console.log(`Brokers: ${brokers.join(", ")}`);
  console.log(`Topic: ${TOPIC}`);
  console.log(`Group: ${groupId}`);

  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: TOPIC,
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
    });
    console.log("Topic is ready");

    consumerConnection = await startJsonConsumer({
      client,
      groupId,
      topic: TOPIC,
      fromBeginning: true,
      async onMessage(event, payload) {
        if (event.eventId !== eventId) return;

        try {
          assert.equal(event.eventType, "order.created");
          assert.equal(event.eventVersion, 1);
          assert.equal(event.source, "order-service");
          assert.equal(event.data.orderId, "order-integration-1");
          assert.equal(payload.message.key.toString(), "order-integration-1");
          resolveReceived(event);
        } catch (error) {
          rejectReceived(error);
        }
      },
    });

    producerConnection = await connectKafkaProducer(client);

    const event = {
      eventId,
      eventType: "order.created",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      source: "order-service",
      correlationId: "kafka-integration-test",
      data: {
        orderId: "order-integration-1",
        customerId: "customer-integration-1",
      },
    };

    await producerConnection.publish(
      TOPIC,
      event.data.orderId,
      event,
    );
    console.log("Event published");

    await withTimeout(received, 20_000);
    console.log("Event consumed and validated");
    console.log("Kafka client checks passed");
  } finally {
    await consumerConnection?.close().catch(() => undefined);
    await producerConnection?.close().catch(() => undefined);
    await admin.disconnect().catch(() => undefined);
    console.log("Kafka connections closed");
  }
}

main().catch((error) => {
  console.error("Kafka integration test failed");
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});