import { Admin, Consumer, ConsumerConfig, EachMessagePayload, Kafka, KafkaConfig, logLevel, Producer, ProducerConfig } from "kafkajs";
import { ClientLogger, createConsoleLogger } from "./logger";

const defaultLogger = createConsoleLogger("kafka-client");

export interface KafkaClientConfig {
  clientId: string;
  brokers: string[];
  options?: Omit<KafkaConfig, "clientId" | "brokers">;
  logger?: ClientLogger;
}

export interface FoodPulseEvent<T> {
  eventId: string;
  eventType: string;
  eventVersion: number;
  occurredAt: string;
  source: string;
  correlationId: string;
  data: T;
}

export interface KafkaClient {
  kafka: Kafka;
  logger: ClientLogger;
  clientId: string;
  createAdmin(): Admin;
}

export interface KafkaProducerConnection {
  producer: Producer;
  publish<T>(
    topic: string,
    key: string,
    event: FoodPulseEvent<T>,
  ): Promise<void>;
  close(): Promise<void>;
}

export interface KafkaConsumerConnection {
  consumer: Consumer;
  close(): Promise<void>;
}

function requireText(value: string, field: string): void {
  if (!value.trim()) {
    throw new Error(`${field} is required`);
  }
}

export function createKafkaClient(config: KafkaClientConfig): KafkaClient {
  requireText(config.clientId, "Kafka clientId");
  if (!config.brokers || config.brokers.length === 0) {
    throw new Error("Kafka brokers are required");
  }
  config.brokers.forEach((broker) => requireText(broker, "Kafka broker"));
  const logger = config.logger ?? defaultLogger;

  const kafka = new Kafka({
    clientId: config.clientId,
    brokers: [...config.brokers],
    ...config.options,
    logLevel: config.options?.logLevel ?? logLevel.NOTHING,
  });

  logger.info("Kafka client created", {
    clientId: config.clientId,
    brokerCount: config.brokers.length,
  });

  return {
    kafka,
    logger,
    clientId: config.clientId,
    createAdmin: () => kafka.admin(),
  };
}

export async function connectKafkaProducer(client: KafkaClient, options: ProducerConfig = {}): Promise<KafkaProducerConnection> {
  const producer = client.kafka.producer({
    ...options,
    allowAutoTopicCreation: false,
  });

  const context = { clientId: client.clientId };
  client.logger.info("Connecting Kafka producer", context);

  try {
    await producer.connect();
    client.logger.info("Kafka producer connected", context);
  } catch (error) {
    client.logger.error("Failed to connect Kafka producer", error, context);
    await producer.disconnect().catch(() => undefined);
    throw error;
  }

  return {
    producer,
    async publish<T>(topic: string, key: string, event: FoodPulseEvent<T>): Promise<void> {
      requireText(topic, "Kafka topic");
      requireText(key, "Kafka message key");
      requireText(event.eventId, "Kafka eventId");
      requireText(event.eventType, "Kafka eventType");

      await producer.send({
        topic,
        acks: -1,
        messages: [
          {
            key,
            value: JSON.stringify(event),
            headers: {
              "event-id": event.eventId,
              "event-type": event.eventType,
              "event-version": event.eventVersion.toString(),
              "source": event.source,
              "correlation-id": event.correlationId,
            }
          }
        ]
      });

      client.logger.info("Kafka event published", {
        clientId: client.clientId,
        topic,
        eventId: event.eventId,
        eventType: event.eventType,
        messageKey: key,
      });
    },
    async close() {
      await producer.disconnect();
      client.logger.info("Kafka producer disconnected", context);
    },
  };
}

export interface StartJsonConsumerConfig<T> {
  client: KafkaClient;
  groupId: string;
  topic: string;
  fromBeginning?: boolean;
  consumerOptions?: Omit<ConsumerConfig, "groupId">;
  onMessage(
    event: FoodPulseEvent<T>,
    payload: EachMessagePayload,
  ): Promise<void> | void;
}

export async function startJsonConsumer<T>(config: StartJsonConsumerConfig<T>): Promise<KafkaConsumerConnection> {
  requireText(config.groupId, "Kafka consumer groupId");
  requireText(config.topic, "Kafka consumer topic");

  const consumer = config.client.kafka.consumer({
    groupId: config.groupId,
    ...config.consumerOptions,
  });

  const context = {
    clientId: config.client.clientId,
    groupId: config.groupId,
    topic: config.topic,
  };

  config.client.logger.info("Connecting Kafka consumer", context);

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    config.client.logger.info("Kafka consumer group joined", {
      ...context,
      memberId: payload.memberId,
      leaderId: payload.leaderId,
      isLeader: payload.isLeader,
    });
  });

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    config.client.logger.error("Kafka consumer crashed", payload.error, { ...context, restart: payload.restart });
  });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: config.topic, fromBeginning: config.fromBeginning ?? false });
    await consumer.run({
      eachMessage: async (payload) => {
        const { message } = payload;

        const value = message.value?.toString();
        if (!value) {
          config.client.logger.warn("Kafka consumer received empty message", context);
          throw new Error("Kafka message value is empty");
        }

        let event: FoodPulseEvent<T>;
        try {
          event = JSON.parse(value) as FoodPulseEvent<T>;
        } catch (error) {
          config.client.logger.error("Kafka consumer failed to parse message", error, context);
          throw new Error("Failed to parse Kafka message value as JSON");
        }

        await config.onMessage(event, payload);
        config.client.logger.debug("Kafka message processed", {
          ...context,
          eventId: event.eventId,
          eventType: event.eventType,
          partition: payload.partition,
          offset: payload.message.offset,
        });
      }
    });

    config.client.logger.info("Kafka consumer started", context);
  } catch (error) {
    config.client.logger.error("Failed to start Kafka consumer", error, context);
    await consumer.disconnect().catch(() => undefined);
    throw error;
  }
  return {
    consumer,
    async close() {
      await consumer.stop();
      await consumer.disconnect();
      config.client.logger.info("Kafka consumer disconnected", context);
    },
  };
}