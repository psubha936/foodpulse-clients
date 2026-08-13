import { createClient, RedisClientOptions, RedisClientType } from "redis";
import { ClientLogger, createConsoleLogger } from "./logger";

const defaultLogger = createConsoleLogger("redis-client");

export interface RedisConnectionConfig {
  url: string;
  clientName: string;
  options?: RedisClientOptions;
  onError?: (error: Error) => void;
  logger?: ClientLogger;
}

export interface RedisConnection {
  client: RedisClientType;
  ping(): Promise<string>;
  close(): Promise<void>;
}

export async function connectRedis(config: RedisConnectionConfig): Promise<RedisConnection> {
  if (!config.url.trim()) {
    throw new Error("Redis URL is required");
  }

  if (!config.clientName.trim()) {
    throw new Error("Redis client name is required");
  }

  const logger = config.logger ?? defaultLogger;
  const logContext = { clientName: config.clientName };

  logger.info("Connecting to Redis", logContext);

  const client = createClient({
    ...config.options,
    url: config.url,
    name: config.clientName,
  });

  client.on("error", (error) => {
    logger.error("Redis client error", error, logContext);

    if (config.onError) {
      config.onError(error);
    }
  });

  client.on("reconnecting", () => {
    logger.warn("Redis client reconnecting", logContext);
  });

  try {
    await client.connect();
    await client.ping();
    logger.info("Connected to Redis", logContext);
  } catch (error) {
    logger.error("Failed to connect to Redis", error, logContext);
    if (client.isOpen) {
      client.destroy();
    }
    throw error;
  }

  return {
    client: client as RedisClientType,
    async ping() {
      const response = await client.ping();
      logger.debug("Redis ping passed", logContext);
      return response;
    },
    async close() {
      if (client.isOpen) {
        await client.close();
      }
      logger.info("Redis connection closed", logContext);
    },
  };
}
