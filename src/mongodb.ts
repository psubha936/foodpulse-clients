import { Db, MongoClient, MongoClientOptions, ServerApiVersion } from "mongodb";
import { ClientLogger, createConsoleLogger } from "./logger";

const defaultLogger = createConsoleLogger("mongodb-client");

export interface MongoConnectionConfig {
  uri: string;
  databaseName: string;
  appName: string;
  options?: MongoClientOptions;
  logger?: ClientLogger;
}

export interface MongoConnection {
  client: MongoClient;
  db: Db;
  ping(): Promise<void>;
  close(): Promise<void>;
}


export async function connectMongo(config: MongoConnectionConfig): Promise<MongoConnection> {
  if (!config.uri.trim()) {
    throw new Error("MongoDB URI is required");
  }

  if (!config.databaseName.trim()) {
    throw new Error("MongoDB database name is required");
  }

  if (!config.appName.trim()) {
    throw new Error("MongoDB app name is required");
  }

  const logger = config.logger ?? defaultLogger;
  const logContext = {
    appName: config.appName,
    databaseName: config.databaseName,
  };

  logger.info("Connecting to MongoDB", logContext);

  const clientOptions = Object.assign(
    {} as MongoClientOptions,
    config.options,
  );

  clientOptions.appName = config.appName;

  clientOptions.serverApi ??= {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  };

  clientOptions.serverSelectionTimeoutMS ??= 10_000;

  const client = new MongoClient(config.uri, clientOptions);
  const db = client.db(config.databaseName);

  try {
    await client.connect();
    await client.db("admin").command({ ping: 1 });
    logger.info("Connected to MongoDB", logContext);
  } catch (error) {
    logger.error("Failed to connect to MongoDB", error, logContext);
    await client.close().catch(() => undefined);
    throw error;
  }

  return {
    client,
    db,
    async ping() {
      await client.db("admin").command({ ping: 1 });
      logger.debug("MongoDB ping passed", logContext);
    },
    async close() {
      await client.close();
      logger.info("MongoDB connection closed", logContext);
    },
  };
}
