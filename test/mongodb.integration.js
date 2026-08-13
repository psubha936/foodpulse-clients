const assert = require("node:assert/strict");
const { connectMongo } = require("../dist");

function getSafeClusterHost(uri) {
  const withoutProtocol = uri.replace(/^mongodb(?:\+srv)?:\/\//, "");
  const afterCredentials = withoutProtocol.includes("@")
    ? withoutProtocol.slice(withoutProtocol.lastIndexOf("@") + 1)
    : withoutProtocol;

  return afterCredentials.split(/[/?]/, 1)[0];
}

async function main() {
  const uri = process.env.MONGODB_URI;

  if (!uri) {
    throw new Error(
      "MONGODB_URI is missing. Copy .env.example to .env and add your Atlas values.",
    );
  }

  // This is the complete input passed to the TypeScript MongoDB client method.
  // Only the secret URI comes from .env; safe application settings are written
  // directly in this test.
  const config = {
    uri,
    databaseName: "foodpulse",
    appName: "foodpulse-clients-test",
    options: {
      serverSelectionTimeoutMS: 10_000,
      maxPoolSize: 5,
      minPoolSize: 0,
    },
  };

  console.log("MongoDB Atlas integration test starting");
  console.log(`Cluster: ${getSafeClusterHost(config.uri)}`);
  console.log(`Database: ${config.databaseName}`);
  console.log(`Application: ${config.appName}`);
  console.log(`Maximum pool size: ${config.options.maxPoolSize}`);

  // Directly call the method exported by src/mongodb.ts through dist/index.js.
  const connection = await connectMongo(config);

  try {
    console.log("Connected to MongoDB Atlas");

    await connection.ping();
    console.log("Ping passed");

    assert.equal(connection.db.databaseName, config.databaseName);
    console.log("Database selection passed");

    const collections = await connection.db
      .listCollections({}, { nameOnly: true })
      .toArray();

    console.log(
      `Collections (${collections.length}): ${
        collections.map(({ name }) => name).join(", ") || "none"
      }`,
    );

    console.log("MongoDB client checks passed");
  } finally {
    await connection.close();
    console.log("MongoDB connection closed");
  }
}

main().catch((error) => {
  console.error("MongoDB integration test failed");
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
