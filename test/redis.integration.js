const assert = require("node:assert/strict");
const { connectRedis } = require("../dist");

const TEST_KEY = "foodpulse:clients:integration";

function getSafeRedisEndpoint(url) {
  const parsed = new URL(url);
  return `${parsed.hostname}:${parsed.port || "6379"}`;
}

async function main() {
  const url = process.env.REDIS_URL;

  if (!url) {
    throw new Error(
      "REDIS_URL is missing. Add it to the ignored foodpulse-clients/.env file.",
    );
  }

  const errors = [];

  const config = {
    url,
    clientName: "foodpulse-clients-test",
    options: {
      socket: {
        connectTimeout: 10_000,
        reconnectStrategy: false,
      },
    },
    onError(error) {
      errors.push(error.message);
    },
  };

  console.log("Redis integration test starting");
  console.log(`Endpoint: ${getSafeRedisEndpoint(config.url)}`);
  console.log(`Client name: ${config.clientName}`);

  const connection = await connectRedis(config);

  try {
    assert.equal(connection.client.isOpen, true);
    assert.equal(connection.client.isReady, true);
    console.log("Connection state passed");

    assert.equal(await connection.ping(), "PONG");
    console.log("Ping passed");

    await connection.client.set(TEST_KEY, "working", { EX: 30 });
    assert.equal(await connection.client.get(TEST_KEY), "working");
    console.log("SET and GET passed");

    const ttl = await connection.client.ttl(TEST_KEY);
    assert.equal(ttl > 0 && ttl <= 30, true);
    console.log(`TTL passed: ${ttl} seconds`);

    assert.equal(await connection.client.del(TEST_KEY), 1);
    assert.equal(await connection.client.get(TEST_KEY), null);
    console.log("DEL passed");

    assert.deepEqual(errors, []);
    console.log("Redis client checks passed");
  } finally {
    if (connection.client.isOpen) {
      await connection.client.del(TEST_KEY).catch(() => undefined);
    }

    await connection.close();
    console.log("Redis connection closed");
  }

  assert.equal(connection.client.isOpen, false);
  console.log("Closed-state check passed");
}

main().catch((error) => {
  console.error("Redis integration test failed");
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});