const assert = require("node:assert/strict");
const test = require("node:test");
const { createConsoleLogger } = require("../dist");

test("client logger redacts connection credentials", () => {
  const output = [];
  const originalLog = console.log;

  console.log = (line) => output.push(line);

  try {
    const logger = createConsoleLogger("logger-test");
    logger.info("Testing redaction", {
      uri: "mongodb+srv://real-user:real-password@example.mongodb.net/foodpulse",
      endpoint: "redis://default:redis-password@127.0.0.1:16379",
      password: "another-password",
    });
  } finally {
    console.log = originalLog;
  }

  assert.equal(output.length, 1);
  assert.equal(output[0].includes("real-user"), false);
  assert.equal(output[0].includes("real-password"), false);
  assert.equal(output[0].includes("redis-password"), false);
  assert.equal(output[0].includes("another-password"), false);
  assert.equal(output[0].includes("<redacted>"), true);
});
