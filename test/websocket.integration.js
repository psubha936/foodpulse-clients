const assert = require("node:assert/strict");
const { createServer } = require("node:http");
const {
  attachRealtimeServer,
  createNodeWebSocketClient,
} = require("../dist");

function waitForEvent(target, eventName) {
  return new Promise((resolve, reject) => {
    target.once(eventName, resolve);
    target.once("error", reject);
  });
}

function waitForJson(socket, predicate, timeoutMs = 5_000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("Timed out waiting for WebSocket message")),
      timeoutMs,
    );

    const onMessage = (data) => {
      const message = JSON.parse(data.toString());
      if (!predicate(message)) return;

      clearTimeout(timer);
      socket.off("message", onMessage);
      resolve(message);
    };

    socket.on("message", onMessage);
  });
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("HTTP server did not return a TCP address");
  }

  return address.port;
}

async function main() {
  const httpServer = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "up" }));
  });

  const realtime = attachRealtimeServer({
    server: httpServer,
    path: "/ws",
    heartbeatIntervalMs: 1_000,
    maxPayloadBytes: 16 * 1024,
    maxBufferedBytes: 64 * 1024,
    authenticate(request) {
      if (request.headers.authorization !== "Bearer integration-ticket") {
        return null;
      }

      return {
        userId: "customer-integration-1",
        roles: ["customer"],
      };
    },
    authorizeChannel(principal, channel) {
      return (
        principal.userId === "customer-integration-1" &&
        channel === "order:order-integration-1"
      );
    },
  });

  let socket;

  try {
    const port = await listen(httpServer);
    console.log(`WebSocket test server listening on 127.0.0.1:${port}`);

    socket = createNodeWebSocketClient(`ws://127.0.0.1:${port}/ws`, {
      headers: {
        authorization: "Bearer integration-ticket",
      },
    });

    await waitForEvent(socket, "open");
    assert.equal(realtime.connectionCount(), 1);
    console.log("Authenticated connection passed");

    const ackPromise = waitForJson(
      socket,
      (message) =>
        message.type === "ack" && message.requestId === "subscribe-1",
    );

    socket.send(
      JSON.stringify({
        type: "subscribe",
        requestId: "subscribe-1",
        channel: "order:order-integration-1",
      }),
    );

    const ack = await ackPromise;
    assert.equal(ack.action, "subscribe");
    console.log("Authorized subscription passed");

    const eventPromise = waitForJson(
      socket,
      (message) =>
        message.type === "event" && message.eventId === "event-integration-1",
    );

    const delivered = realtime.publish({
      type: "event",
      eventId: "event-integration-1",
      eventType: "order.status.changed",
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      channel: "order:order-integration-1",
      data: {
        orderId: "order-integration-1",
        status: "CONFIRMED",
      },
    });

    assert.equal(delivered, 1);
    const event = await eventPromise;
    assert.equal(event.data.status, "CONFIRMED");
    console.log("Realtime event delivery passed");
  } finally {
    socket?.close();
    await realtime.close();
    await new Promise((resolve) => httpServer.close(resolve));
    console.log("WebSocket and HTTP servers closed");
  }
}

main().catch((error) => {
  console.error("WebSocket integration test failed");
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});