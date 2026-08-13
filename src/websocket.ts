import { randomUUID } from "node:crypto";
import { IncomingMessage, Server as HttpServer } from "node:http";
import { Duplex } from "node:stream";
import WebSocket, {
  ClientOptions,
  RawData,
  ServerOptions,
  WebSocketServer,
} from "ws";
import { ClientLogger, createConsoleLogger } from "./logger";

const defaultLogger = createConsoleLogger("websocket-client");

export interface WebSocketPrincipal {
  userId: string;
  roles: string[];
}

export interface RealtimeEvent<T = unknown> {
  type: "event";
  eventId: string;
  eventType: string;
  eventVersion: number;
  occurredAt: string;
  channel: string;
  data: T;
}

export type ClientRealtimeMessage =
  | {
    type: "subscribe";
    requestId: string;
    channel: string;
  }
  | {
    type: "unsubscribe";
    requestId: string;
    channel: string;
  };

export type ServerRealtimeMessage =
  | RealtimeEvent
  | {
    type: "ack";
    requestId: string;
    action: "subscribe" | "unsubscribe";
    channel: string;
  }
  | {
    type: "error";
    requestId?: string;
    code: string;
    message: string;
  };

export interface RealtimeServerConfig {
  server: HttpServer;
  path: string;
  authenticate(request: IncomingMessage):
    | Promise<WebSocketPrincipal | null>
    | WebSocketPrincipal
    | null;
  authorizeChannel(
    principal: WebSocketPrincipal,
    channel: string,
  ): Promise<boolean> | boolean;
  logger?: ClientLogger;
  heartbeatIntervalMs?: number;
  maxPayloadBytes?: number;
  maxBufferedBytes?: number;
}

export interface RealtimeServer {
  webSocketServer: WebSocketServer;
  publish<T>(event: RealtimeEvent<T>): number;
  connectionCount(): number;
  close(): Promise<void>;
}

interface ConnectionState {
  connectionId: string;
  principal: WebSocketPrincipal;
  channels: Set<string>;
  alive: boolean;
}

function parseJson(data: RawData): unknown {
  return JSON.parse(data.toString());
}

function isClientMessage(value: unknown): value is ClientRealtimeMessage {
  if (!value || typeof value !== "object") return false;

  const message = value as Record<string, unknown>;

  return (
    (message.type === "subscribe" || message.type === "unsubscribe") &&
    typeof message.requestId === "string" &&
    message.requestId.length > 0 &&
    typeof message.channel === "string" &&
    message.channel.length > 0
  );
}

function sendJson(socket: WebSocket, message: ServerRealtimeMessage): boolean {
  if (socket.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify(message));
  return true;
}

function rejectUpgrade(
  socket: Duplex,
  status: "401 Unauthorized" | "404 Not Found",
): void {
  socket.write(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

export function attachRealtimeServer(
  config: RealtimeServerConfig,
): RealtimeServer {
  if (!config.path.startsWith("/")) {
    throw new Error("WebSocket path must start with /");
  }

  const logger = config.logger ?? defaultLogger;
  const heartbeatIntervalMs = config.heartbeatIntervalMs ?? 30_000;
  const maxBufferedBytes = config.maxBufferedBytes ?? 1_000_000;

  const serverOptions: ServerOptions = {
    noServer: true,
    clientTracking: true,
    maxPayload: config.maxPayloadBytes ?? 64 * 1024,
    perMessageDeflate: false,
  };

  const webSocketServer = new WebSocketServer(serverOptions);
  const states = new WeakMap<WebSocket, ConnectionState>();
  const authenticatedPrincipals = new WeakMap<WebSocket, WebSocketPrincipal>();

  const onUpgrade = async (
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer,
  ): Promise<void> => {
    const requestUrl = new URL(
      request.url ?? "/",
      `http://${request.headers.host ?? "localhost"}`,
    );

    if (requestUrl.pathname !== config.path) {
      rejectUpgrade(socket, "404 Not Found");
      return;
    }

    let principal: WebSocketPrincipal | null;

    try {
      principal = await config.authenticate(request);
    } catch (error) {
      logger.error("WebSocket authentication failed", error);
      rejectUpgrade(socket, "401 Unauthorized");
      return;
    }

    if (!principal) {
      logger.warn("WebSocket upgrade rejected");
      rejectUpgrade(socket, "401 Unauthorized");
      return;
    }

    webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
      authenticatedPrincipals.set(webSocket, principal);
      webSocketServer.emit("connection", webSocket, request);
    });
  };

  config.server.on("upgrade", onUpgrade);

  webSocketServer.on("connection", (socket: WebSocket) => {
    const principal = authenticatedPrincipals.get(socket);

    if (!principal) {
      socket.close(1011, "Authentication state missing");
      return;
    }

    const state: ConnectionState = {
      connectionId: randomUUID(),
      principal,
      channels: new Set(),
      alive: true,
    };

    states.set(socket, state);

    logger.info("WebSocket connected", {
      connectionId: state.connectionId,
      userId: principal.userId,
    });

    socket.on("pong", () => {
      state.alive = true;
    });

    socket.on("error", (error) => {
      logger.error("WebSocket connection error", error, {
        connectionId: state.connectionId,
        userId: principal.userId,
      });
    });

    socket.on("message", async (data, isBinary) => {
      if (isBinary) {
        sendJson(socket, {
          type: "error",
          code: "BINARY_NOT_SUPPORTED",
          message: "Only JSON text messages are supported",
        });
        return;
      }

      let message: unknown;

      try {
        message = parseJson(data);
      } catch {
        sendJson(socket, {
          type: "error",
          code: "INVALID_JSON",
          message: "Message must contain valid JSON",
        });
        return;
      }

      if (!isClientMessage(message)) {
        sendJson(socket, {
          type: "error",
          code: "INVALID_MESSAGE",
          message: "Unsupported message format",
        });
        return;
      }

      if (message.type === "subscribe") {
        const allowed = await config.authorizeChannel(
          principal,
          message.channel,
        );

        if (!allowed) {
          sendJson(socket, {
            type: "error",
            requestId: message.requestId,
            code: "FORBIDDEN_CHANNEL",
            message: "You cannot subscribe to this channel",
          });
          return;
        }

        state.channels.add(message.channel);
      } else {
        state.channels.delete(message.channel);
      }

      sendJson(socket, {
        type: "ack",
        requestId: message.requestId,
        action: message.type,
        channel: message.channel,
      });

      logger.debug("WebSocket subscription changed", {
        connectionId: state.connectionId,
        userId: principal.userId,
        action: message.type,
        channel: message.channel,
      });
    });

    socket.on("close", (code) => {
      logger.info("WebSocket disconnected", {
        connectionId: state.connectionId,
        userId: principal.userId,
        closeCode: code,
      });
    });
  });

  const heartbeat = setInterval(() => {
    for (const socket of webSocketServer.clients) {
      const state = states.get(socket);
      if (!state) continue;

      if (!state.alive) {
        logger.warn("Terminating unresponsive WebSocket", {
          connectionId: state.connectionId,
          userId: state.principal.userId,
        });
        socket.terminate();
        continue;
      }

      state.alive = false;
      socket.ping();
    }
  }, heartbeatIntervalMs);

  heartbeat.unref();

  return {
    webSocketServer,
    publish(event) {
      let delivered = 0;

      for (const socket of webSocketServer.clients) {
        const state = states.get(socket);

        if (!state?.channels.has(event.channel)) continue;

        if (socket.bufferedAmount > maxBufferedBytes) {
          logger.warn("Closing slow WebSocket client", {
            connectionId: state.connectionId,
            userId: state.principal.userId,
            bufferedBytes: socket.bufferedAmount,
          });
          socket.close(1013, "Client is too slow");
          continue;
        }

        if (sendJson(socket, event)) delivered += 1;
      }

      logger.debug("WebSocket event published", {
        eventId: event.eventId,
        eventType: event.eventType,
        channel: event.channel,
        delivered,
      });

      return delivered;
    },
    connectionCount: () => webSocketServer.clients.size,
    async close() {
      clearInterval(heartbeat);
      config.server.off("upgrade", onUpgrade);

      for (const socket of webSocketServer.clients) {
        socket.close(1001, "Server shutting down");
      }

      await new Promise<void>((resolve, reject) => {
        webSocketServer.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });

      logger.info("WebSocket server closed");
    },
  };
}

export function createNodeWebSocketClient(
  url: string,
  options?: ClientOptions,
): WebSocket {
  return new WebSocket(url, options);
}