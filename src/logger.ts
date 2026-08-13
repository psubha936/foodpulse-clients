export type LogContext = Record<string, unknown>;

export interface ClientLogger {
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, error?: unknown, context?: LogContext): void;
}

type LogLevel = "debug" | "info" | "warn" | "error";

const SECRET_KEY_PATTERN =
  /password|secret|token|credential|authorization|access.?key|private.?key|uri|url/i;

function redactText(value: string): string {
  return value
    .replace(
      /(mongodb(?:\+srv)?|rediss?):\/\/[^@\s]+@/gi,
      "$1://<redacted>@",
    )
    .replace(
      /([?&](?:password|token|secret)=)[^&\s]+/gi,
      "$1<redacted>",
    );
}

function sanitizeContext(context?: LogContext): LogContext | undefined {
  if (!context) return undefined;

  return Object.fromEntries(
    Object.entries(context).map(([key, value]) => {
      if (SECRET_KEY_PATTERN.test(key)) return [key, "<redacted>"];
      if (typeof value === "string") return [key, redactText(value)];
      return [key, value];
    }),
  );
}

function errorDetails(error: unknown): LogContext | undefined {
  if (error instanceof Error) {
    return {
      errorName: error.name,
      errorMessage: redactText(error.message),
    };
  }

  if (error !== undefined) {
    return { errorMessage: redactText(String(error)) };
  }

  return undefined;
}

export function createConsoleLogger(component: string): ClientLogger {
  const write = (
    level: LogLevel,
    message: string,
    context?: LogContext,
    error?: unknown,
  ): void => {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      component,
      message,
      ...sanitizeContext(context),
      ...errorDetails(error),
    };

    const output = JSON.stringify(entry);

    if (level === "error") {
      console.error(output);
    } else if (level === "warn") {
      console.warn(output);
    } else {
      console.log(output);
    }
  };

  return {
    debug: (message, context) => write("debug", message, context),
    info: (message, context) => write("info", message, context),
    warn: (message, context) => write("warn", message, context),
    error: (message, error, context) =>
      write("error", message, context, error),
  };
}
