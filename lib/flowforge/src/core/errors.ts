export class FlowForgeTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

export function toError(value: unknown): Error {
  if (value instanceof Error) return value;
  if (typeof value === "string") return new Error(value);
  return new Error("Workflow task failed with a non-Error value.", { cause: value });
}

export function serializeError(error: Error): {
  name: string;
  message: string;
  stack?: string;
} {
  return {
    name: error.name,
    message: error.message,
    ...(error.stack ? { stack: error.stack } : {}),
  };
}
