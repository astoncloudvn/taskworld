import type { ExecutionRecord } from "../types.js";
import type { ExecutionQuery, StorageAdapter } from "./adapter.js";
import { normalizeLimit } from "./memory.js";

/**
 * Minimal Redis command interface implemented by node-redis clients.
 * FlowForge does not import or require a Redis package.
 */
export interface RedisCommandClient {
  sendCommand(command: string[]): Promise<unknown>;
}

export interface RedisStorageOptions {
  keyPrefix?: string;
  ttlSeconds?: number;
}

/** Redis-backed execution history using JSON records and sorted-set indexes. */
export class RedisStorageAdapter implements StorageAdapter {
  private readonly prefix: string;
  private readonly ttlSeconds?: number;

  constructor(
    private readonly client: RedisCommandClient,
    options: RedisStorageOptions = {},
  ) {
    this.prefix = options.keyPrefix ?? "flowforge";
    if (!this.prefix.trim()) throw new Error("Redis keyPrefix cannot be empty.");
    if (
      options.ttlSeconds !== undefined &&
      (!Number.isInteger(options.ttlSeconds) || options.ttlSeconds < 1)
    ) {
      throw new RangeError("Redis ttlSeconds must be a positive integer.");
    }
    this.ttlSeconds = options.ttlSeconds;
  }

  async save(record: ExecutionRecord): Promise<void> {
    const recordKey = this.recordKey(record.id);
    const serialized = JSON.stringify(record);
    if (this.ttlSeconds) {
      await this.command(["SET", recordKey, serialized, "EX", String(this.ttlSeconds)]);
    } else {
      await this.command(["SET", recordKey, serialized]);
    }
    const score = String(Date.parse(record.createdAt));
    const indexes = [this.indexKey(), this.indexKey(record.workflowId)];
    await Promise.all(
      indexes.map((key) => this.command(["ZADD", key, score, record.id])),
    );
    if (this.ttlSeconds) {
      await Promise.all(
        indexes.map((key) =>
          this.command(["EXPIRE", key, String(this.ttlSeconds)]),
        ),
      );
    }
  }

  async get(executionId: string): Promise<ExecutionRecord | undefined> {
    const value = await this.command(["GET", this.recordKey(executionId)]);
    if (typeof value !== "string") return undefined;
    return parseRecord(value);
  }

  async list(query: ExecutionQuery = {}): Promise<ExecutionRecord[]> {
    const limit = normalizeLimit(query.limit);
    const index = this.indexKey(query.workflowId);
    const ids = await this.command(["ZREVRANGE", index, "0", String(limit - 1)]);
    if (!Array.isArray(ids)) {
      throw new Error("Redis ZREVRANGE returned an unexpected response.");
    }
    const records = await Promise.all(
      ids.map(async (id) => {
        if (typeof id !== "string") return undefined;
        return this.get(id);
      }),
    );
    return records.filter((record): record is ExecutionRecord => record !== undefined);
  }

  async delete(executionId: string): Promise<boolean> {
    const record = await this.get(executionId);
    if (!record) return false;
    const deleted = await this.command(["DEL", this.recordKey(executionId)]);
    await Promise.all([
      this.command(["ZREM", this.indexKey(), executionId]),
      this.command(["ZREM", this.indexKey(record.workflowId), executionId]),
    ]);
    return Number(deleted) > 0;
  }

  private recordKey(id: string): string {
    return `${this.prefix}:execution:${encodeURIComponent(id)}`;
  }

  private indexKey(workflowId?: string): string {
    return workflowId
      ? `${this.prefix}:history:workflow:${encodeURIComponent(workflowId)}`
      : `${this.prefix}:history:all`;
  }

  private async command(parts: string[]): Promise<unknown> {
    return this.client.sendCommand(parts);
  }
}

function parseRecord(serialized: string): ExecutionRecord {
  try {
    const value: unknown = JSON.parse(serialized);
    if (!value || typeof value !== "object" || !("id" in value)) {
      throw new Error("record is missing its id");
    }
    return value as ExecutionRecord;
  } catch (error) {
    throw new Error("Could not parse a FlowForge execution from Redis.", {
      cause: error,
    });
  }
}
