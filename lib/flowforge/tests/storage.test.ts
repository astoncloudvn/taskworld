import { describe, expect, it } from "vitest";
import { MemoryStorageAdapter } from "../src/storage/memory.js";
import { RedisStorageAdapter } from "../src/storage/redis.js";
import { PostgresStorageAdapter } from "../src/storage/postgres.js";
import type { ExecutionRecord } from "../src/types.js";

function record(id: string, workflowId = "sample"): ExecutionRecord {
  return {
    id,
    workflowId,
    status: "succeeded",
    input: {},
    output: id,
    metadata: {},
    steps: [],
    logs: [],
    progress: 100,
    createdAt: new Date(0).toISOString(),
    finishedAt: new Date(0).toISOString(),
  };
}

describe("memory storage", () => {
  it("isolates stored values and deletes records", async () => {
    const storage = new MemoryStorageAdapter();
    const item = record("one");
    await storage.save(item);
    item.metadata.changed = true;

    expect((await storage.get("one"))?.metadata.changed).toBeUndefined();
    expect(await storage.delete("one")).toBe(true);
    expect(await storage.get("one")).toBeUndefined();
  });

  it("evicts the oldest records at its configured retention limit", async () => {
    const storage = new MemoryStorageAdapter({ maxRecords: 2 });
    await storage.save(record("oldest"));
    await storage.save({ ...record("middle"), createdAt: new Date(1).toISOString() });
    await storage.save({ ...record("newest"), createdAt: new Date(2).toISOString() });

    expect(await storage.get("oldest")).toBeUndefined();
    expect((await storage.list()).map((item) => item.id)).toEqual(["newest", "middle"]);
  });
});

describe("Redis adapter", () => {
  it("stores records and maintains history indexes with Redis commands", async () => {
    const calls: string[][] = [];
    const values = new Map<string, string>();
    const client = {
      async sendCommand(command: string[]): Promise<unknown> {
        calls.push(command);
        if (command[0] === "SET") {
          values.set(command[1]!, command[2]!);
          return "OK";
        }
        if (command[0] === "GET") return values.get(command[1]!) ?? null;
        if (command[0] === "ZADD" || command[0] === "EXPIRE") return 1;
        if (command[0] === "ZREVRANGE") return ["redis-id"];
        if (command[0] === "DEL") {
          values.delete(command[1]!);
          return 1;
        }
        if (command[0] === "ZREM") return 1;
        return null;
      },
    };
    const storage = new RedisStorageAdapter(client);
    await storage.save(record("redis-id"));

    expect((await storage.get("redis-id"))?.workflowId).toBe("sample");
    expect((await storage.list())[0]?.id).toBe("redis-id");
    expect(await storage.delete("redis-id")).toBe(true);
    expect(calls.filter((command) => command[0] === "ZADD")).toHaveLength(2);
  });
});

describe("PostgreSQL adapter", () => {
  it("creates the schema lazily and persists parameterized JSON records", async () => {
    const calls: Array<{ sql: string; values?: unknown[] }> = [];
    const rows = new Map<string, ExecutionRecord>();
    const client = {
      async query(sql: string, values?: unknown[]) {
        calls.push({ sql, values });
        if (sql.startsWith("INSERT")) {
          rows.set(String(values?.[0]), JSON.parse(String(values?.[3])) as ExecutionRecord);
        }
        if (sql.startsWith("SELECT")) {
          const found = rows.get(String(values?.[0]));
          return { rows: found ? [{ record: found }] : [], rowCount: found ? 1 : 0 };
        }
        if (sql.startsWith("DELETE")) {
          const deleted = rows.delete(String(values?.[0]));
          return { rows: [], rowCount: deleted ? 1 : 0 };
        }
        return { rows: [], rowCount: 1 };
      },
    };
    const storage = new PostgresStorageAdapter(client);
    await storage.save(record("pg-id"));
    expect((await storage.get("pg-id"))?.id).toBe("pg-id");
    expect(calls[0]?.sql).toContain("CREATE TABLE IF NOT EXISTS");
    expect(calls[1]?.values?.[0]).toBe("pg-id");
    expect(await storage.delete("pg-id")).toBe(true);
  });

  it("rejects unsafe table names", () => {
    const client = { query: async () => ({ rows: [] }) };
    expect(() => new PostgresStorageAdapter(client, { tableName: "x; DROP TABLE users" }))
      .toThrow(/plain SQL identifier/);
  });
});
