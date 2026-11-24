import type { ExecutionRecord } from "../types.js";
import type { ExecutionQuery, StorageAdapter } from "./adapter.js";
import { normalizeLimit } from "./memory.js";

export interface SqlQueryResult {
  rows: Array<Record<string, unknown>>;
  rowCount?: number | null;
}

/** Structural subset implemented by node-postgres Pool and Client instances. */
export interface SqlQueryClient {
  query(text: string, values?: unknown[]): Promise<SqlQueryResult>;
}

export interface PostgresStorageOptions {
  tableName?: string;
}

/**
 * PostgreSQL-backed execution history. The table is created lazily on first
 * use; the query client remains owned and closed by the application.
 */
export class PostgresStorageAdapter implements StorageAdapter {
  private readonly table: string;
  private schemaPromise?: Promise<void>;

  constructor(
    private readonly client: SqlQueryClient,
    options: PostgresStorageOptions = {},
  ) {
    const tableName = options.tableName ?? "flowforge_executions";
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
      throw new Error("PostgreSQL tableName must be a plain SQL identifier.");
    }
    this.table = `"${tableName}"`;
  }

  async save(record: ExecutionRecord): Promise<void> {
    await this.ensureSchema();
    await this.client.query(
      `INSERT INTO ${this.table} (id, workflow_id, created_at, record)
       VALUES ($1, $2, $3, $4::jsonb)
       ON CONFLICT (id) DO UPDATE SET
         workflow_id = EXCLUDED.workflow_id,
         created_at = EXCLUDED.created_at,
         record = EXCLUDED.record`,
      [record.id, record.workflowId, record.createdAt, JSON.stringify(record)],
    );
  }

  async get(executionId: string): Promise<ExecutionRecord | undefined> {
    await this.ensureSchema();
    const result = await this.client.query(
      `SELECT record FROM ${this.table} WHERE id = $1`,
      [executionId],
    );
    return result.rows[0] ? parseRecord(result.rows[0].record) : undefined;
  }

  async list(query: ExecutionQuery = {}): Promise<ExecutionRecord[]> {
    await this.ensureSchema();
    const limit = normalizeLimit(query.limit);
    const result = query.workflowId
      ? await this.client.query(
          `SELECT record FROM ${this.table}
           WHERE workflow_id = $1
           ORDER BY created_at DESC
           LIMIT $2`,
          [query.workflowId, limit],
        )
      : await this.client.query(
          `SELECT record FROM ${this.table}
           ORDER BY created_at DESC
           LIMIT $1`,
          [limit],
        );
    return result.rows.map((row) => parseRecord(row.record));
  }

  async delete(executionId: string): Promise<boolean> {
    await this.ensureSchema();
    const result = await this.client.query(
      `DELETE FROM ${this.table} WHERE id = $1`,
      [executionId],
    );
    return (result.rowCount ?? 0) > 0;
  }

  private async ensureSchema(): Promise<void> {
    this.schemaPromise ??= this.client
      .query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL,
          created_at timestamptz NOT NULL,
          record jsonb NOT NULL
        )`,
      )
      .then(() => undefined)
      .catch((error: unknown) => {
        this.schemaPromise = undefined;
        throw error;
      });
    await this.schemaPromise;
  }
}

function parseRecord(value: unknown): ExecutionRecord {
  const parsed = typeof value === "string" ? JSON.parse(value) : value;
  if (!parsed || typeof parsed !== "object" || !("id" in parsed)) {
    throw new Error("PostgreSQL returned an invalid FlowForge execution record.");
  }
  return parsed as ExecutionRecord;
}
