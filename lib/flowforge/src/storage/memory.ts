import type { ExecutionQuery, StorageAdapter } from "./adapter.js";
import type { ExecutionRecord } from "../types.js";

export interface MemoryStorageOptions {
  /** Maximum retained records. Oldest records are removed when the limit is reached. */
  maxRecords?: number;
}

function copy<T>(value: T): T {
  return structuredClone(value);
}

/** In-process storage. Records disappear when the Node.js process exits. */
export class MemoryStorageAdapter implements StorageAdapter {
  private readonly records = new Map<string, ExecutionRecord>();

  constructor(private readonly options: MemoryStorageOptions = {}) {
    const maxRecords = options.maxRecords ?? 10_000;
    if (!Number.isInteger(maxRecords) || maxRecords < 1) {
      throw new RangeError("Memory storage maxRecords must be a positive integer.");
    }
  }

  async save(record: ExecutionRecord): Promise<void> {
    this.records.set(record.id, copy(record));
    const maxRecords = this.options.maxRecords ?? 10_000;
    while (this.records.size > maxRecords) {
      let oldestId: string | undefined;
      let oldestTimestamp: string | undefined;
      for (const [id, current] of this.records) {
        if (oldestTimestamp === undefined || current.createdAt < oldestTimestamp) {
          oldestId = id;
          oldestTimestamp = current.createdAt;
        }
      }
      if (oldestId === undefined) break;
      this.records.delete(oldestId);
    }
  }

  async get(executionId: string): Promise<ExecutionRecord | undefined> {
    const record = this.records.get(executionId);
    return record ? copy(record) : undefined;
  }

  async list(query: ExecutionQuery = {}): Promise<ExecutionRecord[]> {
    const limit = normalizeLimit(query.limit);
    return [...this.records.values()]
      .filter((record) => !query.workflowId || record.workflowId === query.workflowId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .slice(0, limit)
      .map(copy);
  }

  async delete(executionId: string): Promise<boolean> {
    return this.records.delete(executionId);
  }
}

export function normalizeLimit(limit: number | undefined): number {
  if (limit === undefined) return 100;
  if (!Number.isInteger(limit) || limit < 1) {
    throw new RangeError("Storage list limit must be a positive integer.");
  }
  return limit;
}
