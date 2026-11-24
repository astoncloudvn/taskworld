import type { ExecutionRecord } from "../types.js";

export interface ExecutionQuery {
  workflowId?: string;
  limit?: number;
}

/**
 * Persistence boundary for execution history. Implementations should resolve
 * only after the record has been accepted by the backing store.
 */
export interface StorageAdapter {
  save(record: ExecutionRecord): Promise<void>;
  get(executionId: string): Promise<ExecutionRecord | undefined>;
  list(query?: ExecutionQuery): Promise<ExecutionRecord[]>;
  delete(executionId: string): Promise<boolean>;
}
