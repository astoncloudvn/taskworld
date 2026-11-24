export type ExecutionStatus =
  | "queued"
  | "running"
  | "paused"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timed_out";

export type StepStatus = "pending" | "running" | "succeeded" | "failed" | "skipped";

export interface ErrorInfo {
  name: string;
  message: string;
  stack?: string;
}

export interface StepExecutionRecord {
  id: string;
  status: StepStatus;
  attempts: number;
  startedAt?: string;
  finishedAt?: string;
  durationMs?: number;
  output?: unknown;
  error?: ErrorInfo;
}

export interface ExecutionLog {
  timestamp: string;
  level: "info" | "warn" | "error";
  message: string;
  stepId?: string;
  data?: unknown;
}

export interface ExecutionRecord {
  id: string;
  workflowId: string;
  status: ExecutionStatus;
  input: unknown;
  output?: unknown;
  metadata: Record<string, unknown>;
  steps: StepExecutionRecord[];
  logs: ExecutionLog[];
  progress: number;
  currentStepId?: string;
  error?: ErrorInfo;
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  durationMs?: number;
}

export interface ExecutionResult extends ExecutionRecord {}

export interface RunOptions {
  input?: unknown;
  metadata?: Record<string, unknown>;
  taskConcurrency?: number;
  timeoutMs?: number;
  throwOnError?: boolean;
}

export interface WorkflowChainResult {
  status: Exclude<ExecutionStatus, "queued" | "running" | "paused">;
  executions: ExecutionResult[];
  output?: unknown;
  failedWorkflowId?: string;
  error?: ErrorInfo;
}

export interface WorkflowChainOptions {
  input?: unknown;
  metadata?: Record<string, unknown>;
  taskConcurrency?: number;
  timeoutMs?: number;
}

export interface WorkflowContext<TInput = unknown, TPrevious = unknown> {
  readonly input: TInput;
  readonly previous: TPrevious;
  readonly outputs: Readonly<Record<string, unknown>>;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly executionId: string;
  readonly workflowId: string;
  readonly stepId: string;
  readonly signal: AbortSignal;
  progress(value: number, message?: string): void;
  log(message: string, data?: unknown): void;
  emit(event: string, payload?: unknown): Promise<void>;
}

export type TaskHandler<
  TInput = unknown,
  TOutput = unknown,
  TPrevious = unknown,
> = (
  context: WorkflowContext<TInput, TPrevious>,
) => TOutput | Promise<TOutput>;

export interface StepOptions<TInput = unknown, TPrevious = unknown> {
  /** Step IDs that must succeed before this step becomes eligible. */
  dependsOn?: string[];
  condition?: (
    context: WorkflowContext<TInput, TPrevious>,
  ) => boolean | Promise<boolean>;
  timeoutMs?: number;
  retries?: number;
  retryDelayMs?: number;
  maxRetryDelayMs?: number;
  /** Convert a task failure into a successful output. */
  recover?: (
    error: Error,
    context: WorkflowContext<TInput, TPrevious>,
  ) => unknown | Promise<unknown>;
}

export interface WorkflowStep<TInput = unknown, TPrevious = unknown> {
  id: string;
  run: TaskHandler<TInput, unknown, TPrevious>;
  options: StepOptions<TInput, TPrevious>;
}

export interface WorkflowDefinition<TInput = unknown> {
  id: string;
  steps: WorkflowStep<TInput, unknown>[];
  createdAt: string;
}

export interface FlowForgeOptions {
  /** Maximum number of workflow executions running at once. Defaults to 5. */
  concurrency?: number;
  /** Maximum number of queued executions. Defaults to 10,000. */
  maxQueuedExecutions?: number;
  /** Default retries for each task. Defaults to 0. */
  retries?: number;
  /** Initial retry backoff in milliseconds. Defaults to 250. */
  retryDelayMs?: number;
  /** Upper bound for exponential retry backoff. Defaults to 30 seconds. */
  maxRetryDelayMs?: number;
  /** Default timeout for each task, if set. */
  taskTimeoutMs?: number;
  storage?: import("./storage/adapter.js").StorageAdapter;
  clock?: () => Date;
}

export interface WorkflowEvent<T = unknown> {
  type: string;
  timestamp: string;
  payload: T;
  executionId?: string;
  workflowId?: string;
  stepId?: string;
}
