import { randomUUID } from "node:crypto";
import { ConcurrencyGate, abortReason } from "./concurrency.js";
import { FlowForgeTimeoutError, serializeError, toError } from "./errors.js";
import { WorkflowBuilder } from "./workflow.js";
import { EventBus, type EventListener } from "../events.js";
import { PluginManager, type FlowForgeHook, type FlowForgePlugin, type HookName, type RegisteredTask } from "../plugins/manager.js";
import { MemoryStorageAdapter } from "../storage/memory.js";
import { Scheduler, type ScheduledRunOptions } from "../scheduler/scheduler.js";
import type {
  ErrorInfo,
  ExecutionLog,
  ExecutionRecord,
  ExecutionResult,
  FlowForgeOptions,
  RunOptions,
  StepExecutionRecord,
  StepOptions,
  WorkflowContext,
  WorkflowDefinition,
  WorkflowChainOptions,
  WorkflowChainResult,
  WorkflowEvent,
  WorkflowStep,
} from "../types.js";
import type { StorageAdapter } from "../storage/adapter.js";

const MAX_LOG_ENTRIES = 5_000;

export class ExecutionHandle {
  constructor(
    readonly id: string,
    readonly result: Promise<ExecutionResult>,
    private readonly pauseExecution: () => boolean,
    private readonly resumeExecution: () => boolean,
    private readonly cancelExecution: (reason?: string) => boolean,
  ) {}

  pause(): boolean {
    return this.pauseExecution();
  }

  resume(): boolean {
    return this.resumeExecution();
  }

  cancel(reason?: string): boolean {
    return this.cancelExecution(reason);
  }
}

interface RuntimeExecution {
  record: ExecutionRecord;
  definition: WorkflowDefinition;
  options: RunOptions;
  controller: AbortController;
  paused: boolean;
  resumeGate?: () => void;
  saveChain: Promise<void>;
  pause(): boolean;
  resume(): boolean;
  cancel(reason?: string): boolean;
}

/**
 * FlowForge workflow engine. Use `run` for a single result or `start` when
 * pause, resume, and cancellation controls are needed.
 */
export class FlowForge {
  readonly events = new EventBus();
  readonly scheduler: Scheduler;

  private readonly definitions = new Map<string, WorkflowDefinition>();
  private readonly executions = new Map<string, RuntimeExecution>();
  private readonly gate: ConcurrencyGate;
  private readonly plugins = new PluginManager();
  private readonly storage: StorageAdapter;
  private readonly clock: () => Date;
  private readonly defaults: Required<
    Pick<
      FlowForgeOptions,
      | "concurrency"
      | "maxQueuedExecutions"
      | "retries"
      | "retryDelayMs"
      | "maxRetryDelayMs"
    >
  > &
    Pick<FlowForgeOptions, "taskTimeoutMs">;
  private closed = false;

  constructor(options: FlowForgeOptions = {}) {
    const concurrency = options.concurrency ?? 5;
    const maxQueuedExecutions = options.maxQueuedExecutions ?? 10_000;
    this.gate = new ConcurrencyGate(concurrency, maxQueuedExecutions);
    this.defaults = {
      concurrency,
      maxQueuedExecutions,
      retries: options.retries ?? 0,
      retryDelayMs: options.retryDelayMs ?? 250,
      maxRetryDelayMs: options.maxRetryDelayMs ?? 30_000,
      taskTimeoutMs: options.taskTimeoutMs,
    };
    validateRetries(this.defaults.retries, "retries");
    validateDuration(this.defaults.retryDelayMs, "retryDelayMs", true);
    validateDuration(this.defaults.maxRetryDelayMs, "maxRetryDelayMs", true);
    if (options.taskTimeoutMs !== undefined) {
      validateDuration(options.taskTimeoutMs, "taskTimeoutMs", false);
    }
    this.storage = options.storage ?? new MemoryStorageAdapter();
    this.clock = options.clock ?? (() => new Date());
    this.scheduler = new Scheduler(
      async (workflowId: string, runOptions: ScheduledRunOptions) => {
        const result = await this.run(workflowId, runOptions);
        if (result.status === "failed" || result.status === "timed_out") {
          await this.events.emit("schedule:execution-failed", result);
        }
        return result;
      },
      (error, scheduleId) => {
        void this.events.emit("schedule:error", { error: toError(error), scheduleId });
      },
    );
  }

  /** Register a named workflow and return its fluent builder. */
  create<TInput = unknown>(
    id: string,
  ): WorkflowBuilder<TInput, undefined> {
    this.assertOpen();
    if (!id.trim()) throw new Error("Workflow id cannot be empty.");
    if (this.definitions.has(id)) throw new Error(`Workflow "${id}" is already registered.`);
    const definition: WorkflowDefinition<TInput> = {
      id,
      steps: [],
      createdAt: this.now(),
    };
    const builder = new WorkflowBuilder<TInput, undefined>(definition);
    builder.setTaskResolver((name, args, context) =>
      this.plugins.getTask(name)(args, context as WorkflowContext),
    );
    this.definitions.set(id, definition as WorkflowDefinition);
    return builder;
  }

  /** Start an execution and return a handle for lifecycle controls. */
  async start<TInput = unknown>(
    workflowId: string,
    options: RunOptions & { input?: TInput } = {},
  ): Promise<ExecutionHandle> {
    this.assertOpen();
    const definition = this.definitions.get(workflowId);
    if (!definition) throw new Error(`Workflow "${workflowId}" is not registered.`);
    validateWorkflow(definition);
    if (options.taskConcurrency !== undefined) {
      validateConcurrency(options.taskConcurrency);
    }
    if (options.timeoutMs !== undefined) {
      validateDuration(options.timeoutMs, "timeoutMs", false);
    }

    const timestamp = this.now();
    const record: ExecutionRecord = {
      id: randomUUID(),
      workflowId,
      status: "queued",
      input: options.input,
      metadata: { ...(options.metadata ?? {}) },
      steps: definition.steps.map((step) => ({
        id: step.id,
        status: "pending",
        attempts: 0,
      })),
      logs: [],
      progress: 0,
      createdAt: timestamp,
    };
    const controller = new AbortController();
    const runtime = {} as RuntimeExecution;
    Object.assign(runtime, {
      record,
      definition,
      options,
      controller,
      paused: false,
      saveChain: Promise.resolve(),
    });
    runtime.pause = () => this.pauseRuntime(runtime);
    runtime.resume = () => this.resumeRuntime(runtime);
    runtime.cancel = (reason?: string) => this.cancelRuntime(runtime, reason);
    await this.storage.save(record);

    let resolveResult!: (result: ExecutionResult) => void;
    let rejectResult!: (error: unknown) => void;
    const result = new Promise<ExecutionResult>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    void result.catch(() => undefined);
    const handle = new ExecutionHandle(
      record.id,
      result,
      runtime.pause,
      runtime.resume,
      runtime.cancel,
    );
    this.executions.set(record.id, runtime);
    void this.execute(runtime).then(resolveResult, rejectResult);
    return handle;
  }

  /** Run a workflow and wait for its final execution result. */
  async run<TInput = unknown>(
    workflowId: string,
    options: RunOptions & { input?: TInput } = {},
  ): Promise<ExecutionResult> {
    const handle = await this.start(workflowId, options);
    const result = await handle.result;
    if (options.throwOnError && result.error) {
      throw new Error(result.error.message, { cause: result.error });
    }
    return result;
  }

  /**
   * Run registered workflows sequentially, passing each successful output as
   * the next workflow's input. The chain stops at the first unsuccessful run.
   */
  async chain(
    workflowIds: string[],
    options: WorkflowChainOptions = {},
  ): Promise<WorkflowChainResult> {
    if (workflowIds.length === 0) {
      throw new Error("chain() requires at least one workflow id.");
    }
    for (const workflowId of workflowIds) this.assertWorkflowExists(workflowId);

    const executions: ExecutionResult[] = [];
    let input = options.input;
    for (const workflowId of workflowIds) {
      const result = await this.run(workflowId, {
        input,
        metadata: options.metadata,
        taskConcurrency: options.taskConcurrency,
        timeoutMs: options.timeoutMs,
      });
      executions.push(result);
      if (result.status !== "succeeded") {
        return {
          status: result.status as WorkflowChainResult["status"],
          executions,
          ...(result.output === undefined ? {} : { output: result.output }),
          failedWorkflowId: workflowId,
          ...(result.error ? { error: result.error } : {}),
        };
      }
      input = result.output;
    }
    const output = executions[executions.length - 1]?.output;
    return {
      status: "succeeded",
      executions,
      ...(output === undefined ? {} : { output }),
    };
  }

  pause(executionId: string): boolean {
    return this.executions.get(executionId)?.pause() ?? false;
  }

  resume(executionId: string): boolean {
    return this.executions.get(executionId)?.resume() ?? false;
  }

  cancel(executionId: string, reason?: string): boolean {
    return this.executions.get(executionId)?.cancel(reason) ?? false;
  }

  async getExecution(executionId: string): Promise<ExecutionRecord | undefined> {
    return this.storage.get(executionId);
  }

  async history(query: {
    workflowId?: string;
    limit?: number;
  } = {}): Promise<ExecutionRecord[]> {
    return this.storage.list(query);
  }

  getWorkflowIds(): string[] {
    return [...this.definitions.keys()];
  }

  /** Subscribe to engine events. Listener errors are sent to `listener:error`. */
  on<T = unknown>(event: string, listener: EventListener<T>): () => void {
    return this.events.on(event, listener);
  }

  once<T = unknown>(event: string, listener: EventListener<T>): () => void {
    return this.events.once(event, listener);
  }

  off<T = unknown>(event: string, listener: EventListener<T>): void {
    this.events.off(event, listener);
  }

  async emit<T = unknown>(event: string, payload: T): Promise<void> {
    await this.events.emit(event, payload);
  }

  /** Trigger a workflow when a named application event is emitted. */
  onEvent<T = unknown>(
    event: string,
    workflowId: string,
    input: (payload: T) => unknown = (payload) => payload,
  ): () => void {
    if (!this.definitions.has(workflowId)) {
      throw new Error(`Cannot attach event trigger: workflow "${workflowId}" is not registered.`);
    }
    return this.on<T>(event, async (payload) => {
      const result = await this.run(workflowId, { input: input(payload) });
      if (result.status === "failed" || result.status === "timed_out") {
        await this.events.emit("trigger:execution-failed", result);
      }
    });
  }

  /** Schedule recurring or delayed workflow executions. */
  scheduleInterval(
    workflowId: string,
    intervalMs: number,
    options?: ScheduledRunOptions,
  ): string {
    this.assertWorkflowExists(workflowId);
    return this.scheduler.interval(workflowId, intervalMs, options);
  }

  scheduleCron(
    workflowId: string,
    expression: string,
    options?: ScheduledRunOptions,
  ): string {
    this.assertWorkflowExists(workflowId);
    return this.scheduler.cron(workflowId, expression, options);
  }

  scheduleDelayed(
    workflowId: string,
    delayMs: number,
    options?: ScheduledRunOptions,
  ): string {
    this.assertWorkflowExists(workflowId);
    return this.scheduler.delay(workflowId, delayMs, options);
  }

  cancelSchedule(scheduleId: string): boolean {
    return this.scheduler.cancel(scheduleId);
  }

  use(plugin: FlowForgePlugin): this {
    this.assertOpen();
    this.plugins.addPlugin(plugin, this);
    return this;
  }

  registerTask(name: string, task: RegisteredTask): this {
    this.assertOpen();
    this.plugins.registerTask(name, task);
    return this;
  }

  addHook(name: HookName, hook: FlowForgeHook): () => void {
    this.assertOpen();
    return this.plugins.addHook(name, hook);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.scheduler.close();
    const runtimes = [...this.executions.values()];
    for (const runtime of runtimes) runtime.cancel("FlowForge is closing.");
    await Promise.all(
      runtimes.map((runtime) =>
        this.waitForCompletion(runtime.record.id).catch(() => undefined),
      ),
    );
    this.events.clear();
  }

  private async waitForCompletion(executionId: string): Promise<void> {
    while (this.executions.has(executionId)) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }

  private pauseRuntime(runtime: RuntimeExecution): boolean {
    if (isFinal(runtime.record.status) || runtime.paused) return false;
    runtime.paused = true;
    if (runtime.record.status === "running") runtime.record.status = "paused";
    this.persistInBackground(runtime);
    void this.events.emit("execution:paused", snapshot(runtime.record));
    return true;
  }

  private resumeRuntime(runtime: RuntimeExecution): boolean {
    if (isFinal(runtime.record.status) || !runtime.paused) return false;
    runtime.paused = false;
    if (runtime.record.status === "paused") runtime.record.status = "running";
    runtime.resumeGate?.();
    runtime.resumeGate = undefined;
    this.persistInBackground(runtime);
    void this.events.emit("execution:resumed", snapshot(runtime.record));
    return true;
  }

  private cancelRuntime(runtime: RuntimeExecution, reason?: string): boolean {
    if (isFinal(runtime.record.status) || runtime.controller.signal.aborted) return false;
    runtime.paused = false;
    runtime.resumeGate?.();
    runtime.resumeGate = undefined;
    runtime.controller.abort(new Error(reason ?? "Execution was cancelled."));
    return true;
  }

  private async execute(runtime: RuntimeExecution): Promise<ExecutionResult> {
    let acquired = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const began = performance.now();
    try {
      await this.gate.acquire(runtime.controller.signal);
      acquired = true;
      await this.waitIfPaused(runtime);
      if (runtime.controller.signal.aborted) throw abortReason(runtime.controller.signal);
      runtime.record.status = "running";
      runtime.record.startedAt = this.now();
      await this.persist(runtime);
      await this.events.emit("execution:started", snapshot(runtime.record));
      await this.plugins.runHooks("beforeWorkflow", { execution: snapshot(runtime.record) });

      if (runtime.options.timeoutMs !== undefined) {
        timeout = setTimeout(() => {
          runtime.paused = false;
          runtime.resumeGate?.();
          runtime.resumeGate = undefined;
          runtime.controller.abort(
            new FlowForgeTimeoutError(
              `Workflow "${runtime.record.workflowId}" exceeded ${runtime.options.timeoutMs}ms.`,
            ),
          );
        }, runtime.options.timeoutMs);
      }
      await this.runSteps(runtime);
      runtime.record.status = "succeeded";
      runtime.record.progress = 100;
      runtime.record.output = lastOutput(runtime.record);
      await this.plugins.runHooks("afterWorkflow", { execution: snapshot(runtime.record) });
    } catch (caught) {
      const error = toError(caught);
      const timedOut =
        runtime.controller.signal.aborted &&
        runtime.controller.signal.reason instanceof FlowForgeTimeoutError;
      runtime.record.status = timedOut ? "timed_out" : runtime.controller.signal.aborted
        ? "cancelled"
        : "failed";
      runtime.record.error = serializeError(error);
      runtime.record.logs.push({
        timestamp: this.now(),
        level: "error",
        message: error.message,
        stepId: runtime.record.currentStepId,
      });
      await this.plugins
        .runHooks("onError", {
          execution: snapshot(runtime.record),
          error,
        })
        .catch((hookError: unknown) => {
          runtime.record.logs.push({
            timestamp: this.now(),
            level: "error",
            message: `Error hook failed: ${toError(hookError).message}`,
          });
        });
    } finally {
      if (timeout) clearTimeout(timeout);
      if (acquired) this.gate.release();
      runtime.record.finishedAt = this.now();
      runtime.record.durationMs = Math.max(0, Math.round(performance.now() - began));
      if (runtime.record.status === "queued" || runtime.record.status === "running" ||
          runtime.record.status === "paused") {
        runtime.record.status = "failed";
        runtime.record.error = {
          name: "Error",
          message: "Execution ended in an invalid state.",
        };
      }
      try {
        await this.persist(runtime);
        if (runtime.record.status === "succeeded") {
          await this.events.emit("execution:completed", snapshot(runtime.record));
        } else if (runtime.record.status === "cancelled") {
          await this.events.emit("execution:cancelled", snapshot(runtime.record));
        } else {
          await this.events.emit("execution:failed", snapshot(runtime.record));
        }
      } finally {
        this.executions.delete(runtime.record.id);
      }
    }
    return snapshot(runtime.record);
  }

  private async runSteps(runtime: RuntimeExecution): Promise<void> {
    const { definition, record } = runtime;
    const stepRecords = new Map(record.steps.map((step) => [step.id, step]));
    const outputs: Record<string, unknown> = {};
    const maxConcurrent = runtime.options.taskConcurrency ?? this.defaults.concurrency;

    while (record.steps.some((step) => step.status === "pending")) {
      await this.waitIfPaused(runtime);
      if (runtime.controller.signal.aborted) throw abortReason(runtime.controller.signal);

      for (const step of definition.steps) {
        const status = stepRecords.get(step.id)!;
        if (status.status !== "pending") continue;
        const dependencies = step.options.dependsOn ?? [];
        if (
          dependencies.some((dependency) => {
            const dependencyStatus = stepRecords.get(dependency)?.status;
            return dependencyStatus === "skipped" || dependencyStatus === "failed";
          })
        ) {
          status.status = "skipped";
          status.finishedAt = this.now();
          await this.emitStepEvent(runtime, "step:skipped", status);
        }
      }
      if (!record.steps.some((step) => step.status === "pending")) break;

      const ready = definition.steps.filter((step) => {
        const status = stepRecords.get(step.id)!;
        return (
          status.status === "pending" &&
          (step.options.dependsOn ?? []).every(
            (dependency) => stepRecords.get(dependency)?.status === "succeeded",
          )
        );
      });
      if (ready.length === 0) {
        throw new Error(
          `Workflow "${definition.id}" has pending tasks with no runnable dependencies.`,
        );
      }

      const runnable: WorkflowStep[] = [];
      for (const step of ready) {
        if (runnable.length >= maxConcurrent) break;
        const context = this.makeContext(runtime, step, outputs, runtime.controller.signal);
        try {
          if (step.options.condition && !(await step.options.condition(context))) {
            const status = stepRecords.get(step.id)!;
            status.status = "skipped";
            status.finishedAt = this.now();
            await this.emitStepEvent(runtime, "step:skipped", status);
            continue;
          }
          runnable.push(step);
        } catch (caught) {
          const status = stepRecords.get(step.id)!;
          const error = toError(caught);
          status.status = "failed";
          status.error = serializeError(error);
          status.finishedAt = this.now();
          await this.emitStepEvent(runtime, "step:failed", status);
          throw error;
        }
      }
      if (runnable.length === 0) continue;

      const outcomes = await Promise.allSettled(
        runnable.map((step) =>
          this.executeStep(runtime, step, stepRecords.get(step.id)!, outputs),
        ),
      );
      await this.flush(runtime);
      const failure = outcomes.find(
        (outcome): outcome is PromiseRejectedResult => outcome.status === "rejected",
      );
      if (failure) throw failure.reason;
    }
    await this.flush(runtime);
    if (record.steps.some((step) => step.status === "failed")) {
      throw new Error("One or more workflow tasks failed.");
    }
  }

  private async executeStep(
    runtime: RuntimeExecution,
    step: WorkflowStep,
    stepRecord: StepExecutionRecord,
    outputs: Record<string, unknown>,
  ): Promise<void> {
    await this.waitIfPaused(runtime);
    if (runtime.controller.signal.aborted) throw abortReason(runtime.controller.signal);
    runtime.record.currentStepId = step.id;
    stepRecord.status = "running";
    stepRecord.startedAt = this.now();
    await this.persist(runtime);
    const context = this.makeContext(runtime, step, outputs, runtime.controller.signal);
    const began = performance.now();
    try {
      await this.plugins.runHooks("beforeStep", {
        execution: snapshot(runtime.record),
        step: structuredClone(stepRecord),
        taskContext: context,
      });
      await this.events.emit("step:started", {
        ...snapshot(runtime.record),
        step: structuredClone(stepRecord),
      });
      const output = await this.runWithRetry(runtime, step, context, stepRecord);
      outputs[step.id] = output;
      stepRecord.output = output;
      stepRecord.status = "succeeded";
      stepRecord.finishedAt = this.now();
      stepRecord.durationMs = Math.max(0, Math.round(performance.now() - began));
      runtime.record.progress =
        (runtime.record.steps.filter((item) => item.status === "succeeded" || item.status === "skipped")
          .length /
          Math.max(1, runtime.record.steps.length)) *
        100;
      await this.plugins.runHooks("afterStep", {
        execution: snapshot(runtime.record),
        step: structuredClone(stepRecord),
        taskContext: context,
      });
      await this.emitStepEvent(runtime, "step:completed", stepRecord);
    } catch (caught) {
      const error = toError(caught);
      stepRecord.status = "failed";
      stepRecord.error = serializeError(error);
      stepRecord.finishedAt = this.now();
      stepRecord.durationMs = Math.max(0, Math.round(performance.now() - began));
      await this.emitStepEvent(runtime, "step:failed", stepRecord);
      throw error;
    } finally {
      runtime.record.currentStepId = undefined;
      await this.persist(runtime);
    }
  }

  private async runWithRetry(
    runtime: RuntimeExecution,
    step: WorkflowStep,
    context: WorkflowContext,
    stepRecord: StepExecutionRecord,
  ): Promise<unknown> {
    const retries = step.options.retries ?? this.defaults.retries;
    validateRetries(retries, `retries for step "${step.id}"`);
    const baseDelay = step.options.retryDelayMs ?? this.defaults.retryDelayMs;
    const maxDelay = step.options.maxRetryDelayMs ?? this.defaults.maxRetryDelayMs;
    validateDuration(baseDelay, `retryDelayMs for step "${step.id}"`, true);
    validateDuration(maxDelay, `maxRetryDelayMs for step "${step.id}"`, true);
    const timeoutMs = step.options.timeoutMs ?? this.defaults.taskTimeoutMs;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
      if (runtime.controller.signal.aborted) throw abortReason(runtime.controller.signal);
      stepRecord.attempts = attempt + 1;
      try {
        const taskController = new AbortController();
        const attemptContext: WorkflowContext = {
          ...context,
          signal: taskController.signal,
        };
        return await invokeWithTimeout(
          () => step.run(attemptContext),
          timeoutMs,
          taskController,
          runtime.controller.signal,
          `Step "${step.id}"`,
        );
      } catch (caught) {
        const error = toError(caught);
        if (runtime.controller.signal.aborted) throw error;
        if (attempt >= retries) {
          if (step.options.recover) {
            return await step.options.recover(error, context);
          }
          throw error;
        }
        const delayMs = Math.min(maxDelay, baseDelay * 2 ** attempt);
        runtime.record.logs.push({
          timestamp: this.now(),
          level: "warn",
          message: `Step "${step.id}" failed on attempt ${attempt + 1}; retrying in ${delayMs}ms.`,
          stepId: step.id,
          data: { error: error.message, nextAttempt: attempt + 2 },
        });
        await this.persist(runtime);
        await abortableDelay(delayMs, runtime.controller.signal);
      }
    }
    throw new Error(`Step "${step.id}" exhausted its retry attempts.`);
  }

  private makeContext(
    runtime: RuntimeExecution,
    step: WorkflowStep,
    outputs: Record<string, unknown>,
    signal: AbortSignal,
  ): WorkflowContext {
    const dependencies = step.options.dependsOn ?? [];
    const previous =
      dependencies.length === 0
        ? undefined
        : dependencies.length === 1
          ? outputs[dependencies[0]!]
          : Object.fromEntries(dependencies.map((id) => [id, outputs[id]]));
    return {
      input: runtime.record.input,
      previous,
      outputs,
      metadata: runtime.record.metadata,
      executionId: runtime.record.id,
      workflowId: runtime.record.workflowId,
      stepId: step.id,
      signal,
      progress: (value, message) => {
        if (!Number.isFinite(value)) throw new RangeError("Progress must be a finite number.");
        runtime.record.progress = Math.min(100, Math.max(0, value));
        if (message) this.addLog(runtime, "info", message, step.id);
        this.persistInBackground(runtime);
      },
      log: (message, data) => {
        this.addLog(runtime, "info", message, step.id, data);
        this.persistInBackground(runtime);
      },
      emit: async (event, payload) => {
        await this.events.emit(event, {
          type: event,
          timestamp: this.now(),
          payload,
          executionId: runtime.record.id,
          workflowId: runtime.record.workflowId,
          stepId: step.id,
        } satisfies WorkflowEvent);
      },
    };
  }

  private async emitStepEvent(
    runtime: RuntimeExecution,
    event: string,
    step: StepExecutionRecord,
  ): Promise<void> {
    const payload = {
      ...snapshot(runtime.record),
      step: structuredClone(step),
    };
    await this.events.emit(event, payload);
    await this.persist(runtime);
  }

  private addLog(
    runtime: RuntimeExecution,
    level: ExecutionLog["level"],
    message: string,
    stepId?: string,
    data?: unknown,
  ): void {
    runtime.record.logs.push({
      timestamp: this.now(),
      level,
      message,
      ...(stepId ? { stepId } : {}),
      ...(data === undefined ? {} : { data }),
    });
    if (runtime.record.logs.length > MAX_LOG_ENTRIES) {
      runtime.record.logs.splice(0, runtime.record.logs.length - MAX_LOG_ENTRIES);
    }
  }

  private async waitIfPaused(runtime: RuntimeExecution): Promise<void> {
    while (runtime.paused && !runtime.controller.signal.aborted) {
      await new Promise<void>((resolve) => {
        runtime.resumeGate = resolve;
      });
    }
  }

  private async persist(runtime: RuntimeExecution): Promise<void> {
    const record = snapshot(runtime.record);
    runtime.saveChain = runtime.saveChain.then(() => this.storage.save(record));
    await runtime.saveChain;
  }

  private persistInBackground(runtime: RuntimeExecution): void {
    void this.persist(runtime).catch((error: unknown) =>
      this.events.emit("storage:error", {
        executionId: runtime.record.id,
        error: toError(error),
      }),
    );
  }

  private async flush(runtime: RuntimeExecution): Promise<void> {
    await runtime.saveChain;
  }

  private assertWorkflowExists(workflowId: string): void {
    if (!this.definitions.has(workflowId)) {
      throw new Error(`Workflow "${workflowId}" is not registered.`);
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("FlowForge is closed.");
  }

  private now(): string {
    return this.clock().toISOString();
  }
}

function snapshot<T>(value: T): T {
  return structuredClone(value);
}

function lastOutput(record: ExecutionRecord): unknown {
  for (let index = record.steps.length - 1; index >= 0; index -= 1) {
    const step = record.steps[index]!;
    if (step.status === "succeeded") return step.output;
  }
  return undefined;
}

function isFinal(status: ExecutionRecord["status"]): boolean {
  return ["succeeded", "failed", "cancelled", "timed_out"].includes(status);
}

function validateWorkflow(definition: WorkflowDefinition): void {
  const stepIds = new Set(definition.steps.map((step) => step.id));
  for (const step of definition.steps) {
    for (const dependency of step.options.dependsOn ?? []) {
      if (!stepIds.has(dependency)) {
        throw new Error(
          `Workflow "${definition.id}" step "${step.id}" depends on unknown step "${dependency}".`,
        );
      }
      if (dependency === step.id) {
        throw new Error(`Workflow step "${step.id}" cannot depend on itself.`);
      }
    }
  }

  const complete = new Set<string>();
  while (complete.size < definition.steps.length) {
    const ready = definition.steps.filter(
      (step) =>
        !complete.has(step.id) &&
        (step.options.dependsOn ?? []).every((dependency) => complete.has(dependency)),
    );
    if (ready.length === 0) {
      throw new Error(`Workflow "${definition.id}" contains a dependency cycle.`);
    }
    ready.forEach((step) => complete.add(step.id));
  }
}

function validateConcurrency(value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError("taskConcurrency must be a positive integer.");
  }
}

function validateRetries(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative integer.`);
  }
}

function validateDuration(value: number, name: string, allowZero: boolean): void {
  if (!Number.isFinite(value) || (allowZero ? value < 0 : value <= 0)) {
    throw new RangeError(
      `${name} must be a ${allowZero ? "non-negative" : "positive"} finite number.`,
    );
  }
}

function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function invokeWithTimeout<T>(
  task: () => T | Promise<T>,
  timeoutMs: number | undefined,
  taskController: AbortController,
  parentSignal: AbortSignal,
  label: string,
): Promise<T> {
  if (parentSignal.aborted) return Promise.reject(abortReason(parentSignal));
  let timer: ReturnType<typeof setTimeout> | undefined;
  let rejectAbort!: (error: Error) => void;
  const abortPromise = new Promise<never>((_, reject) => {
    rejectAbort = reject;
  });
  const onTaskAbort = (): void => rejectAbort(abortReason(taskController.signal));
  const onParentAbort = (): void => {
    taskController.abort(abortReason(parentSignal));
  };
  taskController.signal.addEventListener("abort", onTaskAbort, { once: true });
  parentSignal.addEventListener("abort", onParentAbort, { once: true });

  if (timeoutMs !== undefined) {
    timer = setTimeout(() => {
      taskController.abort(
        new FlowForgeTimeoutError(`${label} exceeded ${timeoutMs}ms.`),
      );
    }, timeoutMs);
  }
  const taskPromise = Promise.resolve().then(task);
  return Promise.race([taskPromise, abortPromise]).finally(() => {
    if (timer) clearTimeout(timer);
    taskController.signal.removeEventListener("abort", onTaskAbort);
    parentSignal.removeEventListener("abort", onParentAbort);
  }) as Promise<T>;
}
