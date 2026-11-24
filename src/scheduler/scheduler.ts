import { randomUUID } from "node:crypto";
import { matchesCron, parseCron } from "./cron.js";

export interface ScheduledRunOptions {
  input?: unknown;
  metadata?: Record<string, unknown>;
}

export type ScheduledRunner = (
  workflowId: string,
  options: ScheduledRunOptions,
) => Promise<unknown>;

interface ScheduledTask {
  timer: ReturnType<typeof setTimeout> | ReturnType<typeof setInterval>;
  kind: "timeout" | "interval";
}

/** In-process delayed, interval, and cron scheduler. */
export class Scheduler {
  private readonly tasks = new Map<string, ScheduledTask>();
  private closed = false;

  constructor(
    private readonly runWorkflow: ScheduledRunner,
    private readonly onError: (error: unknown, scheduleId: string) => void = () =>
      undefined,
  ) {}

  interval(
    workflowId: string,
    intervalMs: number,
    options: ScheduledRunOptions = {},
  ): string {
    this.assertOpen();
    assertWorkflowId(workflowId);
    assertPositiveDuration(intervalMs, "intervalMs");
    const id = randomUUID();
    const timer = setInterval(() => {
      void this.run(workflowId, options, id);
    }, intervalMs);
    this.tasks.set(id, { timer, kind: "interval" });
    return id;
  }

  delay(
    workflowId: string,
    delayMs: number,
    options: ScheduledRunOptions = {},
  ): string {
    this.assertOpen();
    assertWorkflowId(workflowId);
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      throw new RangeError("delayMs must be a non-negative finite number.");
    }
    const id = randomUUID();
    const timer = setTimeout(() => {
      this.tasks.delete(id);
      void this.run(workflowId, options, id);
    }, delayMs);
    this.tasks.set(id, { timer, kind: "timeout" });
    return id;
  }

  cron(
    workflowId: string,
    expression: string,
    options: ScheduledRunOptions = {},
  ): string {
    this.assertOpen();
    assertWorkflowId(workflowId);
    const parsed = parseCron(expression);
    const id = randomUUID();
    let lastMinute = minuteKey(new Date());
    const timer = setInterval(() => {
      const now = new Date();
      const key = minuteKey(now);
      if (key === lastMinute) return;
      lastMinute = key;
      if (matchesCron(parsed, now)) void this.run(workflowId, options, id);
    }, 1_000);
    this.tasks.set(id, { timer, kind: "interval" });
    return id;
  }

  cancel(scheduleId: string): boolean {
    const task = this.tasks.get(scheduleId);
    if (!task) return false;
    clearTimeout(task.timer);
    this.tasks.delete(scheduleId);
    return true;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const task of this.tasks.values()) clearTimeout(task.timer);
    this.tasks.clear();
  }

  get size(): number {
    return this.tasks.size;
  }

  private async run(
    workflowId: string,
    options: ScheduledRunOptions,
    scheduleId: string,
  ): Promise<void> {
    try {
      await this.runWorkflow(workflowId, options);
    } catch (error) {
      this.onError(error, scheduleId);
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("Scheduler is closed.");
  }
}

function minuteKey(date: Date): string {
  return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}-${date.getHours()}-${date.getMinutes()}`;
}

function assertWorkflowId(workflowId: string): void {
  if (!workflowId.trim()) throw new Error("workflowId cannot be empty.");
}

function assertPositiveDuration(value: number, name: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive finite number.`);
  }
}
