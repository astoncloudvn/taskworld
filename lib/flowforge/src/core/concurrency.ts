interface Waiter {
  resolve: () => void;
  reject: (error: Error) => void;
  signal: AbortSignal;
  onAbort: () => void;
}

export class ConcurrencyGate {
  private active = 0;
  private readonly queue: Waiter[] = [];

  constructor(
    private readonly capacity: number,
    private readonly maxQueueSize = 10_000,
  ) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError("Concurrency must be a positive integer.");
    }
    if (!Number.isInteger(maxQueueSize) || maxQueueSize < 1) {
      throw new RangeError("maxQueuedExecutions must be a positive integer.");
    }
  }

  async acquire(signal: AbortSignal): Promise<void> {
    if (signal.aborted) throw abortReason(signal);
    if (this.active < this.capacity) {
      this.active += 1;
      return;
    }
    if (this.queue.length >= this.maxQueueSize) {
      throw new Error(
        `FlowForge execution queue is full (limit: ${this.maxQueueSize}).`,
      );
    }
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = {
        resolve,
        reject,
        signal,
        onAbort: () => {
          const index = this.queue.indexOf(waiter);
          if (index >= 0) this.queue.splice(index, 1);
          reject(abortReason(signal));
        },
      };
      signal.addEventListener("abort", waiter.onAbort, { once: true });
      this.queue.push(waiter);
    });
  }

  release(): void {
    const next = this.queue.shift();
    if (!next) {
      this.active = Math.max(0, this.active - 1);
      return;
    }
    next.signal.removeEventListener("abort", next.onAbort);
    if (next.signal.aborted) {
      next.reject(abortReason(next.signal));
      this.release();
      return;
    }
    next.resolve();
  }
}

export function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error ? reason : new Error("Execution was cancelled.");
}
