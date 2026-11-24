export type EventListener<T = unknown> = (payload: T) => void | Promise<void>;

/** Async event bus that isolates listener failures from workflow execution. */
export class EventBus {
  private readonly listeners = new Map<string, Set<EventListener>>();

  on<T = unknown>(event: string, listener: EventListener<T>): () => void {
    if (!event.trim()) throw new Error("Event name cannot be empty.");
    const bucket = this.listeners.get(event) ?? new Set<EventListener>();
    bucket.add(listener as EventListener);
    this.listeners.set(event, bucket);
    return () => this.off(event, listener);
  }

  once<T = unknown>(event: string, listener: EventListener<T>): () => void {
    let remove = (): void => undefined;
    const wrapper: EventListener<T> = async (payload) => {
      remove();
      await listener(payload);
    };
    remove = this.on(event, wrapper);
    return remove;
  }

  off<T = unknown>(event: string, listener: EventListener<T>): void {
    const bucket = this.listeners.get(event);
    bucket?.delete(listener as EventListener);
    if (bucket?.size === 0) this.listeners.delete(event);
  }

  async emit<T = unknown>(event: string, payload: T): Promise<void> {
    const listeners = [...(this.listeners.get(event) ?? [])];
    const outcomes = await Promise.allSettled(
      listeners.map((listener) => listener(payload)),
    );
    const errors = outcomes.flatMap((outcome) =>
      outcome.status === "rejected" ? [outcome.reason] : [],
    );
    if (errors.length && event !== "listener:error") {
      const failures = await Promise.allSettled(
        [...(this.listeners.get("listener:error") ?? [])].map((listener) =>
          listener({ event, errors }),
        ),
      );
      void failures;
    }
  }

  clear(): void {
    this.listeners.clear();
  }
}
