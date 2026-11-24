# FlowForge implementation report

## Implemented

- TypeScript workflow definitions with sequential tasks, dependency graphs, concurrent groups, workflow chaining, and conditional tasks.
- Per-task retries with exponential backoff, timeouts, recovery handlers, and structured failure results.
- Execution IDs, states, metadata, progress, step outputs, attempts, bounded logs and queues, capped in-memory history, and pause/resume/cancel handles.
- Event subscriptions, custom event triggers, and workflow lifecycle events.
- In-process interval, delayed, and five-field cron scheduling.
- Plugin registration, reusable custom tasks, and workflow/step/error hooks.
- Memory storage, Redis command-client storage, PostgreSQL query-client storage, and a custom adapter contract.
- ESM, CommonJS, TypeScript declarations, and source maps; no mandatory runtime dependencies.
- Unit and integration coverage for workflow execution, retry/recovery, concurrency graph behavior, execution controls, events, scheduling, plugins, and storage adapters.
- Quick-start and parallel-workflow examples with API and operational documentation.

## Boundaries and remaining limitations

- The scheduler and active executions are process-local. Restarting the application loses scheduled timers and active workflow state; persistent queue claiming, leasing, replay, and distributed workers are not implemented.
- Redis and PostgreSQL adapters persist execution history only. They do not make execution atomic across processes or coordinate concurrent workers.
- A task that ignores its `AbortSignal` may continue performing external side effects after cancellation or timeout has returned control to FlowForge. Side effects are not rolled back.
- Pausing takes effect at task boundaries and does not interrupt already-running tasks.
- Cron scheduling uses the host's local timezone and polls once per second; timezone-specific cron behavior and second-level cron fields are not supported.
- The PostgreSQL adapter creates a JSONB history table lazily; applications may instead pre-create an equivalent table and manage migrations themselves.
- The library is ready for independent package-level installation checks. Publishing has not been performed, per the specification.
