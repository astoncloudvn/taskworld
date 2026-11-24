# FlowForge

![FlowForge logo](./assets/flowforge-logo.svg)

FlowForge is an open-source workflow and automation engine for Node.js. Define reusable workflows in TypeScript or JavaScript, then execute sequential or concurrent tasks with retries, timeouts, lifecycle controls, events, schedules, plugins, and pluggable execution-history storage.

- **Package:** `flowforge`
- **Version:** `1.0.0`
- **Node.js:** 20+
- **License:** MIT
- **Modules:** ESM and CommonJS
- **Runtime dependencies:** none

## Installation

```sh
npm install flowforge
```

## Quick start

```ts
import { FlowForge } from "flowforge";

const app = new FlowForge({ concurrency: 5, retries: 3 });

app
  .create<{ username: string }>("user-registration")
  .step("validate", async ({ input }) => {
    if (!input.username.trim()) throw new Error("A username is required.");
    return { username: input.username.trim(), email: input.email };
  })
  .step("save", ({ previous }) => ({
    id: `user-${previous.username}`,
    profile: previous,
  }))
  .step("prepare-welcome-message", ({ previous }) => ({
    userId: previous.id,
    recipient: previous.profile.email,
    template: "welcome",
  }));

const result = await app.run("user-registration", {
  input: { username: "green_alley" },
  metadata: { requestId: "req-123" },
});

if (result.status !== "succeeded") {
  throw new Error(result.error?.message ?? `Workflow ended as ${result.status}.`);
}

console.log(result.output);
await app.close();
```

`run()` resolves to an `ExecutionResult` for successful, failed, timed-out, and cancelled executions. Configuration errors (such as an unknown workflow or invalid dependency graph) reject before the execution starts. Set `throwOnError: true` if you want task failures to reject as well.
The quick start builds a notification payload; connect the last step to your email provider to send it.

## Workflows and tasks

`create(id)` registers a workflow immediately and returns a chainable `WorkflowBuilder`. Workflow IDs and step IDs must be unique. A normal `step()` depends on the previous step, and its return value is available as `context.previous` in the next step.

### Concurrent tasks

Tasks in a `parallel()` group share the same dependencies. The next chained step waits for every task in that group and receives their outputs in `context.previous`.

```ts
app
  .create<{ orderId: string }>("process-order")
  .step("load-order", ({ input }) => loadOrder(input.orderId))
  .parallel([
    { id: "reserve-stock", run: ({ previous }) => reserveStock(previous) },
    { id: "authorize-payment", run: ({ previous }) => authorizePayment(previous) },
  ])
  .step("confirm", ({ outputs }) =>
    confirmOrder(outputs["reserve-stock"], outputs["authorize-payment"]),
  );
```

For more control, `task(id, handler, options)` adds a node to the workflow graph. Set `dependsOn` to one or more step IDs, or use `dependsOn: []` to create an independent task. Dependencies are validated before execution; missing steps and cycles produce descriptive errors. `taskConcurrency` on `run()` limits how many ready tasks in that execution can run at once.

### Conditional tasks and recovery

```ts
app.create("maybe-notify").step("notify", sendNotification, {
  condition: ({ input }) => input.shouldNotify === true,
  timeoutMs: 10_000,
  retries: 2,
  recover: (error) => ({ notificationError: error.message }),
});
```

A condition that returns `false` marks the task `skipped`; dependent tasks do not run. A `recover` handler converts the final task error into a successful output. Otherwise FlowForge retries the task and marks the execution failed when attempts are exhausted.

Retries use exponential backoff: `retryDelayMs * 2^attempt`, capped by `maxRetryDelayMs`. FlowForge defaults are 0 retries, 250 ms initial delay, and a 30-second cap. Configure defaults in the constructor or override them on a specific task.

### Chaining workflows

Pass one workflow's successful output into the next workflow with `chain()`. The chain stops at the first unsuccessful execution and returns the individual execution records:

```ts
const chain = await app.chain(["load-order", "calculate-shipping", "confirm-order"], {
  input: { orderId: "order-123" },
});

if (chain.status !== "succeeded") {
  console.error(chain.failedWorkflowId, chain.error?.message);
}
```

## Execution context

Every task receives:

| Property | Description |
| --- | --- |
| `input` | The value passed to `run()` or the scheduler |
| `previous` | The output of the dependency, or an ID-to-output object for multiple dependencies |
| `outputs` | Outputs from completed tasks, keyed by step ID |
| `metadata` | Caller-supplied execution metadata |
| `executionId`, `workflowId`, `stepId` | Identifiers for tracing and persistence |
| `signal` | Abort signal for cancellation and timeouts |
| `progress(percent, message?)` | Update execution progress from 0 to 100 |
| `log(message, data?)` | Add a timestamped entry to execution history |
| `emit(event, payload?)` | Emit an application event |

Inputs, outputs, metadata, and log data should be structured-cloneable. Redis and PostgreSQL adapters serialize records as JSON, so those values should also be JSON-serializable when using those adapters.

## Managing executions

`run()` is convenient when only the final result is needed. Use `start()` to receive an `ExecutionHandle`:

```ts
const execution = await app.start("long-running-workflow", { input: payload });

execution.pause();  // pauses at the next task boundary
execution.resume();
execution.cancel("No longer needed");

const result = await execution.result;
```

You can also call `app.pause(id)`, `app.resume(id)`, or `app.cancel(id)`. Cancellation is cooperative: FlowForge aborts the task signal and stops waiting for the task, but external side effects may continue if a task ignores its signal. A timeout does not roll back a database write or other side effect already performed.

`getExecution(id)` reads one stored record. `history({ workflowId, limit })` returns the newest stored executions; the default limit is 100. Each record contains status, metadata, task attempts, outputs, errors, timestamps, duration, progress, and bounded log history.

## Events

FlowForge exposes an async event bus. Listener failures are isolated and reported to `listener:error`; they do not fail the workflow.

```ts
app.on("execution:completed", (execution) => {
  console.info(execution.workflowId, execution.id);
});

app.onEvent("orders:created", "process-order", (order) => ({
  orderId: order.id,
}));

await app.emit("orders:created", { id: "order-123" });
```

Built-in events include `execution:started`, `execution:paused`, `execution:resumed`, `execution:completed`, `execution:failed`, `execution:cancelled`, `step:started`, `step:completed`, `step:skipped`, `step:failed`, and `listener:error`.

## Scheduling

The scheduler runs inside the current Node.js process:

```ts
const everyMinute = app.scheduleInterval("refresh-cache", 60_000);
const weekdaysAtNine = app.scheduleCron("daily-report", "0 9 * * 1-5");
const oneShot = app.scheduleDelayed("send-reminder", 5_000, {
  input: { userId: "user-123" },
});

app.cancelSchedule(everyMinute);
await app.close(); // clears all remaining schedules
```

Cron expressions use five fields (`minute hour day-of-month month day-of-week`), standard lists/ranges/steps, local machine time, and Sunday `0` or `7`. Cron schedules are checked once per second and fire at most once per matching minute. Recurring timers can overlap when an execution takes longer than its interval; use the engine's `concurrency` limit to control active workflows.

## Storage adapters

The built-in `MemoryStorageAdapter` stores history in the current process and is the default. It retains at most 10,000 records by default and evicts the oldest records at capacity; configure a different limit with `new MemoryStorageAdapter({ maxRecords })`. Supply another `StorageAdapter` to persist execution history:

```ts
import { FlowForge, PostgresStorageAdapter } from "flowforge";
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const app = new FlowForge({
  storage: new PostgresStorageAdapter(pool),
});
```

The PostgreSQL adapter lazily creates its execution-history table on first use. The pool is owned by the application and is not closed by `app.close()`.

```ts
import { createClient } from "redis";
import { FlowForge, RedisStorageAdapter } from "flowforge";

const client = createClient();
await client.connect();
const app = new FlowForge({
  storage: new RedisStorageAdapter(client, {
    keyPrefix: "my-service:flowforge",
    ttlSeconds: 60 * 60 * 24 * 30,
  }),
});
```

The Redis adapter uses standard Redis commands through the client's `sendCommand` method. FlowForge does not install or manage Redis or PostgreSQL clients. Both adapters store execution records and history; workflow definitions and running tasks remain in the application process.

Custom adapters implement `StorageAdapter`:

```ts
interface StorageAdapter {
  save(record: ExecutionRecord): Promise<void>;
  get(executionId: string): Promise<ExecutionRecord | undefined>;
  list(query?: { workflowId?: string; limit?: number }): Promise<ExecutionRecord[]>;
  delete(executionId: string): Promise<boolean>;
}
```

## Plugins and custom tasks

Plugins can register custom task handlers and lifecycle hooks:

```ts
import type { FlowForgePlugin } from "flowforge";

const greetingPlugin: FlowForgePlugin = {
  name: "greetings",
  install(app) {
    app.registerTask("greet", (args, context) => {
      const name = (args as { name: string }).name;
      context.log(`Greeting ${name}`);
      return `Hello, ${name}`;
    });
    app.addHook("afterStep", ({ execution, step }) => {
      console.info(execution.id, step?.id);
    });
  },
};

app.use(greetingPlugin);
app.create("welcome").use("greet-user", "greet", { name: "Ari" });
```

Hook names are `beforeWorkflow`, `afterWorkflow`, `beforeStep`, `afterStep`, and `onError`. A hook failure is reported as an execution failure rather than silently ignored. Plugin installation is synchronous; register dependencies before running workflows that use them.

## Configuration

```ts
const app = new FlowForge({
  concurrency: 5,       // max active workflow executions
  maxQueuedExecutions: 10_000,
  retries: 3,           // default task retries
  retryDelayMs: 250,    // initial retry delay
  maxRetryDelayMs: 30_000,
  taskTimeoutMs: 30_000,
  storage: new MemoryStorageAdapter(),
});
```

`run()` accepts `input`, `metadata`, `taskConcurrency`, `timeoutMs`, and `throwOnError`. A run-level timeout cancels the execution signal and returns status `timed_out`; step-level timeouts fail that step and may be retried.
`maxQueuedExecutions` bounds pending workflow runs; overflow is recorded as a failed execution with a queue-capacity error.

## TypeScript and module support

The package includes strict TypeScript declarations, ESM and CommonJS entry points, and source maps. It has no required runtime dependencies. Importing from TypeScript or JavaScript:

```ts
import { FlowForge } from "flowforge";
```

```js
const { FlowForge } = require("flowforge");
```

## Development

From this package directory:

```sh
npm install
npm run build
npm test
npm run typecheck
npm pack
```

The workspace uses pnpm; from the repository root, use `pnpm install`, then `pnpm --filter flowforge run build`, `pnpm --filter flowforge test`, and `pnpm --filter flowforge run typecheck`.

## Publishing

The package is prepared for npm publication under the name `flowforge`. Before publishing, review the package contents with `npm pack --dry-run` and confirm the npm name is available and the release details are correct. Publish only with an explicitly authorized npm account:

```sh
npm login
npm publish --access public
```

FlowForge is not published automatically by this project.

## License

MIT. See [LICENSE](./LICENSE).
