# TaskWorld

**TaskWorld** is an open-source workflow and automation engine for Node.js. Define reusable workflows in TypeScript or JavaScript, then execute sequential or concurrent tasks with retries, timeouts, lifecycle controls, events, schedules, plugins, and pluggable execution-history storage.

- **Package:** `taskworld`
- **Version:** `1.0.0`
- **Node.js:** 20+
- **License:** MIT
- **Modules:** ESM and CommonJS
- **Runtime dependencies:** None

## Installation

```bash
npm install taskworld
```

## Quick Start

```javascript
import { TaskWorld } from "taskworld";

const app = new TaskWorld({
  concurrency: 5,
  retries: 3
});

app
  .create("user-registration")
  .step("validate", async ({ input }) => {
    if (!input.username.trim()) {
      throw new Error("A username is required.");
    }

    return {
      username: input.username.trim(),
      email: input.email
    };
  })
  .step("save", ({ previous }) => ({
    id: `user-${previous.username}`,
    ...previous
  }));

await app.run("user-registration", {
  username: "alice",
  email: "alice@example.com"
});
```

## Features

- **Workflow Definitions:** Build reusable workflows using TypeScript or JavaScript.
- **Sequential Execution:** Execute tasks in a predefined order.
- **Concurrent Execution:** Run independent tasks simultaneously.
- **Automatic Retries:** Retry failed tasks with configurable retry limits.
- **Timeouts:** Set execution time limits for tasks and workflows.
- **Lifecycle Controls:** Manage workflow execution and task states.
- **Event System:** Listen to workflow and task lifecycle events.
- **Scheduling:** Schedule workflows for automatic execution.
- **Plugin System:** Extend functionality through custom plugins.
- **Execution History:** Persist execution history using pluggable storage adapters.
- **Dual Module Support:** Support both ESM and CommonJS environments.
- **Zero Runtime Dependencies:** Keep the runtime lightweight without external dependencies.

## Requirements

- Node.js 20 or newer
- npm, pnpm, or yarn

## Module Support

TaskWorld supports both ESM and CommonJS.

**ES Modules:**

```javascript
import { TaskWorld } from "taskworld";
```

**CommonJS:**

```javascript
const { TaskWorld } = require("taskworld");
```

## TypeScript Support

TaskWorld provides TypeScript type definitions for a better development experience.

```typescript
import { TaskWorld } from "taskworld";

interface UserInput {
  username: string;
  email: string;
}

const app = new TaskWorld({
  concurrency: 5,
  retries: 3
});

app
  .create<UserInput>("user-registration")
  .step("validate", async ({ input }) => {
    return {
      username: input.username.trim(),
      email: input.email
    };
  });
```

## Project Structure

```text
taskworld/
├── src/
│   ├── core/
│   ├── engine/
│   ├── events/
│   ├── plugins/
│   ├── storage/
│   ├── types/
│   └── index.ts
├── tests/
├── examples/
├── dist/
│   ├── esm/
│   ├── cjs/
│   └── types/
├── package.json
├── tsconfig.json
├── README.md
└── LICENSE
```

## Use Cases

TaskWorld can be used to build:

- Background job processing systems.
- Automated data processing pipelines.
- Multi-step application workflows.
- Scheduled maintenance tasks.
- Event-driven automation systems.
- API integration and synchronization pipelines.
- Custom task orchestration engines.

## License

TaskWorld is released under the [MIT License](LICENSE).

---

**TaskWorld** — A lightweight, extensible workflow and automation engine for Node.js.
