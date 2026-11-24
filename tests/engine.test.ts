import { afterEach, describe, expect, it } from "vitest";
import { FlowForge } from "../src/index.js";

const engines: FlowForge[] = [];

function engine(options: ConstructorParameters<typeof FlowForge>[0] = {}): FlowForge {
  const app = new FlowForge(options);
  engines.push(app);
  return app;
}

afterEach(async () => {
  await Promise.all(engines.splice(0).map((app) => app.close()));
});

describe("FlowForge execution", () => {
  it("runs sequential steps and passes input and previous output", async () => {
    const app = engine();
    app
      .create<{ value: number }>("sequence")
      .step("double", ({ input }) => ({ doubled: input.value * 2 }))
      .step("add-one", ({ previous }) => previous.doubled + 1);

    const result = await app.run("sequence", { input: { value: 4 } });

    expect(result.status).toBe("succeeded");
    expect(result.output).toBe(9);
    expect(result.steps.map((step) => step.status)).toEqual(["succeeded", "succeeded"]);
    expect(result.progress).toBe(100);
  });

  it("chains workflows by passing outputs to the next workflow", async () => {
    const app = engine();
    app.create<{ value: number }>("first").step("multiply", ({ input }) => input.value * 3);
    app.create<number>("second").step("increment", ({ input }) => input + 1);

    const result = await app.chain(["first", "second"], {
      input: { value: 4 },
    });

    expect(result.status).toBe("succeeded");
    expect(result.executions).toHaveLength(2);
    expect(result.output).toBe(13);
  });

  it("executes independent tasks in parallel and joins their outputs", async () => {
    const app = engine();
    app
      .create("parallel")
      .parallel([
        { id: "left", run: async () => "L" },
        { id: "right", run: async () => "R" },
      ])
      .step("join", ({ previous }) => previous);

    const result = await app.run("parallel", { taskConcurrency: 2 });

    expect(result.status).toBe("succeeded");
    expect(result.output).toEqual({ left: "L", right: "R" });
  });

  it("honors task concurrency limits", async () => {
    const app = engine();
    let active = 0;
    let maximum = 0;
    const measure = async (): Promise<void> => {
      active += 1;
      maximum = Math.max(maximum, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
    };
    app.create("limited").parallel([
      { id: "one", run: measure },
      { id: "two", run: measure },
      { id: "three", run: measure },
    ]);

    await app.run("limited", { taskConcurrency: 2 });

    expect(maximum).toBe(2);
  });

  it("retries with exponential backoff and records attempts", async () => {
    const app = engine();
    let attempts = 0;
    app.create("retry").step(
      "flaky",
      () => {
        attempts += 1;
        if (attempts < 3) throw new Error("temporary");
        return "recovered";
      },
      { retries: 2, retryDelayMs: 0 },
    );

    const result = await app.run("retry");

    expect(result.status).toBe("succeeded");
    expect(result.output).toBe("recovered");
    expect(result.steps[0]?.attempts).toBe(3);
    expect(result.logs.filter((log) => log.level === "warn")).toHaveLength(2);
  });

  it("supports explicit recovery from a task failure", async () => {
    const app = engine();
    app.create("recovery").step(
      "recoverable",
      () => {
        throw new Error("source unavailable");
      },
      {
        retries: 2,
        retryDelayMs: 0,
        recover: (error) => ({ recovered: error.message }),
      },
    );

    const result = await app.run("recovery");

    expect(result.status).toBe("succeeded");
    expect(result.output).toEqual({ recovered: "source unavailable" });
    expect(result.steps[0]?.attempts).toBe(3);
  });

  it("fails a task after its timeout", async () => {
    const app = engine();
    let signalAborted = false;
    app
      .create("timeout")
      .step(
        "slow",
        ({ signal }) =>
          new Promise((resolve) => {
            signal.addEventListener("abort", () => {
              signalAborted = true;
              resolve(undefined);
            });
          }),
        { timeoutMs: 5 },
      );

    const result = await app.run("timeout");

    expect(result.status).toBe("failed");
    expect(result.steps[0]?.error?.name).toBe("TimeoutError");
    expect(signalAborted).toBe(true);
  });

  it("marks a whole-workflow timeout distinctly", async () => {
    const app = engine();
    app
      .create("workflow-timeout")
      .step("slow", () => new Promise((resolve) => setTimeout(resolve, 100)));

    const result = await app.run("workflow-timeout", { timeoutMs: 5 });

    expect(result.status).toBe("timed_out");
  });

  it("pauses between tasks and resumes", async () => {
    const app = engine();
    let signalStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      signalStarted = resolve;
    });
    let secondStarted = false;
    app
      .create("pause")
      .step("first", async () => {
        signalStarted();
        return "done";
      })
      .step("second", () => {
        secondStarted = true;
      });

    const handle = await app.start("pause");
    await started;
    expect(handle.pause()).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 15));
    expect(secondStarted).toBe(false);
    expect(handle.resume()).toBe(true);
    expect((await handle.result).status).toBe("succeeded");
    expect(secondStarted).toBe(true);
  });

  it("cancels active executions", async () => {
    const app = engine();
    app.create("cancel").step(
      "wait",
      ({ signal }) =>
        new Promise((_, reject) => {
          signal.addEventListener(
            "abort",
            () => reject(signal.reason),
            { once: true },
          );
        }),
    );
    const handle = await app.start("cancel");
    expect(handle.cancel("requested")).toBe(true);
    expect((await handle.result).status).toBe("cancelled");
  });

  it("bounds the queue and returns a clear error when it is full", async () => {
    const app = engine({ concurrency: 1, maxQueuedExecutions: 1 });
    app.create("queued").step(
      "wait",
      () => new Promise((resolve) => setTimeout(resolve, 20)),
    );

    const running = await app.start("queued");
    const waiting = await app.start("queued");
    const overflow = await app.start("queued");
    const overflowResult = await overflow.result;

    expect(overflowResult.status).toBe("failed");
    expect(overflowResult.error?.message).toMatch(/queue is full/);
    await Promise.all([running.result, waiting.result]);
  });

  it("stores execution history and supports filters", async () => {
    const app = engine();
    app.create("one").step("work", () => 1);
    app.create("two").step("work", () => 2);
    await app.run("one");
    await app.run("two");

    expect(await app.history({ workflowId: "one" })).toHaveLength(1);
    expect(await app.history({ limit: 1 })).toHaveLength(1);
  });

  it("starts a workflow from an emitted event", async () => {
    const app = engine();
    app.create<{ value: number }>("triggered").step("capture", ({ input }) => input.value);
    app.onEvent<{ value: number }>("data:ready", "triggered");

    await app.emit("data:ready", { value: 17 });

    expect((await app.history({ workflowId: "triggered" }))[0]?.output).toBe(17);
  });

  it("runs registered plugin tasks and lifecycle hooks", async () => {
    const app = engine();
    const hookCalls: string[] = [];
    app.use({
      name: "test-plugin",
      install(instance) {
        instance.registerTask("format", (args) => `hello ${String(args)}`);
        instance.addHook("afterStep", ({ step }) => {
          hookCalls.push(step?.id ?? "missing");
        });
      },
    });
    app.create("plugin").use("greet", "format", "FlowForge");

    const result = await app.run("plugin");

    expect(result.output).toBe("hello FlowForge");
    expect(hookCalls).toEqual(["greet"]);
  });

  it("rejects unknown dependencies and dependency cycles before running", async () => {
    const app = engine();
    app.create("unknown").step("a", () => 1, { dependsOn: ["missing"] });
    await expect(app.run("unknown")).rejects.toThrow(/unknown step/);

    app
      .create("cycle")
      .task("a", () => 1, { dependsOn: ["b"] })
      .task("b", () => 2, { dependsOn: ["a"] });
    await expect(app.run("cycle")).rejects.toThrow(/dependency cycle/);
  });
});
