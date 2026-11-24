import { describe, expect, it } from "vitest";
import { matchesCron, parseCron } from "../src/scheduler/cron.js";
import { Scheduler } from "../src/scheduler/scheduler.js";

describe("cron expressions", () => {
  it("matches lists, ranges, and steps", () => {
    const cron = parseCron("*/15 9-10 * * 1-5");
    expect(matchesCron(cron, new Date(2026, 5, 1, 9, 30))).toBe(true);
    expect(matchesCron(cron, new Date(2026, 5, 1, 9, 31))).toBe(false);
    expect(matchesCron(cron, new Date(2026, 5, 7, 9, 30))).toBe(false);
  });

  it("applies cron day-of-month and weekday rules with wildcard steps", () => {
    const cron = parseCron("0 0 */1 * 1");
    expect(matchesCron(cron, new Date(2026, 5, 1, 0, 0))).toBe(true);
    expect(matchesCron(cron, new Date(2026, 5, 2, 0, 0))).toBe(false);
  });

  it("rejects malformed cron expressions", () => {
    expect(() => parseCron("every hour")).toThrow(/five fields/);
    expect(() => parseCron("99 * * * *")).toThrow(/Invalid cron/);
  });
});

describe("scheduler", () => {
  it("runs delayed work once and removes the timer", async () => {
    const runs: string[] = [];
    const scheduler = new Scheduler(async (workflowId) => {
      runs.push(workflowId);
    });
    scheduler.delay("delayed", 5);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(runs).toEqual(["delayed"]);
    expect(scheduler.size).toBe(0);
    scheduler.close();
  });

  it("cancels recurring schedules and closes all timers", () => {
    const scheduler = new Scheduler(async () => undefined);
    const id = scheduler.interval("recurring", 1000);
    expect(scheduler.cancel(id)).toBe(true);
    expect(scheduler.cancel(id)).toBe(false);
    scheduler.interval("other", 1000);
    scheduler.close();
    expect(scheduler.size).toBe(0);
  });
});
