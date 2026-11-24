import type { FlowForge } from "../core/engine.js";
import type { WorkflowContext, ExecutionRecord, StepExecutionRecord } from "../types.js";

export type HookName =
  | "beforeWorkflow"
  | "afterWorkflow"
  | "beforeStep"
  | "afterStep"
  | "onError";

export interface HookContext {
  execution: ExecutionRecord;
  step?: StepExecutionRecord;
  taskContext?: WorkflowContext;
  error?: Error;
}

export type FlowForgeHook = (context: HookContext) => void | Promise<void>;

export interface FlowForgePlugin {
  name: string;
  install(app: FlowForge): void;
}

export type RegisteredTask = (
  args: unknown,
  context: WorkflowContext,
) => unknown | Promise<unknown>;

export class PluginManager {
  private readonly pluginNames = new Set<string>();
  private readonly hooks = new Map<HookName, Set<FlowForgeHook>>();
  private readonly tasks = new Map<string, RegisteredTask>();

  addHook(name: HookName, hook: FlowForgeHook): () => void {
    const set = this.hooks.get(name) ?? new Set<FlowForgeHook>();
    set.add(hook);
    this.hooks.set(name, set);
    return () => set.delete(hook);
  }

  async runHooks(name: HookName, context: HookContext): Promise<void> {
    const outcomes = await Promise.allSettled(
      [...(this.hooks.get(name) ?? [])].map((hook) => hook(context)),
    );
    const rejected = outcomes.find(
      (outcome): outcome is PromiseRejectedResult => outcome.status === "rejected",
    );
    if (rejected) {
      throw new Error(`FlowForge ${name} hook failed.`, { cause: rejected.reason });
    }
  }

  addPlugin(plugin: FlowForgePlugin, app: FlowForge): void {
    if (!plugin.name.trim()) throw new Error("Plugin name cannot be empty.");
    if (this.pluginNames.has(plugin.name)) {
      throw new Error(`Plugin "${plugin.name}" is already installed.`);
    }
    this.pluginNames.add(plugin.name);
    try {
      plugin.install(app);
    } catch (error) {
      this.pluginNames.delete(plugin.name);
      throw error;
    }
  }

  registerTask(name: string, task: RegisteredTask): void {
    if (!name.trim()) throw new Error("Custom task name cannot be empty.");
    if (this.tasks.has(name)) {
      throw new Error(`Custom task "${name}" is already registered.`);
    }
    this.tasks.set(name, task);
  }

  getTask(name: string): RegisteredTask {
    const task = this.tasks.get(name);
    if (!task) {
      throw new Error(
        `Custom task "${name}" is not registered. Install its plugin or register it before running the workflow.`,
      );
    }
    return task;
  }
}
