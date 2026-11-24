import type {
  StepOptions,
  TaskHandler,
  WorkflowContext,
  WorkflowDefinition,
  WorkflowStep,
} from "../types.js";

/** Fluent workflow definition builder. Definitions are registered immediately. */
export class WorkflowBuilder<TInput = unknown, TPrevious = unknown> {
  private lastStepIds: string[] = [];

  constructor(private readonly definition: WorkflowDefinition<TInput>) {}

  /** Add the next sequential task. */
  step<TOutput>(
    id: string,
    run: TaskHandler<TInput, TOutput, TPrevious>,
    options: StepOptions<TInput, TPrevious> = {},
  ): WorkflowBuilder<TInput, TOutput> {
    this.addStep(id, run, {
      ...options,
      dependsOn: [...(options.dependsOn ?? this.lastStepIds)],
    });
    this.lastStepIds = [id];
    return this as unknown as WorkflowBuilder<TInput, TOutput>;
  }

  /**
   * Add a task to the workflow graph. Without `dependsOn`, it follows the
   * previously added step; pass `dependsOn: []` to make it an initial task.
   */
  task<TOutput>(
    id: string,
    run: TaskHandler<TInput, TOutput, TPrevious>,
    options: StepOptions<TInput, TPrevious> = {},
  ): WorkflowBuilder<TInput, TOutput> {
    const dependencies = options.dependsOn ?? this.lastStepIds;
    this.addStep(id, run, { ...options, dependsOn: [...dependencies] });
    this.lastStepIds = [id];
    return this as unknown as WorkflowBuilder<TInput, TOutput>;
  }

  /** Add several tasks that share the same dependencies and run concurrently. */
  parallel(
    steps: Array<{
      id: string;
      run: TaskHandler<TInput, unknown, TPrevious>;
      options?: StepOptions<TInput, TPrevious>;
    }>,
  ): WorkflowBuilder<TInput, Record<string, unknown>> {
    if (steps.length === 0) throw new Error("parallel() requires at least one task.");
    const dependencies = [...this.lastStepIds];
    const ids: string[] = [];
    for (const item of steps) {
      this.addStep(item.id, item.run, {
        ...item.options,
        dependsOn: item.options?.dependsOn ?? dependencies,
      });
      ids.push(item.id);
    }
    this.lastStepIds = ids;
    return this as unknown as WorkflowBuilder<TInput, Record<string, unknown>>;
  }

  /** Use a task registered directly or by an installed plugin. */
  use(
    id: string,
    taskName: string,
    args?: unknown,
    options: StepOptions<TInput, TPrevious> = {},
  ): WorkflowBuilder<TInput, unknown> {
    return this.task(
      id,
      (context: WorkflowContext<TInput, TPrevious>) => {
        if (!this.taskResolver) {
          throw new Error(
            `Custom task "${taskName}" is unavailable. Register it before execution.`,
          );
        }
        return this.taskResolver(taskName, args, context);
      },
      options,
    );
  }

  /** @internal Connects custom tasks without coupling the builder to the engine. */
  setTaskResolver(
    resolver: (
      name: string,
      args: unknown,
      context: WorkflowContext<TInput, TPrevious>,
    ) => unknown | Promise<unknown>,
  ): void {
    this.taskResolver = resolver;
  }

  getDefinition(): WorkflowDefinition<TInput> {
    return this.definition;
  }

  private taskResolver?: (
    name: string,
    args: unknown,
    context: WorkflowContext<TInput, TPrevious>,
  ) => unknown | Promise<unknown>;

  private addStep(
    id: string,
    run: TaskHandler<TInput, unknown, TPrevious>,
    options: StepOptions<TInput, TPrevious>,
  ): void {
    if (!id.trim()) throw new Error("Workflow step id cannot be empty.");
    if (this.definition.steps.some((step) => step.id === id)) {
      throw new Error(`Workflow "${this.definition.id}" already has a step named "${id}".`);
    }
    if (typeof run !== "function") {
      throw new TypeError(`Workflow step "${id}" must have a task function.`);
    }
    const step: WorkflowStep<TInput, TPrevious> = { id, run, options };
    this.definition.steps.push(step as unknown as WorkflowStep<TInput, unknown>);
  }
}
