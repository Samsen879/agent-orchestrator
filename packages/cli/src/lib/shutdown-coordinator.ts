type CleanupTask = () => void | Promise<void>;

export class ShutdownCoordinator {
  private readonly tasks: CleanupTask[] = [];
  private readonly signalHandlers = new Map<NodeJS.Signals, () => void>();
  private cleanupPromise: Promise<void> | null = null;

  get isCleaningUp(): boolean {
    return this.cleanupPromise !== null;
  }

  add(task: CleanupTask): void {
    this.tasks.push(task);
  }

  installSignalHandlers(): void {
    if (this.signalHandlers.size > 0) return;

    for (const [signal, exitCode] of [
      ["SIGINT", 130],
      ["SIGTERM", 143],
    ] as const) {
      const handler = (): void => {
        void this.cleanupAndExit(exitCode);
      };
      this.signalHandlers.set(signal, handler);
      process.once(signal, handler);
    }
  }

  removeSignalHandlers(): void {
    for (const [signal, handler] of this.signalHandlers) {
      process.off(signal, handler);
    }
    this.signalHandlers.clear();
  }

  cleanup(): Promise<void> {
    if (this.cleanupPromise) return this.cleanupPromise;

    this.cleanupPromise = (async () => {
      this.removeSignalHandlers();
      for (const task of [...this.tasks].reverse()) {
        try {
          await task();
        } catch (error) {
          console.error(
            `AO shutdown cleanup failed: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
    })();

    return this.cleanupPromise;
  }

  async cleanupAndExit(exitCode: number): Promise<never> {
    await this.cleanup();
    process.exit(exitCode);
  }
}
