import { afterEach, describe, expect, it, vi } from "vitest";
import { ShutdownCoordinator } from "../../src/lib/shutdown-coordinator.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("ShutdownCoordinator", () => {
  it("runs cleanup tasks once in reverse registration order", async () => {
    const order: string[] = [];
    const shutdown = new ShutdownCoordinator();
    shutdown.add(() => order.push("dashboard"));
    shutdown.add(async () => {
      order.push("lifecycle");
    });
    shutdown.add(() => order.push("orchestrator"));

    await Promise.all([shutdown.cleanup(), shutdown.cleanup()]);

    expect(order).toEqual(["orchestrator", "lifecycle", "dashboard"]);
  });

  it("continues cleanup when one task fails", async () => {
    const finalTask = vi.fn();
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const shutdown = new ShutdownCoordinator();
    shutdown.add(finalTask);
    shutdown.add(() => {
      throw new Error("cleanup failed");
    });

    await shutdown.cleanup();

    expect(finalTask).toHaveBeenCalledOnce();
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("cleanup failed"));
  });

  it("installs and removes SIGINT/SIGTERM handlers", async () => {
    const shutdown = new ShutdownCoordinator();
    const beforeInt = process.listenerCount("SIGINT");
    const beforeTerm = process.listenerCount("SIGTERM");

    shutdown.installSignalHandlers();
    expect(process.listenerCount("SIGINT")).toBe(beforeInt + 1);
    expect(process.listenerCount("SIGTERM")).toBe(beforeTerm + 1);

    await shutdown.cleanup();
    expect(process.listenerCount("SIGINT")).toBe(beforeInt);
    expect(process.listenerCount("SIGTERM")).toBe(beforeTerm);
  });
});
