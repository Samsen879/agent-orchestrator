import { spawn, type ChildProcess } from "node:child_process";
import { isPortListening } from "./web-dir.js";

const READY_POLL_MS = 100;
const STOP_POLL_MS = 50;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeExit(code: number | null, signal: NodeJS.Signals | null): string {
  if (signal) return `signal ${signal}`;
  return `code ${code ?? "unknown"}`;
}

/**
 * Wait until the dashboard owns its port, failing if its runner exits first.
 * Two consecutive occupied probes avoid declaring success on a transient bind.
 */
export async function waitForDashboardReady(
  child: ChildProcess,
  ports: number[],
  timeoutMs = 30_000,
): Promise<void> {
  const uniquePorts = [...new Set(ports)];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isChildRunning(child)) {
      throw new Error(
        `Dashboard exited before becoming ready (${describeExit(child.exitCode, child.signalCode)})`,
      );
    }

    const firstProbe = await Promise.all(uniquePorts.map((port) => isPortListening(port)));
    if (firstProbe.every(Boolean)) {
      await delay(READY_POLL_MS);
      if (!isChildRunning(child)) {
        throw new Error(
          `Dashboard exited before becoming ready (${describeExit(child.exitCode, child.signalCode)})`,
        );
      }
      const secondProbe = await Promise.all(uniquePorts.map((port) => isPortListening(port)));
      if (secondProbe.every(Boolean)) return;
    }

    await delay(READY_POLL_MS);
  }

  throw new Error(
    `Dashboard services did not become ready on ports ${uniquePorts.join(", ")} within ${timeoutMs}ms`,
  );
}

function isChildRunning(child: ChildProcess): boolean {
  return child.exitCode === null && child.signalCode === null;
}

async function waitForChildExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isChildRunning(child)) return true;
    await delay(STOP_POLL_MS);
  }
  return !isChildRunning(child);
}

async function taskkill(pid: number): Promise<void> {
  await new Promise<void>((resolve) => {
    const killer = spawn("taskkill", ["/PID", String(pid), "/T", "/F"], {
      stdio: "ignore",
    });
    killer.once("error", () => resolve());
    killer.once("exit", () => resolve());
  });
}

/** Stop a detached dashboard runner and every child it spawned. */
export async function stopDashboardProcessTree(
  child: ChildProcess,
  timeoutMs = 5_000,
): Promise<void> {
  const pid = child.pid;
  if (!pid || !isChildRunning(child)) return;

  if (process.platform === "win32") {
    await taskkill(pid);
    return;
  }

  try {
    process.kill(-pid, "SIGTERM");
  } catch {
    try {
      child.kill("SIGTERM");
    } catch {
      return;
    }
  }

  if (await waitForChildExit(child, timeoutMs)) return;

  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      // Process already exited.
    }
  }

  await waitForChildExit(child, timeoutMs);
}
