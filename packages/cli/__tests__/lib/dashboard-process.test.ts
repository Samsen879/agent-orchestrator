import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockIsPortListening } = vi.hoisted(() => ({
  mockIsPortListening: vi.fn(),
}));

vi.mock("../../src/lib/web-dir.js", () => ({
  isPortListening: (...args: unknown[]) => mockIsPortListening(...args),
}));

import {
  stopDashboardProcessTree,
  waitForDashboardReady,
} from "../../src/lib/dashboard-process.js";

function fakeChild(overrides: Partial<ChildProcess> = {}): ChildProcess {
  return Object.assign(new EventEmitter(), {
    pid: 4242,
    exitCode: null,
    signalCode: null,
    kill: vi.fn(),
    ...overrides,
  }) as ChildProcess;
}

beforeEach(() => {
  mockIsPortListening.mockReset();
});

describe("waitForDashboardReady", () => {
  it("requires two consecutive occupied probes", async () => {
    mockIsPortListening
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true);

    await waitForDashboardReady(fakeChild(), [3310], 1_000);

    expect(mockIsPortListening).toHaveBeenCalledTimes(3);
  });

  it("fails when the dashboard runner already exited", async () => {
    const child = fakeChild({ exitCode: 0 });

    await expect(waitForDashboardReady(child, [3310], 1_000)).rejects.toThrow(
      "exited before becoming ready",
    );
    expect(mockIsPortListening).not.toHaveBeenCalled();
  });

  it("waits for the dashboard and both terminal services", async () => {
    mockIsPortListening
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true);

    await waitForDashboardReady(fakeChild(), [3310, 14810, 14811], 1_000);

    expect(mockIsPortListening).toHaveBeenCalledTimes(9);
  });
});

describe("stopDashboardProcessTree", () => {
  it.runIf(process.platform !== "win32")("signals the detached process group", async () => {
    const child = fakeChild();
    const killSpy = vi.spyOn(process, "kill").mockImplementation(() => {
      Object.defineProperty(child, "exitCode", { value: 0, configurable: true });
      return true;
    });

    await stopDashboardProcessTree(child, 100);

    expect(killSpy).toHaveBeenCalledWith(-4242, "SIGTERM");
  });
});
