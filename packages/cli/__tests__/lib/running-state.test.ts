import { afterEach, describe, expect, it, vi } from "vitest";
import { join } from "node:path";
import type { RunningState } from "../../src/lib/running-state.js";

const TEST_HOME = "/tmp/ao-running-state-home";
const STATE_DIR = join(TEST_HOME, ".agent-orchestrator");
const STATE_FILE = join(STATE_DIR, "running.json");
const LOCK_FILE = join(STATE_DIR, "running.lock");

type FsMockOptions = {
  openSyncImpl?: () => number;
};

type RunningStateModule = typeof import("../../src/lib/running-state.js");

function makeFsError(code: string, message = code): NodeJS.ErrnoException {
  const error = new Error(message) as NodeJS.ErrnoException;
  error.code = code;
  return error;
}

async function loadRunningStateWithMocks(options: FsMockOptions = {}): Promise<{
  module: RunningStateModule;
  mockOpenSync: ReturnType<typeof vi.fn>;
  mockCloseSync: ReturnType<typeof vi.fn>;
  mockMkdirSync: ReturnType<typeof vi.fn>;
  mockReadFileSync: ReturnType<typeof vi.fn>;
  mockSleep: ReturnType<typeof vi.fn>;
  mockUnlinkSync: ReturnType<typeof vi.fn>;
  mockWriteFileSync: ReturnType<typeof vi.fn>;
}> {
  vi.resetModules();

  let now = 0;
  vi.spyOn(Date, "now").mockImplementation(() => now);

  const mockOpenSync = vi.fn(options.openSyncImpl ?? (() => 123));
  const mockCloseSync = vi.fn();
  const mockMkdirSync = vi.fn();
  const mockReadFileSync = vi.fn();
  const mockWriteFileSync = vi.fn();
  const mockUnlinkSync = vi.fn();
  const mockSleep = vi.fn(async (ms?: number) => {
    now += ms ?? 0;
  });

  vi.doMock("node:fs", () => ({
    readFileSync: mockReadFileSync,
    writeFileSync: mockWriteFileSync,
    mkdirSync: mockMkdirSync,
    unlinkSync: mockUnlinkSync,
    openSync: mockOpenSync,
    closeSync: mockCloseSync,
    constants: {
      O_CREAT: 0x40,
      O_EXCL: 0x80,
      O_WRONLY: 0x1,
    },
  }));

  vi.doMock("node:os", () => ({
    homedir: () => TEST_HOME,
  }));

  vi.doMock("node:timers/promises", () => ({
    setTimeout: mockSleep,
  }));

  const module = await import("../../src/lib/running-state.js");

  return {
    module,
    mockOpenSync,
    mockCloseSync,
    mockMkdirSync,
    mockReadFileSync,
    mockSleep,
    mockUnlinkSync,
    mockWriteFileSync,
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.resetModules();
});

describe("running-state lock handling", () => {
  it("retries EEXIST lock contention until it acquires the lock", async () => {
    const openErrors = [
      makeFsError("EEXIST", "lock exists"),
      makeFsError("EEXIST", "lock exists"),
    ];

    const state: RunningState = {
      pid: 42,
      configPath: "/tmp/agent-orchestrator.yaml",
      port: 3310,
      startedAt: "2026-04-03T00:00:00.000Z",
      projects: ["ciecopilot-home"],
    };

    const {
      module,
      mockOpenSync,
      mockSleep,
      mockWriteFileSync,
      mockUnlinkSync,
    } = await loadRunningStateWithMocks({
      openSyncImpl: () => {
        const nextError = openErrors.shift();
        if (nextError) throw nextError;
        return 123;
      },
    });

    await expect(module.register(state)).resolves.toBeUndefined();

    expect(mockOpenSync).toHaveBeenCalledTimes(3);
    expect(mockSleep).toHaveBeenCalledTimes(2);
    expect(mockWriteFileSync).toHaveBeenCalledWith(
      STATE_FILE,
      JSON.stringify(state, null, 2),
      "utf-8",
    );
    expect(mockUnlinkSync).toHaveBeenCalledWith(LOCK_FILE);
  });

  it.each(["EACCES", "EPERM"])(
    "surfaces %s as a permission error instead of a lock-contention timeout",
    async (code) => {
      const { module, mockOpenSync, mockSleep } = await loadRunningStateWithMocks({
        openSyncImpl: () => {
          throw makeFsError(code, "permission denied");
        },
      });

      await expect(module.unregister()).rejects.toThrow(
        `Cannot access ${LOCK_FILE} (${code})`,
      );
      expect(mockOpenSync).toHaveBeenCalledTimes(1);
      expect(mockSleep).not.toHaveBeenCalled();
    },
  );
});
