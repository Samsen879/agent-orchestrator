import { createServer, type Server } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { isPortAvailable, isPortListening } from "../../src/lib/web-dir.js";

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers
      .splice(0)
      .map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
});

async function listen(host: string, ipv6Only?: boolean): Promise<Server | null> {
  const server = createServer();
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen({ port: 0, host, ipv6Only }, resolve);
    });
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "EAFNOSUPPORT" || code === "EADDRNOTAVAIL") return null;
    throw error;
  }
  servers.push(server);
  return server;
}

function portOf(server: Server): number {
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Expected TCP address");
  return address.port;
}

describe("isPortAvailable", () => {
  it("detects an IPv4 listener", async () => {
    const server = await listen("0.0.0.0");
    expect(server).not.toBeNull();
    expect(await isPortAvailable(portOf(server!))).toBe(false);
  });

  it("detects an IPv6-only listener that an IPv4 connect probe would miss", async () => {
    const server = await listen("::", true);
    if (!server) return;
    expect(await isPortAvailable(portOf(server))).toBe(false);
  });

  it("reports a released port as available", async () => {
    const server = await listen("0.0.0.0");
    expect(server).not.toBeNull();
    const port = portOf(server!);
    servers.splice(servers.indexOf(server!), 1);
    await new Promise<void>((resolve) => server!.close(() => resolve()));

    expect(await isPortAvailable(port)).toBe(true);
  });
});

describe("isPortListening", () => {
  it("detects IPv4 readiness without taking ownership of the port", async () => {
    const server = await listen("0.0.0.0");
    expect(server).not.toBeNull();

    expect(await isPortListening(portOf(server!))).toBe(true);
    expect(server!.listening).toBe(true);
  });

  it("detects an IPv6-only listener", async () => {
    const server = await listen("::", true);
    if (!server) return;
    expect(await isPortListening(portOf(server))).toBe(true);
  });

  it("returns false after the listener closes", async () => {
    const server = await listen("0.0.0.0");
    expect(server).not.toBeNull();
    const port = portOf(server!);
    servers.splice(servers.indexOf(server!), 1);
    await new Promise<void>((resolve) => server!.close(() => resolve()));

    expect(await isPortListening(port)).toBe(false);
  });
});
