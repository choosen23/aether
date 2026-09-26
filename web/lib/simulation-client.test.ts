import { describe, expect, it, vi } from "vitest";

import { WebSimulationClient } from "./simulation-client";
import { SimulationStore } from "./simulation-store";

class FakeSocket {
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;

  emit(message: unknown): void {
    this.onmessage?.({ data: JSON.stringify(message) });
  }

  emitRaw(data: string): void {
    this.onmessage?.({ data });
  }

  close(): void {}

  shutdown(): void {
    this.onclose?.();
  }
}

const snapshot = {
  runs: [],
  run: null,
};

describe("WebSimulationClient", () => {
  it("ignores malformed websocket messages", async () => {
    const store = new SimulationStore();
    const socket = new FakeSocket();
    const client = new WebSimulationClient({
      store,
      wsUrl: "ws://example.test/simulation",
      fetchSnapshot: async () => snapshot,
      openSocket: () => socket,
    });

    await client.start();
    socket.emitRaw("{not-json");
    socket.emit({ message_type: "simulation.unknown", sent_at: "2026-09-27T10:00:00Z" });

    expect(store.getSnapshot().runs).toEqual([]);
  });

  it("caps exponential retry delay", async () => {
    const store = new SimulationStore();
    const socket = new FakeSocket();
    const delays: number[] = [];
    const timerApi = {
      setTimeout: (callback: () => void, delayMs: number) => {
        delays.push(delayMs);
        return setTimeout(callback, 0) as unknown as number;
      },
      clearTimeout: () => undefined,
    };
    const client = new WebSimulationClient({
      store,
      wsUrl: "ws://example.test/simulation",
      fetchSnapshot: async () => snapshot,
      openSocket: () => socket,
      timerApi,
      retryBaseMs: 50,
      retryMaxMs: 100,
    });

    await client.start();
    for (let i = 0; i < 4; i += 1) {
      socket.shutdown();
      await Promise.resolve();
    }

    expect(delays.length).toBeGreaterThan(0);
    expect(Math.max(...delays)).toBeLessThanOrEqual(120);
  });
});
