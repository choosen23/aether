import { describe, expect, it, vi } from "vitest";

import { MobilityClient } from "./mobility-client";
import { MobilityStore } from "./mobility-store";

class FakeSocket {
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;

  emit(message: unknown): void {
    this.onmessage?.({ data: JSON.stringify(message) });
  }

  close(): void {}

  shutdown(): void {
    this.onclose?.();
  }
}

const upsert = (cellId: string) => ({
  message_type: "mobility.cell.upsert" as const,
  schema_version: 1 as const,
  sent_at: "2026-09-24T09:10:10Z",
  cells: [
    {
      density: {
        event_id: `d-${cellId}`,
        event_type: "mobility.density" as const,
        schema_version: 1 as const,
        generated_at: "2026-09-24T09:10:10Z",
        window_started_at: "2026-09-24T09:07:10Z",
        window_ended_at: "2026-09-24T09:10:10Z",
        source_event_watermark: "2026-09-24T09:10:00Z",
        cell_id: cellId,
        h3_resolution: 4,
        aircraft_count: 1,
        airborne_count: 1,
        on_ground_count: 0,
        average_altitude_m: 1000,
        average_ground_speed_mps: 200,
        provenance: "derived" as const,
      },
      demand: {
        event_id: `r-${cellId}`,
        event_type: "demand.region" as const,
        schema_version: 1 as const,
        generated_at: "2026-09-24T09:10:10Z",
        window_started_at: "2026-09-24T09:07:10Z",
        window_ended_at: "2026-09-24T09:10:10Z",
        cell_id: cellId,
        h3_resolution: 4,
        aircraft_count: 1,
        demand_units: 1,
        model_version: "density-linear-v1" as const,
        coefficient_set: "default-v1" as const,
        provenance: "modelled" as const,
      },
    },
  ],
});

const snapshot = (cellId: string) => ({
  generated_at: "2026-09-24T09:10:00Z",
  source_status: {
    source: "opensky" as const,
    status: "live" as const,
    last_observation_at: "2026-09-24T09:10:00Z",
    age_seconds: 0,
  },
  cells: upsert(cellId).cells,
});

describe("MobilityClient", () => {
  it("replaces the store from a snapshot before applying stream deltas", async () => {
    const store = new MobilityStore();
    const socket = new FakeSocket();
    const client = new MobilityClient({
      store,
      wsUrl: "ws://example.test/mobility",
      fetchSnapshot: async () => snapshot("841f1d7ffffffff"),
      openSocket: () => socket,
    });

    await client.start();
    expect(store.ids()).toEqual(["841f1d7ffffffff"]);

    socket.emit(upsert("841f1d5ffffffff"));
    expect(store.ids()).toEqual(["841f1d5ffffffff", "841f1d7ffffffff"]);
  });

  it("buffers stream deltas received while the snapshot is loading", async () => {
    const store = new MobilityStore();
    const socket = new FakeSocket();
    let resolveSnapshot!: (value: ReturnType<typeof snapshot>) => void;
    const pendingSnapshot = new Promise<ReturnType<typeof snapshot>>((resolve) => {
      resolveSnapshot = resolve;
    });
    const client = new MobilityClient({
      store,
      wsUrl: "ws://example.test/mobility",
      fetchSnapshot: async () => pendingSnapshot,
      openSocket: () => socket,
    });

    const starting = client.start();
    socket.emit(upsert("841f1d5ffffffff"));
    resolveSnapshot(snapshot("841f1d7ffffffff"));
    await starting;

    expect(store.ids()).toEqual(["841f1d5ffffffff", "841f1d7ffffffff"]);
  });

  it("retries when loading the snapshot fails", async () => {
    const store = new MobilityStore();
    const socket = new FakeSocket();
    const retry = vi.fn();
    let attempts = 0;
    const client = new MobilityClient({
      store,
      wsUrl: "ws://example.test/mobility",
      fetchSnapshot: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("temporary failure");
        return snapshot("841f1d7ffffffff");
      },
      openSocket: () => socket,
      timerApi: {
        setTimeout: (callback) => {
          retry.mockImplementation(callback);
          return 1;
        },
        clearTimeout: () => undefined,
      },
    });

    await client.start();
    expect(retry).toHaveBeenCalledTimes(0);
    retry();
    await Promise.resolve();
    await Promise.resolve();

    expect(attempts).toBe(2);
    expect(store.ids()).toEqual(["841f1d7ffffffff"]);
  });
});
