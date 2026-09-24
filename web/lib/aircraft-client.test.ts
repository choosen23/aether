import { describe, expect, it } from "vitest";

import { AircraftClient } from "./aircraft-client";
import { AircraftStore } from "./aircraft-store";
import type { AircraftSnapshot, ProjectedAircraft } from "./contracts";

class FakeSocket {
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;

  closeCalled = false;

  close(): void {
    this.closeCalled = true;
  }

  emit(message: unknown): void {
    this.onmessage?.({ data: JSON.stringify(message) });
  }

  open(): void {
    this.onopen?.();
  }

  shutdown(): void {
    this.onclose?.();
  }
}

const aircraft = (icao24: string): ProjectedAircraft => ({
  icao24,
  callsign: `${icao24.toUpperCase()}-CALL`,
  origin_country: "Unknown",
  position: { latitude: 50, longitude: 8, barometric_altitude_m: 10000, on_ground: false },
  motion: { ground_speed_mps: 200, track_degrees: 90, vertical_rate_mps: 0 },
  last_contact_at: "2026-09-24T09:10:00Z",
  position_source: "ads_b",
  event_id: `event-${icao24}`,
  observed_at: "2026-09-24T09:10:00Z",
});

const snapshot = (aircraftList: ProjectedAircraft[]): AircraftSnapshot => ({
  generated_at: "2026-09-24T09:10:00Z",
  source_status: {
    source: "opensky",
    status: "live",
    last_observation_at: "2026-09-24T09:10:00Z",
    age_seconds: 0,
  },
  aircraft: aircraftList,
});

describe("AircraftClient", () => {
  it("replaces state from reconnect snapshot before applying deltas", async () => {
    const store = new AircraftStore();
    const sockets: FakeSocket[] = [];
    const snapshots = [snapshot([aircraft("a00001")]), snapshot([aircraft("b00002")])];
    let snapshotIndex = 0;

    const client = new AircraftClient({
      store,
      wsUrl: "ws://example.test/aircraft",
      fetchSnapshot: async () => snapshots[snapshotIndex++],
      openSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket;
      },
      timerApi: {
        setTimeout: (cb) => {
          cb();
          return 1;
        },
        clearTimeout: () => undefined,
      },
      retryBaseMs: 1,
    });

    await client.start();
    expect(store.ids()).toEqual(["a00001"]);

    sockets[0].shutdown();
    await Promise.resolve();
    await Promise.resolve();
    expect(store.ids()).toEqual(["b00002"]);

    sockets[1].emit({
      message_type: "aircraft.upsert",
      schema_version: 1,
      sent_at: "2026-09-24T09:10:01Z",
      aircraft: [aircraft("c00003")],
    });
    expect(store.ids()).toEqual(["b00002", "c00003"]);
  });

  it("aborts fetch and closes socket on stop", async () => {
    const store = new AircraftStore();
    const socket = new FakeSocket();
    let aborted = false;

    const client = new AircraftClient({
      store,
      wsUrl: "ws://example.test/aircraft",
      fetchSnapshot: async (signal) => {
        signal.addEventListener("abort", () => {
          aborted = true;
        });
        return snapshot([aircraft("a00001")]);
      },
      openSocket: () => socket,
    });

    await client.start();
    client.stop();

    expect(aborted).toBe(true);
    expect(socket.closeCalled).toBe(true);
  });

  it("reports connected only after the WebSocket opens", async () => {
    const socket = new FakeSocket();
    let connected = 0;
    const client = new AircraftClient({
      store: new AircraftStore(),
      wsUrl: "ws://example.test/aircraft",
      fetchSnapshot: async () => snapshot([]),
      openSocket: () => socket,
      onConnected: () => {
        connected += 1;
      },
    });

    await client.start();
    expect(connected).toBe(0);
    socket.open();
    expect(connected).toBe(1);
  });
});
