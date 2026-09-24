import { describe, expect, it } from "vitest";

import { MobilityStore } from "./mobility-store";

const CELL = "841f1d7ffffffff";
const WINDOW_1 = "2026-09-24T09:10:00Z";
const WINDOW_2 = "2026-09-24T09:10:10Z";

const snapshot = (window = WINDOW_1) => ({
  generated_at: "2026-09-24T09:10:00Z",
  source_status: {
    source: "opensky" as const,
    status: "live" as const,
    last_observation_at: "2026-09-24T09:10:00Z",
    age_seconds: 0,
  },
  cells: [
    {
      density: {
        event_id: "d1",
        event_type: "mobility.density" as const,
        schema_version: 1 as const,
        generated_at: window,
        window_started_at: window,
        window_ended_at: window,
        source_event_watermark: window,
        cell_id: CELL,
        h3_resolution: 4,
        aircraft_count: 1,
        airborne_count: 1,
        on_ground_count: 0,
        average_altitude_m: 1000,
        average_ground_speed_mps: 200,
        provenance: "derived" as const,
      },
      demand: {
        event_id: "x1",
        event_type: "demand.region" as const,
        schema_version: 1 as const,
        generated_at: window,
        window_started_at: window,
        window_ended_at: window,
        cell_id: CELL,
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

describe("MobilityStore", () => {
  it("does not regress a cell with an older aggregate window", () => {
    const store = new MobilityStore();
    store.replaceSnapshot(snapshot(WINDOW_2));
    store.apply({
      message_type: "mobility.cell.upsert",
      schema_version: 1,
      sent_at: WINDOW_2,
      cells: snapshot(WINDOW_1).cells,
    });

    expect(store.get(CELL)?.density.window_ended_at).toBe(WINDOW_2);
  });
});
