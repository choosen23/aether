import { describe, expect, it } from "vitest";

import { AircraftStore } from "./aircraft-store";
import type { AircraftRemove, AircraftSnapshot, AircraftUpsert, ProjectedAircraft } from "./contracts";

const at = (icao24: string): ProjectedAircraft => ({
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

const snapshot = (aircraft: ProjectedAircraft[]): AircraftSnapshot => ({
  generated_at: "2026-09-24T09:10:00Z",
  source_status: {
    source: "opensky",
    status: "live",
    last_observation_at: "2026-09-24T09:10:00Z",
    age_seconds: 0,
  },
  aircraft,
});

const upsert = (aircraft: ProjectedAircraft[]): AircraftUpsert => ({
  message_type: "aircraft.upsert",
  schema_version: 1,
  sent_at: "2026-09-24T09:10:01Z",
  aircraft,
});

const remove = (icao24: string[]): AircraftRemove => ({
  message_type: "aircraft.remove",
  schema_version: 1,
  sent_at: "2026-09-24T09:10:01Z",
  icao24,
});

describe("AircraftStore", () => {
  it("replaces state from a reconnect snapshot before applying deltas", () => {
    const store = new AircraftStore();
    store.replaceSnapshot(snapshot([at("a00001"), at("b00002")]));
    store.replaceSnapshot(snapshot([at("b00002")]));
    store.apply(upsert([at("c00003")]));
    expect(store.ids()).toEqual(["b00002", "c00003"]);
  });

  it("removes explicitly expired aircraft", () => {
    const store = new AircraftStore();
    store.replaceSnapshot(snapshot([at("a00001")]));
    store.apply(remove(["a00001"]));
    expect(store.ids()).toEqual([]);
  });
});
