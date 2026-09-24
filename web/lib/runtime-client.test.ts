import { expect, it } from "vitest";

import { AircraftStore } from "./aircraft-store";
import type { AircraftSnapshot, AircraftUpsert, ProjectedAircraft } from "./contracts";
import { interpolateStore } from "./runtime-client";

const aircraft = (observedAt: string, longitude: number): ProjectedAircraft => ({
  icao24: "3c6444",
  callsign: "DLH4YA",
  origin_country: "Germany",
  position: { latitude: 50, longitude, barometric_altitude_m: 10000, on_ground: false },
  motion: { ground_speed_mps: 220, track_degrees: 90, vertical_rate_mps: 0 },
  last_contact_at: observedAt,
  position_source: "ads_b",
  event_id: observedAt,
  observed_at: observedAt,
});

it("renders with one observation interval of delay for continuous movement", () => {
  const store = new AircraftStore();
  const initial: AircraftSnapshot = {
    generated_at: "2026-09-24T09:10:00Z",
    source_status: {
      source: "opensky",
      status: "live",
      last_observation_at: "2026-09-24T09:10:00Z",
      age_seconds: 0,
    },
    aircraft: [aircraft("2026-09-24T09:10:00Z", 8)],
  };
  const update: AircraftUpsert = {
    message_type: "aircraft.upsert",
    schema_version: 1,
    sent_at: "2026-09-24T09:10:10Z",
    aircraft: [aircraft("2026-09-24T09:10:10Z", 10)],
  };
  store.replaceSnapshot(initial);
  store.apply(update);

  const rendered = interpolateStore(store, Date.parse("2026-09-24T09:10:15Z"), 10_000);

  expect(rendered[0].position?.longitude).toBeCloseTo(9);
});
