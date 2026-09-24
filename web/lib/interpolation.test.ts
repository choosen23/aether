import { describe, expect, it } from "vitest";

import type { AircraftTrack } from "./aircraft-store";
import type { ProjectedAircraft } from "./contracts";
import { interpolateAircraft } from "./interpolation";

const at = (
  longitude: number,
  heading: number,
  observedAt: string,
): ProjectedAircraft => ({
  icao24: "3c6444",
  callsign: "DLH4YA",
  origin_country: "Germany",
  position: {
    latitude: 50,
    longitude,
    barometric_altitude_m: 10000,
    on_ground: false,
  },
  motion: {
    ground_speed_mps: 220,
    track_degrees: heading,
    vertical_rate_mps: 0,
  },
  last_contact_at: observedAt,
  position_source: "ads_b",
  event_id: `event-${observedAt}`,
  observed_at: observedAt,
});

const track = (previous: ProjectedAircraft, current: ProjectedAircraft): AircraftTrack => ({
  previous,
  current,
});

describe("interpolateAircraft", () => {
  it("uses shortest paths across longitude and heading wraparound", () => {
    const start = "2026-09-24T09:10:00.000Z";
    const end = "2026-09-24T09:10:10.000Z";
    const midpoint = Date.parse("2026-09-24T09:10:05.000Z");

    const point = interpolateAircraft(track(at(179, 359, start), at(-179, 1, end)), midpoint, 10_000);
    expect(Math.abs(Math.abs(point.position!.longitude) - 180)).toBeLessThan(0.01);
    expect(point.motion!.track_degrees).toBeCloseTo(0);
  });

  it("freezes at the newest observation after the horizon", () => {
    const start = "2026-09-24T09:10:00.000Z";
    const end = "2026-09-24T09:10:10.000Z";
    const current = at(-179, 1, end);
    const afterHorizon = Date.parse("2026-09-24T09:10:21.000Z");

    expect(interpolateAircraft(track(at(179, 359, start), current), afterHorizon, 10_000)).toEqual(
      current,
    );
  });
});
