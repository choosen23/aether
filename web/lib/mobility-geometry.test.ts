import { expect, it } from "vitest";

import { toMobilityFeatureCollection } from "./mobility-geometry";

const CELL = {
  density: {
    event_id: "d1",
    event_type: "mobility.density" as const,
    schema_version: 1 as const,
    generated_at: "2026-09-24T09:10:00Z",
    window_started_at: "2026-09-24T09:07:00Z",
    window_ended_at: "2026-09-24T09:10:00Z",
    source_event_watermark: "2026-09-24T09:10:00Z",
    cell_id: "841f1d7ffffffff",
    h3_resolution: 4,
    aircraft_count: 1,
    airborne_count: 1,
    on_ground_count: 0,
    average_altitude_m: 1000,
    average_ground_speed_mps: 200,
    provenance: "derived" as const,
  },
  demand: {
    event_id: "r1",
    event_type: "demand.region" as const,
    schema_version: 1 as const,
    generated_at: "2026-09-24T09:10:00Z",
    window_started_at: "2026-09-24T09:07:00Z",
    window_ended_at: "2026-09-24T09:10:00Z",
    cell_id: "841f1d7ffffffff",
    h3_resolution: 4,
    aircraft_count: 1,
    demand_units: 1,
    model_version: "density-linear-v1" as const,
    coefficient_set: "default-v1" as const,
    provenance: "modelled" as const,
  },
};

it("closes H3 polygons using GeoJSON longitude-latitude order", () => {
  const feature = toMobilityFeatureCollection([CELL], "density", Date.now()).features[0];
  const ring = feature.geometry.coordinates[0];
  expect(ring[0]).toEqual(ring.at(-1));
  expect(
    ring.every(([lon, lat]) => lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90),
  ).toBe(true);
});

it("rejects invalid rings without crashing", () => {
  expect(
    toMobilityFeatureCollection(
      [{ ...CELL, density: { ...CELL.density, cell_id: "not-h3" } }],
      "density",
      Date.now(),
    ).features,
  ).toEqual([]);
  expect(() => toMobilityFeatureCollection([CELL], "demand", Date.now())).not.toThrow();
});
