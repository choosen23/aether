import { describe, expect, it } from "vitest";

import {
  mobilityLayerVisibility,
  mobilityRenderSignature,
  toFeatureCollection,
} from "./maplibre-map";
import type { ProjectedAircraft } from "./contracts";
import type { MobilityCell } from "./types-mobility";

const aircraft: ProjectedAircraft = {
  icao24: "3c6444",
  callsign: "DLH4YA",
  origin_country: "Germany",
  position: { latitude: 50.04, longitude: 8.56, barometric_altitude_m: 10668, on_ground: false },
  motion: { ground_speed_mps: 232.1, track_degrees: 271.4, vertical_rate_mps: -1.2 },
  last_contact_at: "2026-09-24T09:10:00Z",
  position_source: "ads_b",
  event_id: "event-110",
  observed_at: "2026-09-24T09:10:00Z",
};

const mobilityCell: MobilityCell = {
  density: {
    event_id: "d1",
    event_type: "mobility.density",
    schema_version: 1,
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
    provenance: "derived",
  },
  demand: {
    event_id: "r1",
    event_type: "demand.region",
    schema_version: 1,
    generated_at: "2026-09-24T09:10:00Z",
    window_started_at: "2026-09-24T09:07:00Z",
    window_ended_at: "2026-09-24T09:10:00Z",
    cell_id: "841f1d7ffffffff",
    h3_resolution: 4,
    aircraft_count: 1,
    demand_units: 1,
    model_version: "density-linear-v1",
    coefficient_set: "default-v1",
    provenance: "modelled",
  },
};

describe("toFeatureCollection", () => {
  it("creates directional point features and marks stale observations", () => {
    const collection = toFeatureCollection(
      [aircraft],
      Date.parse("2026-09-24T09:10:31Z"),
      30_000,
    );

    expect(collection.features).toEqual([
      {
        type: "Feature",
        id: "3c6444",
        geometry: { type: "Point", coordinates: [8.56, 50.04] },
        properties: {
          icao24: "3c6444",
          callsign: "DLH4YA",
          track_degrees: 271.4,
          stale: true,
        },
      },
    ]);
  });

  it("excludes aircraft without coordinates", () => {
    const collection = toFeatureCollection([{ ...aircraft, position: null }], Date.now(), 30_000);
    expect(collection.features).toEqual([]);
  });
});

describe("mobility map state", () => {
  it("invalidates rendered cells when their source watermark becomes stale", () => {
    const live = mobilityRenderSignature(
      [mobilityCell],
      "density",
      Date.parse("2026-09-24T09:11:14Z"),
    );
    const stale = mobilityRenderSignature(
      [mobilityCell],
      "density",
      Date.parse("2026-09-24T09:11:16Z"),
    );

    expect(stale).not.toBe(live);
  });

  it("hides every mobility layer in aircraft-only mode", () => {
    expect(mobilityLayerVisibility("aircraft")).toEqual({
      density: "none",
      demand: "none",
      outline: "none",
    });
  });
});
