import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";

import { MobilityCellDetail } from "./mobility-cell-detail";

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
    aircraft_count: 12,
    airborne_count: 10,
    on_ground_count: 2,
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
    aircraft_count: 12,
    demand_units: 10.7,
    model_version: "density-linear-v1" as const,
    coefficient_set: "default-v1" as const,
    provenance: "modelled" as const,
  },
};

it("shows density counts and provenance for selected cell", () => {
  render(<MobilityCellDetail cell={CELL} />);
  expect(screen.getByText("12 aircraft")).toBeVisible();
  expect(screen.getByText(/Density: derived from OpenSky positions; demand: modelled\./)).toBeVisible();
});
