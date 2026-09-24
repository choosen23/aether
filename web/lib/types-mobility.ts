import type { SourceStatus } from "./contracts";

export type MobilityDensityEvent = {
  event_id: string;
  event_type: "mobility.density";
  schema_version: 1;
  generated_at: string;
  window_started_at: string;
  window_ended_at: string;
  source_event_watermark: string;
  cell_id: string;
  h3_resolution: number;
  aircraft_count: number;
  airborne_count: number;
  on_ground_count: number;
  average_altitude_m: number | null;
  average_ground_speed_mps: number | null;
  provenance: "derived";
};

export type RegionalDemandEvent = {
  event_id: string;
  event_type: "demand.region";
  schema_version: 1;
  generated_at: string;
  window_started_at: string;
  window_ended_at: string;
  cell_id: string;
  h3_resolution: number;
  aircraft_count: number;
  demand_units: number;
  model_version: "density-linear-v1";
  coefficient_set: "default-v1";
  provenance: "modelled";
};

export type MobilityCell = {
  density: MobilityDensityEvent;
  demand: RegionalDemandEvent;
};

export type MobilitySnapshot = {
  generated_at: string;
  source_status: SourceStatus;
  cells: MobilityCell[];
};

export type MobilityStreamReady = {
  message_type: "mobility.stream.ready";
  schema_version: 1;
  sent_at: string;
};

export type MobilityCellUpsert = {
  message_type: "mobility.cell.upsert";
  schema_version: 1;
  sent_at: string;
  cells: MobilityCell[];
};

export type MobilityCellRemove = {
  message_type: "mobility.cell.remove";
  schema_version: 1;
  sent_at: string;
  cell_ids: string[];
};

export type MobilityHeartbeat = {
  message_type: "mobility.heartbeat";
  schema_version: 1;
  sent_at: string;
  source_status: SourceStatus;
};

export type MobilityStreamMessage =
  | MobilityStreamReady
  | MobilityCellUpsert
  | MobilityCellRemove
  | MobilityHeartbeat;

export type MobilityLayerMode = "aircraft" | "density" | "demand";
