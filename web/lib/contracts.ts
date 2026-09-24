export type Position = {
  latitude: number;
  longitude: number;
  barometric_altitude_m: number | null;
  on_ground: boolean;
};

export type Motion = {
  ground_speed_mps: number | null;
  track_degrees: number | null;
  vertical_rate_mps: number | null;
};

export type AircraftState = {
  icao24: string;
  callsign: string | null;
  origin_country: string;
  position: Position | null;
  motion: Motion | null;
  last_contact_at: string | null;
  position_source: "ads_b" | "asterix" | "mlat" | "other" | null;
};

export type AircraftPositionEvent = {
  event_id: string;
  event_type: "aircraft.position";
  schema_version: 1;
  observed_at: string;
  ingested_at: string;
  source: "opensky";
  aircraft: AircraftState;
};

export type SourceStatus = {
  source: "opensky";
  status: "live" | "stale" | "offline";
  last_observation_at: string | null;
  age_seconds: number | null;
};

export type AircraftSnapshot = {
  generated_at: string;
  source_status: SourceStatus;
  aircraft: ProjectedAircraft[];
};

export type ProjectedAircraft = AircraftState & {
  event_id: string;
  observed_at: string;
};

export type StreamReady = {
  message_type: "stream.ready";
  schema_version: 1;
  sent_at: string;
};

export type AircraftUpsert = {
  message_type: "aircraft.upsert";
  schema_version: 1;
  sent_at: string;
  aircraft: ProjectedAircraft[];
};

export type AircraftRemove = {
  message_type: "aircraft.remove";
  schema_version: 1;
  sent_at: string;
  icao24: string[];
};

export type Heartbeat = {
  message_type: "heartbeat";
  schema_version: 1;
  sent_at: string;
  source_status: SourceStatus;
};

export type StreamMessage = StreamReady | AircraftUpsert | AircraftRemove | Heartbeat;

export class ContractError extends Error {}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object";

export function parseAircraftPositionEvent(value: unknown): AircraftPositionEvent {
  if (!isRecord(value)) {
    throw new ContractError("event is not an object");
  }
  if (value.event_type !== "aircraft.position") {
    throw new ContractError("event_type must be aircraft.position");
  }
  if (value.schema_version !== 1) {
    throw new ContractError("schema_version must be 1");
  }
  return value as AircraftPositionEvent;
}

function parseByMessageType(value: Record<string, unknown>): StreamMessage {
  switch (value.message_type) {
    case "stream.ready":
    case "aircraft.upsert":
    case "aircraft.remove":
    case "heartbeat":
      return value as StreamMessage;
    default:
      throw new ContractError("unsupported stream message type");
  }
}

export function assertStreamMessage(value: unknown): StreamMessage {
  if (!isRecord(value) || typeof value.message_type !== "string") {
    throw new ContractError("stream message has no message_type");
  }
  return parseByMessageType(value);
}
