export type SimulationRunStatus = "pending" | "running" | "paused" | "completed" | "cancelled" | "failed";

export type RunListItem = {
  run_id: string;
  scenario_id?: string;
  source_trace?: "azure_functions" | "google_borg";
  scheduler_policy: string;
  status: SimulationRunStatus;
  sequence: number;
  summary?: {
    component_metrics?: Record<string, number>;
    combined_score?: number | null;
  };
};

export type RunSnapshot = RunListItem & {
  provenance_notes?: string[];
  summary?: {
    component_metrics?: Record<string, number>;
    combined_score?: number | null;
  };
};

export type SimulationStoreSnapshot = {
  runs: readonly RunListItem[];
  run: RunSnapshot | null;
  streamConnected: boolean;
  lastHeartbeatAt: string | null;
};

export type SimulationSnapshotResponse = {
  runs: RunListItem[];
  run: RunSnapshot | null;
};

export type SimulationStreamReady = {
  message_type: "simulation.stream.ready";
  schema_version: 1;
  sent_at: string;
};

export type SimulationRunUpdated = {
  message_type: "simulation.run.updated";
  schema_version: 1;
  sent_at: string;
  run: RunSnapshot;
};

export type SimulationHeartbeat = {
  message_type: "simulation.heartbeat";
  schema_version: 1;
  sent_at: string;
};

export type SimulationStreamMessage = SimulationStreamReady | SimulationRunUpdated | SimulationHeartbeat;

export class SimulationContractError extends Error {}

const isRecord = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object";

export function assertSimulationStreamMessage(value: unknown): SimulationStreamMessage {
  if (!isRecord(value) || typeof value.message_type !== "string") {
    throw new SimulationContractError("simulation stream message has no message_type");
  }

  switch (value.message_type) {
    case "simulation.stream.ready":
    case "simulation.run.updated":
    case "simulation.heartbeat":
      return value as SimulationStreamMessage;
    default:
      throw new SimulationContractError("unsupported simulation stream message type");
  }
}

export function parseSimulationSnapshot(value: unknown): SimulationSnapshotResponse {
  if (!isRecord(value) || !Array.isArray(value.runs)) {
    throw new SimulationContractError("simulation snapshot has no runs array");
  }
  return value as SimulationSnapshotResponse;
}
