import type {
  MobilityCell,
  MobilityCellRemove,
  MobilityCellUpsert,
  MobilityHeartbeat,
  MobilitySnapshot,
  MobilityStreamMessage,
  MobilityStreamReady,
} from "./types-mobility";

export type {
  MobilityCell,
  MobilityCellRemove,
  MobilityCellUpsert,
  MobilityHeartbeat,
  MobilitySnapshot,
  MobilityStreamMessage,
  MobilityStreamReady,
};

export class MobilityContractError extends Error {}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object";

export function assertMobilityStreamMessage(value: unknown): MobilityStreamMessage {
  if (!isRecord(value) || typeof value.message_type !== "string") {
    throw new MobilityContractError("mobility stream message has no message_type");
  }
  switch (value.message_type) {
    case "mobility.stream.ready":
    case "mobility.cell.upsert":
    case "mobility.cell.remove":
    case "mobility.heartbeat":
      return value as MobilityStreamMessage;
    default:
      throw new MobilityContractError("unsupported mobility stream message type");
  }
}

export function parseMobilitySnapshot(value: unknown): MobilitySnapshot {
  if (!isRecord(value)) {
    throw new MobilityContractError("snapshot is not an object");
  }
  if (!Array.isArray(value.cells)) {
    throw new MobilityContractError("snapshot has no cells array");
  }
  return value as MobilitySnapshot;
}
