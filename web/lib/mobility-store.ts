import type {
  MobilityCell,
  MobilityCellRemove,
  MobilityCellUpsert,
  MobilityHeartbeat,
  MobilitySnapshot,
  MobilityStreamMessage,
} from "./types-mobility";
import type { SourceStatus } from "./contracts";

type MobilityStoreState = {
  generated_at: string | null;
  source_status: SourceStatus | null;
  last_heartbeat_at: string | null;
  cells: Map<string, MobilityCell>;
};

const cloneState = (state: MobilityStoreState): MobilityStoreState => ({
  generated_at: state.generated_at,
  source_status: state.source_status,
  last_heartbeat_at: state.last_heartbeat_at,
  cells: new Map(state.cells),
});

export class MobilityStore {
  private state: MobilityStoreState = {
    generated_at: null,
    source_status: null,
    last_heartbeat_at: null,
    cells: new Map(),
  };

  private listeners = new Set<() => void>();

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): MobilityStoreState => this.state;

  ids(): string[] {
    return [...this.state.cells.keys()].sort();
  }

  get(cellId: string): MobilityCell | undefined {
    return this.state.cells.get(cellId);
  }

  replaceSnapshot(snapshot: MobilitySnapshot): void {
    const next = cloneState(this.state);
    next.generated_at = snapshot.generated_at;
    next.source_status = snapshot.source_status;
    next.cells = new Map(snapshot.cells.map((cell) => [cell.density.cell_id, cell]));
    this.state = next;
    this.emit();
  }

  apply(message: MobilityStreamMessage): void {
    switch (message.message_type) {
      case "mobility.cell.upsert":
        this.applyUpsert(message);
        return;
      case "mobility.cell.remove":
        this.applyRemove(message);
        return;
      case "mobility.heartbeat":
        this.applyHeartbeat(message);
        return;
      case "mobility.stream.ready":
        return;
      default: {
        const exhaustive: never = message;
        throw new Error(`Unsupported mobility stream message ${(exhaustive as { message_type: string }).message_type}`);
      }
    }
  }

  private applyUpsert(message: MobilityCellUpsert): void {
    const next = cloneState(this.state);
    for (const cell of message.cells) {
      const current = next.cells.get(cell.density.cell_id);
      if (!current) {
        next.cells.set(cell.density.cell_id, cell);
        continue;
      }
      const existingWindow = Date.parse(current.density.window_ended_at);
      const incomingWindow = Date.parse(cell.density.window_ended_at);
      if (incomingWindow >= existingWindow) {
        next.cells.set(cell.density.cell_id, cell);
      }
    }
    this.state = next;
    this.emit();
  }

  private applyRemove(message: MobilityCellRemove): void {
    const next = cloneState(this.state);
    for (const cellId of message.cell_ids) {
      next.cells.delete(cellId);
    }
    this.state = next;
    this.emit();
  }

  private applyHeartbeat(message: MobilityHeartbeat): void {
    const next = cloneState(this.state);
    next.last_heartbeat_at = message.sent_at;
    next.source_status = message.source_status;
    this.state = next;
    this.emit();
  }

  private emit(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

export type { MobilityStoreState };
