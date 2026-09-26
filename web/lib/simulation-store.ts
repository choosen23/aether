import type {
  RunListItem,
  RunSnapshot,
  SimulationHeartbeat,
  SimulationSnapshotResponse,
  SimulationStoreSnapshot,
  SimulationStreamMessage,
  SimulationRunUpdated,
} from "./simulation-contracts";

type SimulationStoreState = SimulationStoreSnapshot;

const byRunId = (runs: readonly RunListItem[]): Map<string, RunListItem> =>
  new Map(runs.map((run) => [run.run_id, run]));

const toSortedRuns = (runs: Map<string, RunListItem>): RunListItem[] =>
  [...runs.values()].sort((left, right) => left.run_id.localeCompare(right.run_id));

export class SimulationStore {
  private state: SimulationStoreState = {
    runs: [],
    run: null,
    streamConnected: false,
    lastHeartbeatAt: null,
  };

  private listeners = new Set<() => void>();

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): SimulationStoreState => this.state;

  replaceSnapshot(snapshot: SimulationSnapshotResponse): void {
    this.state = {
      runs: snapshot.runs,
      run: snapshot.run,
      streamConnected: this.state.streamConnected,
      lastHeartbeatAt: this.state.lastHeartbeatAt,
    };
    this.emit();
  }

  setStreamConnected(connected: boolean): void {
    this.state = {
      ...this.state,
      streamConnected: connected,
    };
    this.emit();
  }

  apply(message: SimulationStreamMessage): void {
    switch (message.message_type) {
      case "simulation.run.updated":
        this.applyRunUpdated(message);
        return;
      case "simulation.heartbeat":
        this.applyHeartbeat(message);
        return;
      case "simulation.stream.ready":
        return;
      default: {
        const exhaustive: never = message;
        throw new Error(`Unsupported simulation stream message ${(exhaustive as { message_type: string }).message_type}`);
      }
    }
  }

  private applyRunUpdated(message: SimulationRunUpdated): void {
    const currentRun = this.state.run;
    if (currentRun && currentRun.run_id === message.run.run_id && message.run.sequence < currentRun.sequence) {
      return;
    }

    const nextRuns = byRunId(this.state.runs);
    const existing = nextRuns.get(message.run.run_id);
    if (!existing || message.run.sequence >= existing.sequence) {
      nextRuns.set(message.run.run_id, {
        run_id: message.run.run_id,
        scenario_id: message.run.scenario_id,
        source_trace: message.run.source_trace,
        scheduler_policy: message.run.scheduler_policy,
        status: message.run.status,
        sequence: message.run.sequence,
      });
    }

    this.state = {
      ...this.state,
      runs: toSortedRuns(nextRuns),
      run: this.state.run?.run_id === message.run.run_id ? this.pickNewest(this.state.run, message.run) : message.run,
    };
    this.emit();
  }

  private applyHeartbeat(message: SimulationHeartbeat): void {
    this.state = {
      ...this.state,
      lastHeartbeatAt: message.sent_at,
    };
    this.emit();
  }

  private pickNewest(current: RunSnapshot, incoming: RunSnapshot): RunSnapshot {
    return incoming.sequence >= current.sequence ? incoming : current;
  }

  private emit(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }
}
