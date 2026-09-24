import type {
  AircraftRemove,
  AircraftSnapshot,
  AircraftUpsert,
  Heartbeat,
  ProjectedAircraft,
  SourceStatus,
  StreamMessage,
} from "./contracts";

type AircraftTrack = {
  previous: ProjectedAircraft | null;
  current: ProjectedAircraft;
};

type StoreState = {
  generated_at: string | null;
  source_status: SourceStatus | null;
  tracks: Map<string, AircraftTrack>;
};

const cloneState = (state: StoreState): StoreState => ({
  generated_at: state.generated_at,
  source_status: state.source_status,
  tracks: new Map(state.tracks),
});

export class AircraftStore {
  private state: StoreState = {
    generated_at: null,
    source_status: null,
    tracks: new Map(),
  };

  private listeners = new Set<() => void>();

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): StoreState => this.state;

  ids(): string[] {
    return [...this.state.tracks.keys()].sort();
  }

  getTrack(icao24: string): AircraftTrack | undefined {
    return this.state.tracks.get(icao24);
  }

  replaceSnapshot(snapshot: AircraftSnapshot): void {
    const next = cloneState(this.state);
    next.generated_at = snapshot.generated_at;
    next.source_status = snapshot.source_status;
    next.tracks = new Map(
      snapshot.aircraft.map((aircraft) => [
        aircraft.icao24,
        {
          previous: null,
          current: aircraft,
        },
      ]),
    );
    this.state = next;
    this.emit();
  }

  apply(message: StreamMessage): void {
    switch (message.message_type) {
      case "aircraft.upsert":
        this.applyUpsert(message);
        return;
      case "aircraft.remove":
        this.applyRemove(message);
        return;
      case "heartbeat":
        this.applyHeartbeat(message);
        return;
      case "stream.ready":
        return;
      default: {
        const exhaustive: never = message;
        throw new Error(`Unsupported stream message ${(exhaustive as { message_type: string }).message_type}`);
      }
    }
  }

  private applyUpsert(message: AircraftUpsert): void {
    const next = cloneState(this.state);
    for (const aircraft of message.aircraft) {
      const existing = next.tracks.get(aircraft.icao24);
      if (!existing) {
        next.tracks.set(aircraft.icao24, { previous: null, current: aircraft });
        continue;
      }

      next.tracks.set(aircraft.icao24, {
        previous: existing.current,
        current: aircraft,
      });
    }
    this.state = next;
    this.emit();
  }

  private applyRemove(message: AircraftRemove): void {
    const next = cloneState(this.state);
    for (const icao24 of message.icao24) {
      next.tracks.delete(icao24);
    }
    this.state = next;
    this.emit();
  }

  private applyHeartbeat(message: Heartbeat): void {
    const next = cloneState(this.state);
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

export type { AircraftTrack, StoreState };
