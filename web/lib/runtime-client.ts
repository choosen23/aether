import { AircraftClient } from "./aircraft-client";
import { AircraftStore } from "./aircraft-store";
import { MobilityClient } from "./mobility-client";
import { MobilityStore } from "./mobility-store";
import type { AircraftSnapshot, ProjectedAircraft } from "./contracts";
import type { MobilitySnapshot } from "./mobility-contracts";
import { interpolateAircraft } from "./interpolation";

import type { OperationsClient, OperationsSnapshot } from "../components/operations-shell";

export function interpolateStore(
  store: AircraftStore,
  now: number,
  observationIntervalMs: number,
): ProjectedAircraft[] {
  const renderAt = now - observationIntervalMs;
  return store
    .ids()
    .map((icao24) => store.getTrack(icao24))
    .filter((track): track is NonNullable<typeof track> => track !== undefined)
    .map((track) => interpolateAircraft(track, renderAt, observationIntervalMs));
}

export function createRuntimeOperationsClient(): OperationsClient {
  const store = new AircraftStore();
  const mobilityStore = new MobilityStore();
  let aircraftStreamConnected = false;
  let mobilityStreamConnected = false;
  const listeners = new Set<() => void>();

  const emit = () => {
    for (const listener of listeners) {
      listener();
    }
  };

  const apiHttpUrl = process.env.NEXT_PUBLIC_API_HTTP_URL ?? "http://localhost:8000";
  const apiWsUrl = process.env.NEXT_PUBLIC_API_WS_URL ?? "ws://localhost:8000";

  const fetchSnapshot = async (signal: AbortSignal): Promise<AircraftSnapshot> => {
    const response = await fetch(`${apiHttpUrl}/api/v1/aircraft`, { signal });
    if (!response.ok) {
      throw new Error(`snapshot fetch failed with status ${response.status}`);
    }
    return (await response.json()) as AircraftSnapshot;
  };

  const fetchMobilitySnapshot = async (signal: AbortSignal): Promise<MobilitySnapshot> => {
    const response = await fetch(`${apiHttpUrl}/api/v1/mobility/cells`, { signal });
    if (!response.ok) {
      throw new Error(`mobility snapshot fetch failed with status ${response.status}`);
    }
    return (await response.json()) as MobilitySnapshot;
  };

  const wsUrl = `${apiWsUrl}/api/v1/stream/aircraft`;
  const mobilityWsUrl = `${apiWsUrl}/api/v1/stream/mobility`;

  const client = new AircraftClient({
    store,
    wsUrl,
    fetchSnapshot,
    openSocket: (url) => new WebSocket(url),
    onConnected: () => {
      aircraftStreamConnected = true;
      emit();
    },
    onDisconnected: () => {
      aircraftStreamConnected = false;
      emit();
    },
  });

  const mobilityClient = new MobilityClient({
    store: mobilityStore,
    wsUrl: mobilityWsUrl,
    fetchSnapshot: fetchMobilitySnapshot,
    openSocket: (url) => new WebSocket(url),
    onConnected: () => {
      mobilityStreamConnected = true;
      emit();
    },
    onDisconnected: () => {
      mobilityStreamConnected = false;
      emit();
    },
  });

  return {
    start: async () => {
      await client.start();
      await mobilityClient.start();
      emit();
    },
    stop: () => {
      client.stop();
      mobilityClient.stop();
      emit();
    },
    subscribe: (listener) => {
      listeners.add(listener);
      const unsubscribeStore = store.subscribe(listener);
      const unsubscribeMobilityStore = mobilityStore.subscribe(listener);
      return () => {
        listeners.delete(listener);
        unsubscribeStore();
        unsubscribeMobilityStore();
      };
    },
    getSnapshot: (): OperationsSnapshot => {
      const snapshot = store.getSnapshot();
      const mobilitySnapshot = mobilityStore.getSnapshot();
      return {
        sourceStatus: snapshot.source_status?.status ?? "offline",
        streamConnected: aircraftStreamConnected && mobilityStreamConnected,
        aircraftStreamConnected,
        mobilityStreamConnected,
        aircraft: snapshot.generated_at
          ? store
              .ids()
              .map((icao24) => store.getTrack(icao24)?.current)
              .filter((item): item is NonNullable<typeof item> => item !== undefined)
          : [],
        mobilityCells: mobilitySnapshot.generated_at
          ? mobilityStore
              .ids()
              .map((cellId) => mobilityStore.get(cellId))
              .filter((item): item is NonNullable<typeof item> => item !== undefined)
          : [],
      };
    },
    getAircraft: (now): ProjectedAircraft[] => interpolateStore(store, now, 10_000),
  };
}
