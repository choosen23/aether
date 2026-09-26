import { render, screen } from "@testing-library/react";
import { fireEvent } from "@testing-library/react";
import { expect, it } from "vitest";

import type { MapFactory } from "./airspace-map";
import { OperationsShell, type OperationsClient, type OperationsSnapshot } from "./operations-shell";
import type { ProjectedAircraft } from "../lib/contracts";
import type { SimulationClient } from "../lib/simulation-client";
import type { SimulationStoreSnapshot } from "../lib/simulation-contracts";

const makeAircraft = (icao24: string, callsign: string): ProjectedAircraft => ({
  icao24,
  callsign,
  origin_country: "Germany",
  position: { latitude: 50, longitude: 8, barometric_altitude_m: 10000, on_ground: false },
  motion: { ground_speed_mps: 220, track_degrees: 90, vertical_rate_mps: 0 },
  last_contact_at: "2026-09-24T09:10:00Z",
  position_source: "ads_b",
  event_id: `event-${icao24}`,
  observed_at: "2026-09-24T09:10:00Z",
});

class FakeClient implements OperationsClient {
  private snapshot: OperationsSnapshot;
  private listener: (() => void) | null = null;

  constructor(snapshot: OperationsSnapshot) {
    this.snapshot = snapshot;
  }

  start(): void {}

  stop(): void {}

  subscribe(listener: () => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }

  getSnapshot(): OperationsSnapshot {
    return this.snapshot;
  }

  getAircraft(): ProjectedAircraft[] {
    return this.snapshot.aircraft;
  }

  emit(): void {
    this.listener?.();
  }
}

class FakeSimulationClient implements SimulationClient {
  start(): void {}

  stop(): void {}

  subscribe(): () => void {
    return () => undefined;
  }

  getSnapshot(): SimulationStoreSnapshot {
    return {
      runs: [],
      run: {
        run_id: "run-1",
        scenario_id: "azure-functions-eu-small",
        source_trace: "azure_functions",
        scheduler_policy: "balanced",
        status: "completed",
        sequence: 4,
        provenance_notes: ["modelled origin assignment"],
      },
      streamConnected: true,
      lastHeartbeatAt: null,
    };
  }
}

const baseSnapshot: OperationsSnapshot = {
  sourceStatus: "live",
  streamConnected: true,
  aircraftStreamConnected: true,
  mobilityStreamConnected: true,
  aircraft: [makeAircraft("3c6444", "DLH4YA")],
  mobilityCells: [],
};

it("keeps telemetry status usable when the map style fails", async () => {
  const failingMapFactory: MapFactory = () => {
    throw new Error("style failed");
  };

  const fakeClient = new FakeClient(baseSnapshot);
  render(<OperationsShell mapFactory={failingMapFactory} client={fakeClient} />);

  expect(await screen.findByText(/Map unavailable:/)).toBeVisible();
  expect(screen.getByText("OpenSky")).toBeVisible();
  expect(screen.getByText("Stream connected")).toBeVisible();
});

it("opens aircraft details from a map selection", async () => {
  const selection = { current: null as ((icao24: string) => void) | null };
  const fakeMapFactory: MapFactory = ({ onSelect }) => {
    selection.current = onSelect;
    return {
      destroy: () => undefined,
    };
  };

  const fakeClient = new FakeClient(baseSnapshot);
  render(<OperationsShell mapFactory={fakeMapFactory} client={fakeClient} />);

  if (!selection.current) {
    throw new Error("map selection handler not wired");
  }

  selection.current("3c6444");
  fakeClient.emit();

  expect(await screen.findByText("DLH4YA")).toBeVisible();
});

it("shows modelled provenance when demand mode is selected", async () => {
  const fakeMapFactory: MapFactory = () => ({ destroy: () => undefined });
  const fakeClient = new FakeClient(baseSnapshot);
  render(<OperationsShell mapFactory={fakeMapFactory} client={fakeClient} />);

  fireEvent.click(screen.getAllByRole("radio", { name: "Modelled demand" })[0]);

  expect(screen.getByText("Modelled demand—not measured infrastructure load.")).toBeVisible();
});

it("can switch from live operations to experiments", async () => {
  const fakeMapFactory: MapFactory = () => ({ destroy: () => undefined });
  const fakeClient = new FakeClient(baseSnapshot);
  const fakeExperimentClient = new FakeSimulationClient();

  render(
    <OperationsShell
      mapFactory={fakeMapFactory}
      client={fakeClient}
      experimentClient={fakeExperimentClient}
      experimentMapFactory={() => ({ destroy: () => undefined })}
    />,
  );

  fireEvent.click(screen.getByRole("button", { name: "Experiments" }));

  expect(await screen.findByText("Experiment workspace")).toBeVisible();
  expect(screen.getByText("Azure Functions")).toBeVisible();
});
