"use client";

import { useEffect, useMemo, useState } from "react";
import type { JSX } from "react";

import { AircraftDetail } from "./aircraft-detail";
import { AirspaceMap, type MapFactory } from "./airspace-map";
import { ExperimentWorkspace } from "./experiment-workspace";
import { LayerControl } from "./layer-control";
import { MobilityCellDetail } from "./mobility-cell-detail";
import { SourceHealth } from "./source-health";
import type { ProjectedAircraft } from "../lib/contracts";
import type { SimulationClient } from "../lib/simulation-client";
import type { MobilityCell, MobilityLayerMode } from "../lib/types-mobility";

type OperationsSnapshot = {
  sourceStatus: "live" | "stale" | "offline";
  streamConnected: boolean;
  aircraftStreamConnected: boolean;
  mobilityStreamConnected: boolean;
  aircraft: ProjectedAircraft[];
  mobilityCells: MobilityCell[];
};

type OperationsClient = {
  start: () => void | Promise<void>;
  stop: () => void;
  subscribe: (listener: () => void) => () => void;
  getSnapshot: () => OperationsSnapshot;
  getAircraft: (now: number) => ProjectedAircraft[];
};

type OperationsShellProps = {
  mapFactory: MapFactory;
  client: OperationsClient;
  experimentClient?: SimulationClient;
  experimentMapFactory?: () => { destroy: () => void };
};

type WorkspaceMode = "operations" | "experiments";

export function OperationsShell({
  mapFactory,
  client,
  experimentClient,
  experimentMapFactory,
}: OperationsShellProps): JSX.Element {
  const [mapError, setMapError] = useState<string | null>(null);
  const [selectedIcao24, setSelectedIcao24] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [mobilityMode, setMobilityMode] = useState<MobilityLayerMode>("aircraft");
  const [selectedCellId, setSelectedCellId] = useState<string | null>(null);
  const [workspaceMode, setWorkspaceMode] = useState<WorkspaceMode>("operations");

  useEffect(() => {
    void client.start();
    const unsubscribe = client.subscribe(() => {
      setVersion((value) => value + 1);
    });
    return () => {
      unsubscribe();
      client.stop();
    };
  }, [client]);

  const snapshot = client.getSnapshot();
  const selectedAircraft = useMemo(
    () => snapshot.aircraft.find((item) => item.icao24 === selectedIcao24) ?? null,
    [selectedIcao24, snapshot.aircraft, version],
  );
  const selectedMobilityCell = useMemo(
    () => snapshot.mobilityCells.find((item) => item.density.cell_id === selectedCellId) ?? null,
    [selectedCellId, snapshot.mobilityCells, version],
  );

  return (
    <main className="operations-shell">
      <a className="skip-nav" href="#operations-main">
        Skip to operations
      </a>
      <header className="topbar">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">Æ</span>
          <div><h1>Aether</h1><p>European airspace twin</p></div>
        </div>
        <nav className="workspace-nav" aria-label="Workspace mode">
          <button type="button" onClick={() => setWorkspaceMode("operations")} aria-pressed={workspaceMode === "operations"}>
            Live operations
          </button>
          {experimentClient && experimentMapFactory ? (
            <button type="button" onClick={() => setWorkspaceMode("experiments")} aria-pressed={workspaceMode === "experiments"}>
              Experiments
            </button>
          ) : null}
        </nav>
        <div className="topbar-metrics">
          <p><strong data-testid="aircraft-count">{snapshot.aircraft.length}</strong><span>aircraft tracked</span></p>
          <p><strong>{snapshot.sourceStatus}</strong><span>source state</span></p>
        </div>
      </header>

      <div id="operations-main" className="operations-main">
        {workspaceMode === "experiments" && experimentClient && experimentMapFactory ? (
          <ExperimentWorkspace client={experimentClient} mapFactory={experimentMapFactory} />
        ) : null}
        {workspaceMode === "operations" ? (
          <>
        {mapError ? <div className="map-alert" role="alert">Map unavailable: {mapError}</div> : null}
        <AirspaceMap
          mapFactory={mapFactory}
          getAircraft={(now) => client.getAircraft(now)}
          getMobilityCells={() => snapshot.mobilityCells}
          mobilityMode={mobilityMode}
          onError={(error) => setMapError(error.message)}
          onSelect={(icao24) => setSelectedIcao24(icao24)}
          onSelectCell={(cellId) => setSelectedCellId(cellId)}
        />

        <LayerControl mode={mobilityMode} onChange={setMobilityMode} />
        <AircraftDetail aircraft={selectedAircraft} />
        <MobilityCellDetail cell={selectedMobilityCell} />
        <SourceHealth
          providerName="OpenSky"
          sourceStatus={snapshot.sourceStatus}
          streamConnected={snapshot.streamConnected}
        />
        <div className="map-legend" aria-label="Map legend">
          <span><i className="legend-live" />Live track</span>
          <span><i className="legend-stale" />Stale track</span>
          {mobilityMode === "demand" ? (
            <span>Modelled demand—not measured infrastructure load.</span>
          ) : null}
          <span>Derived motion between OpenSky observations</span>
        </div>
          </>
        ) : null}
      </div>
    </main>
  );
}

export type { OperationsClient, OperationsSnapshot };
