"use client";

import { useEffect, useMemo, useState } from "react";
import type { JSX } from "react";

import { AircraftDetail } from "./aircraft-detail";
import { AirspaceMap, type MapFactory } from "./airspace-map";
import { LayerControl } from "./layer-control";
import { MobilityCellDetail } from "./mobility-cell-detail";
import { SourceHealth } from "./source-health";
import type { ProjectedAircraft } from "../lib/contracts";
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
};

export function OperationsShell({ mapFactory, client }: OperationsShellProps): JSX.Element {
  const [mapError, setMapError] = useState<string | null>(null);
  const [selectedIcao24, setSelectedIcao24] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  const [mobilityMode, setMobilityMode] = useState<MobilityLayerMode>("aircraft");
  const [selectedCellId, setSelectedCellId] = useState<string | null>(null);

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
        <div className="topbar-metrics">
          <p><strong data-testid="aircraft-count">{snapshot.aircraft.length}</strong><span>aircraft tracked</span></p>
          <p><strong>{snapshot.sourceStatus}</strong><span>source state</span></p>
        </div>
      </header>

      <div id="operations-main" className="operations-main">
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
      </div>
    </main>
  );
}

export type { OperationsClient, OperationsSnapshot };
