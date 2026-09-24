"use client";

import { useEffect, useRef } from "react";
import type { JSX } from "react";

import type { ProjectedAircraft } from "../lib/contracts";
import type { MobilityCell, MobilityLayerMode } from "../lib/types-mobility";

type MapController = {
  destroy: () => void;
};

type MapFactory = (options: {
  container: HTMLDivElement;
  getAircraft: (now: number) => ProjectedAircraft[];
  getMobilityCells: () => MobilityCell[];
  mobilityMode: () => MobilityLayerMode;
  onError: (error: Error) => void;
  onSelect: (icao24: string) => void;
  onSelectCell: (cellId: string) => void;
}) => MapController;

type AirspaceMapProps = {
  mapFactory: MapFactory;
  getAircraft: (now: number) => ProjectedAircraft[];
  getMobilityCells: () => MobilityCell[];
  mobilityMode: MobilityLayerMode;
  onSelect: (icao24: string) => void;
  onSelectCell: (cellId: string) => void;
  onError: (error: Error) => void;
};

export function AirspaceMap({
  mapFactory,
  getAircraft,
  getMobilityCells,
  mobilityMode,
  onSelect,
  onSelectCell,
  onError,
}: AirspaceMapProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const getAircraftRef = useRef(getAircraft);
  const getMobilityCellsRef = useRef(getMobilityCells);
  const mobilityModeRef = useRef(mobilityMode);
  const onErrorRef = useRef(onError);
  const onSelectRef = useRef(onSelect);
  const onSelectCellRef = useRef(onSelectCell);

  getAircraftRef.current = getAircraft;
  getMobilityCellsRef.current = getMobilityCells;
  mobilityModeRef.current = mobilityMode;
  onErrorRef.current = onError;
  onSelectRef.current = onSelect;
  onSelectCellRef.current = onSelectCell;

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    let controller: MapController | null = null;
    try {
        controller = mapFactory({
          container: containerRef.current,
          getAircraft: (now) => getAircraftRef.current(now),
          getMobilityCells: () => getMobilityCellsRef.current(),
          mobilityMode: () => mobilityModeRef.current,
          onError: (error) => onErrorRef.current(error),
          onSelect: (icao24) => onSelectRef.current(icao24),
          onSelectCell: (cellId) => onSelectCellRef.current(cellId),
        });
    } catch (error) {
      onError(error instanceof Error ? error : new Error("Map failed to initialize"));
    }

    return () => {
      controller?.destroy();
    };
  }, [mapFactory]);

  return <div aria-label="Airspace map" className="airspace-map" ref={containerRef} />;
}

export type { MapFactory };
