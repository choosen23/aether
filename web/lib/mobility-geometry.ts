import type { Feature, FeatureCollection, Polygon } from "geojson";
import { cellToBoundary, isValidCell } from "h3-js";

import type { MobilityCell, MobilityLayerMode } from "./types-mobility";

type MobilityProperties = {
  cell_id: string;
  aircraft_count: number;
  demand_units: number;
  stale: boolean;
};

const normalizeLongitude = (longitude: number): number => {
  let value = longitude;
  while (value > 180) value -= 360;
  while (value < -180) value += 360;
  return value;
};

function toClosedRing(cellId: string): [number, number][] | null {
  if (!isValidCell(cellId)) {
    return null;
  }
  const boundary = cellToBoundary(cellId, true) as Array<[number, number]>;
  if (boundary.length < 3) {
    return null;
  }
  const ring: [number, number][] = boundary.map(([lon, lat]) => [normalizeLongitude(lon), lat]);
  for (let index = 1; index < ring.length; index += 1) {
    const [prevLon] = ring[index - 1];
    const [currentLon] = ring[index];
    if (Math.abs(currentLon - prevLon) > 180) {
      return null;
    }
  }
  ring.push([...ring[0]] as [number, number]);
  return ring;
}

export function toMobilityFeatureCollection(
  cells: MobilityCell[],
  _mode: MobilityLayerMode,
  now: number,
): FeatureCollection<Polygon, MobilityProperties> {
  const features: Array<Feature<Polygon, MobilityProperties>> = [];
  for (const cell of cells) {
    const ring = toClosedRing(cell.density.cell_id);
    if (!ring) {
      continue;
    }
    features.push({
      type: "Feature",
      id: cell.density.cell_id,
      geometry: {
        type: "Polygon",
        coordinates: [ring],
      },
      properties: {
        cell_id: cell.density.cell_id,
        aircraft_count: cell.density.aircraft_count,
        demand_units: cell.demand.demand_units,
        stale: now - Date.parse(cell.density.source_event_watermark) > 75_000,
      },
    });
  }
  return { type: "FeatureCollection", features };
}
