import type { Feature, FeatureCollection, Point } from "geojson";
import * as maplibregl from "maplibre-gl";
import type { GeoJSONSource, MapLayerMouseEvent } from "maplibre-gl";

import type { MapFactory } from "../components/airspace-map";
import type { ProjectedAircraft } from "./contracts";
import { toMobilityFeatureCollection } from "./mobility-geometry";
import type { MobilityCell, MobilityLayerMode } from "./types-mobility";

const mapLibreWorkerUrl = "/maplibre-gl-worker.mjs";

type AircraftProperties = {
  icao24: string;
  callsign: string;
  track_degrees: number;
  stale: boolean;
};

const mobilityStaleAfterMs = 75_000;

export function mobilityLayerVisibility(mode: MobilityLayerMode): {
  density: "visible" | "none";
  demand: "visible" | "none";
  outline: "visible" | "none";
} {
  return {
    density: mode === "density" ? "visible" : "none",
    demand: mode === "demand" ? "visible" : "none",
    outline: mode === "aircraft" ? "none" : "visible",
  };
}

export function mobilityRenderSignature(
  cells: MobilityCell[],
  mode: MobilityLayerMode,
  now: number,
): string {
  return `${mode}|${cells
    .map((cell) => {
      const stale =
        now - Date.parse(cell.density.source_event_watermark) > mobilityStaleAfterMs;
      return `${cell.density.cell_id}:${cell.density.window_ended_at}:${stale}`;
    })
    .sort()
    .join("|")}`;
}

export function toFeatureCollection(
  aircraft: ProjectedAircraft[],
  now: number,
  staleAfterMs: number,
): FeatureCollection<Point, AircraftProperties> {
  const features: Array<Feature<Point, AircraftProperties>> = [];
  for (const item of aircraft) {
    if (!item.position) continue;
    features.push({
      type: "Feature",
      id: item.icao24,
      geometry: {
        type: "Point",
        coordinates: [item.position.longitude, item.position.latitude],
      },
      properties: {
        icao24: item.icao24,
        callsign: item.callsign ?? item.icao24.toUpperCase(),
        track_degrees: item.motion?.track_degrees ?? 0,
        stale: now - Date.parse(item.observed_at) > staleAfterMs,
      },
    });
  }
  return { type: "FeatureCollection", features };
}

function aircraftGlyph(): ImageData {
  const canvas = document.createElement("canvas");
  canvas.width = 36;
  canvas.height = 36;
  const context = canvas.getContext("2d");
  if (!context) throw new Error("Canvas rendering is unavailable");
  context.fillStyle = "white";
  context.beginPath();
  context.moveTo(18, 2);
  context.lineTo(23, 15);
  context.lineTo(34, 20);
  context.lineTo(34, 24);
  context.lineTo(22, 21);
  context.lineTo(21, 32);
  context.lineTo(15, 32);
  context.lineTo(14, 21);
  context.lineTo(2, 24);
  context.lineTo(2, 20);
  context.lineTo(13, 15);
  context.closePath();
  context.fill();
  return context.getImageData(0, 0, 36, 36);
}

export function createMapLibreFactory(styleUrl: string, staleAfterMs = 30_000): MapFactory {
  return ({ container, getAircraft, getMobilityCells, mobilityMode, onError, onSelect, onSelectCell }) => {
    maplibregl.setWorkerUrl(mapLibreWorkerUrl);
    const map = new maplibregl.Map({
      container,
      style: styleUrl,
      center: [10, 52],
      zoom: 3.35,
      attributionControl: {},
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "bottom-right");

    let frame: number | null = null;
    let lastFrame = 0;
    let lastMobilityMode = "";
    let lastMobilitySignature = "";
    const render = (timestamp: number) => {
      if (timestamp - lastFrame >= 33) {
        const now = Date.now();
        const source = map.getSource("aircraft") as GeoJSONSource | undefined;
        source?.setData(toFeatureCollection(getAircraft(now), now, staleAfterMs));

        const mobilitySource = map.getSource("mobility-cells") as GeoJSONSource | undefined;
        const mode = mobilityMode();
        const cells = getMobilityCells();
        const signature = mobilityRenderSignature(cells, mode, now);
        const mobilityChanged = signature !== lastMobilitySignature || mode !== lastMobilityMode;
        if (mobilitySource && mobilityChanged) {
          const visibility = mobilityLayerVisibility(mode);
          mobilitySource.setData(toMobilityFeatureCollection(cells, mode, now));
          map.setLayoutProperty("mobility-density", "visibility", visibility.density);
          map.setLayoutProperty("mobility-demand", "visibility", visibility.demand);
          map.setLayoutProperty("mobility-outline", "visibility", visibility.outline);
          lastMobilityMode = mode;
          lastMobilitySignature = signature;
        }
        lastFrame = timestamp;
      }
      frame = window.requestAnimationFrame(render);
    };

    map.on("error", (event) =>
      onError(new Error(event.error?.message ?? "Map failed to load")),
    );
    map.on("load", () => {
      map.addImage("aircraft-glyph", aircraftGlyph(), { sdf: true });
      const now = Date.now();
      map.addSource("aircraft", {
        type: "geojson",
        data: toFeatureCollection(getAircraft(now), now, staleAfterMs),
      });
      map.addLayer({
        id: "aircraft-live",
        type: "symbol",
        source: "aircraft",
        layout: {
          "icon-image": "aircraft-glyph",
          "icon-size": 0.56,
          "icon-rotate": ["get", "track_degrees"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
        },
        paint: {
          "icon-color": ["case", ["get", "stale"], "#F2B84B", "#64D8E8"],
          "icon-halo-color": "#07131D",
          "icon-halo-width": 1.5,
        },
      });
      map.on("click", "aircraft-live", (event: MapLayerMouseEvent) => {
        const icao24 = event.features?.[0]?.properties?.icao24;
        if (typeof icao24 === "string") onSelect(icao24);
      });

      map.addSource("mobility-cells", {
        type: "geojson",
        data: toMobilityFeatureCollection(getMobilityCells(), mobilityMode(), now),
      });
      const mobilityVisibility = mobilityLayerVisibility(mobilityMode());
      map.addLayer({
        id: "mobility-density",
        type: "fill",
        source: "mobility-cells",
        paint: {
          "fill-color": [
            "interpolate",
            ["linear"],
            ["get", "aircraft_count"],
            0,
            "#114b63",
            10,
            "#64d8e8",
          ],
          "fill-opacity": ["case", ["get", "stale"], 0.12, 0.36],
        },
        layout: {
          visibility: mobilityVisibility.density,
        },
      });
      map.addLayer({
        id: "mobility-demand",
        type: "fill",
        source: "mobility-cells",
        paint: {
          "fill-color": [
            "interpolate",
            ["linear"],
            ["get", "demand_units"],
            0,
            "#4a2a4f",
            10,
            "#f2b84b",
          ],
          "fill-opacity": ["case", ["get", "stale"], 0.12, 0.36],
        },
        layout: {
          visibility: mobilityVisibility.demand,
        },
      });
      map.addLayer({
        id: "mobility-outline",
        type: "line",
        source: "mobility-cells",
        paint: {
          "line-color": "#9dc6d5",
          "line-opacity": 0.6,
          "line-width": 1,
        },
        layout: {
          visibility: mobilityVisibility.outline,
        },
      });
      map.on("click", "mobility-density", (event: MapLayerMouseEvent) => {
        const cellId = event.features?.[0]?.properties?.cell_id;
        if (typeof cellId === "string") onSelectCell(cellId);
      });
      map.on("click", "mobility-demand", (event: MapLayerMouseEvent) => {
        const cellId = event.features?.[0]?.properties?.cell_id;
        if (typeof cellId === "string") onSelectCell(cellId);
      });
      map.on("mouseenter", "aircraft-live", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseenter", "mobility-density", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseenter", "mobility-demand", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", "aircraft-live", () => {
        map.getCanvas().style.cursor = "";
      });
      map.on("mouseleave", "mobility-density", () => {
        map.getCanvas().style.cursor = "";
      });
      map.on("mouseleave", "mobility-demand", () => {
        map.getCanvas().style.cursor = "";
      });
      frame = window.requestAnimationFrame(render);
    });

    return {
      destroy: () => {
        if (frame !== null) window.cancelAnimationFrame(frame);
        map.remove();
      },
    };
  };
}
