import type { ProjectedAircraft } from "../lib/contracts";
import type { JSX } from "react";

type AircraftDetailProps = {
  aircraft: ProjectedAircraft | null;
};

export function AircraftDetail({ aircraft }: AircraftDetailProps): JSX.Element {
  if (!aircraft) {
    return (
      <aside aria-live="polite" className="ops-panel aircraft-detail">
        <h2>Aircraft detail</h2>
        <p className="empty-copy">Select a track to inspect its live state.</p>
      </aside>
    );
  }

  return (
    <aside aria-live="polite" className="ops-panel aircraft-detail">
      <div className="panel-heading">
        <h2>{aircraft.callsign ?? "No callsign"}</h2>
        <span>{aircraft.icao24.toUpperCase()}</span>
      </div>
      <dl>
        <div><dt>Origin</dt><dd>{aircraft.origin_country}</dd></div>
        <div><dt>Altitude</dt><dd>{formatAltitude(aircraft.position?.barometric_altitude_m)}</dd></div>
        <div><dt>Ground speed</dt><dd>{formatSpeed(aircraft.motion?.ground_speed_mps)}</dd></div>
        <div><dt>Track</dt><dd>{formatTrack(aircraft.motion?.track_degrees)}</dd></div>
        <div><dt>Vertical rate</dt><dd>{formatVerticalRate(aircraft.motion?.vertical_rate_mps)}</dd></div>
        <div><dt>Position source</dt><dd>{aircraft.position_source?.replace("_", "-") ?? "Unknown"}</dd></div>
      </dl>
    </aside>
  );
}

const formatAltitude = (value: number | null | undefined): string =>
  value == null ? "No data" : `${Math.round(value).toLocaleString()} m`;

const formatSpeed = (value: number | null | undefined): string =>
  value == null ? "No data" : `${Math.round(value * 3.6).toLocaleString()} km/h`;

const formatTrack = (value: number | null | undefined): string =>
  value == null ? "No data" : `${Math.round(value).toString().padStart(3, "0")}°`;

const formatVerticalRate = (value: number | null | undefined): string =>
  value == null ? "No data" : `${value >= 0 ? "+" : ""}${value.toFixed(1)} m/s`;
