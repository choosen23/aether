import type { ProjectedAircraft } from "./contracts";

import type { AircraftTrack } from "./aircraft-store";

const clamp = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, value));

const wrapDelta = (delta: number, period: number): number =>
  ((((delta + period / 2) % period) + period) % period) - period / 2;

const normalizeLongitude = (value: number): number => {
  const normalized = ((((value + 180) % 360) + 360) % 360) - 180;
  return normalized === -180 ? 180 : normalized;
};

export function interpolateAngle(from: number, to: number, ratio: number): number {
  return (from + wrapDelta(to - from, 360) * ratio + 360) % 360;
}

export function interpolateAircraft(
  track: AircraftTrack,
  now: number,
  horizonMs: number,
): ProjectedAircraft {
  const current = track.current;
  const previous = track.previous;

  if (!previous) {
    return current;
  }

  const previousMs = Date.parse(previous.observed_at);
  const currentMs = Date.parse(current.observed_at);
  const freezeAtMs = currentMs + horizonMs;

  if (now >= freezeAtMs || now >= currentMs || currentMs <= previousMs) {
    return current;
  }

  const ratio = clamp((now - previousMs) / (currentMs - previousMs), 0, 1);

  if (!previous.position || !current.position) {
    return current;
  }

  const longitudeDelta = wrapDelta(current.position.longitude - previous.position.longitude, 360);
  const longitude = normalizeLongitude(previous.position.longitude + longitudeDelta * ratio);
  const latitude = previous.position.latitude + (current.position.latitude - previous.position.latitude) * ratio;

  const previousAltitude = previous.position.barometric_altitude_m;
  const currentAltitude = current.position.barometric_altitude_m;
  const barometric_altitude_m =
    previousAltitude === null || currentAltitude === null
      ? currentAltitude
      : previousAltitude + (currentAltitude - previousAltitude) * ratio;

  const previousTrack = previous.motion?.track_degrees;
  const currentTrack = current.motion?.track_degrees;
  const trackDegrees =
    previousTrack === null ||
    previousTrack === undefined ||
    currentTrack === null ||
    currentTrack === undefined
      ? currentTrack ?? null
      : interpolateAngle(previousTrack, currentTrack, ratio);

  return {
    ...current,
    position: {
      ...current.position,
      latitude,
      longitude,
      barometric_altitude_m,
    },
    motion: current.motion
      ? {
          ...current.motion,
          track_degrees: trackDegrees,
        }
      : null,
  };
}
