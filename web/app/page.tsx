"use client";

import { useMemo } from "react";
import type { JSX } from "react";

import { OperationsShell } from "../components/operations-shell";

import type { MapFactory } from "../components/airspace-map";
import { createMapLibreFactory } from "../lib/maplibre-map";
import { createRuntimeOperationsClient } from "../lib/runtime-client";

export default function HomePage(): JSX.Element {
  const client = useMemo(() => createRuntimeOperationsClient(), []);
  const mapFactory: MapFactory = useMemo(
    () =>
      createMapLibreFactory(
        process.env.NEXT_PUBLIC_MAP_STYLE_URL ?? "https://tiles.openfreemap.org/styles/dark",
      ),
    [],
  );
  return <OperationsShell mapFactory={mapFactory} client={client} />;
}
