"use client";

import type { JSX } from "react";

import type { MobilityLayerMode } from "../lib/types-mobility";

type LayerControlProps = {
  mode: MobilityLayerMode;
  onChange: (mode: MobilityLayerMode) => void;
};

export function LayerControl({ mode, onChange }: LayerControlProps): JSX.Element {
  return (
    <section className="ops-panel layer-control" aria-label="Layer control">
      <div className="panel-heading">
        <h2>Layers</h2>
      </div>
      <div role="radiogroup" aria-label="Map layers">
        <label>
          <input
            type="radio"
            name="layer-mode"
            checked={mode === "aircraft"}
            onChange={() => onChange("aircraft")}
          />
          Aircraft only
        </label>
        <label>
          <input
            type="radio"
            name="layer-mode"
            checked={mode === "density"}
            onChange={() => onChange("density")}
          />
          Aircraft density
        </label>
        <label>
          <input
            type="radio"
            name="layer-mode"
            checked={mode === "demand"}
            onChange={() => onChange("demand")}
          />
          Modelled demand
        </label>
      </div>
    </section>
  );
}
