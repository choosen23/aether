"use client";

import type { JSX } from "react";

import type { MobilityCell } from "../lib/types-mobility";

type MobilityCellDetailProps = {
  cell: MobilityCell | null;
};

export function MobilityCellDetail({ cell }: MobilityCellDetailProps): JSX.Element {
  return (
    <section className="ops-panel mobility-cell-detail" aria-live="polite">
      <div className="panel-heading">
        <h2>Mobility cell</h2>
        <span>{cell?.density.cell_id ?? "No selection"}</span>
      </div>
      {cell ? (
        <dl>
          <div>
            <dt>Aircraft</dt>
            <dd>{cell.density.aircraft_count} aircraft</dd>
          </div>
          <div>
            <dt>Airborne / on ground</dt>
            <dd>
              {cell.density.airborne_count} / {cell.density.on_ground_count}
            </dd>
          </div>
          <div>
            <dt>Demand units</dt>
            <dd>{cell.demand.demand_units.toFixed(2)}</dd>
          </div>
          <div>
            <dt>Provenance</dt>
            <dd>Density: derived from OpenSky positions; demand: modelled.</dd>
          </div>
        </dl>
      ) : (
        <p className="empty-copy">Select a mobility polygon for cell details.</p>
      )}
    </section>
  );
}
