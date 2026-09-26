"use client";

import { useEffect, useState } from "react";
import type { JSX } from "react";

import { ExperimentComparison } from "./experiment-comparison";
import { ExperimentMap } from "./experiment-map";
import { ExperimentMetrics } from "./experiment-metrics";
import { ExperimentProvenance } from "./experiment-provenance";
import type { SimulationClient } from "../lib/simulation-client";

type ExperimentWorkspaceProps = {
  client: SimulationClient;
  mapFactory: () => { destroy: () => void };
};

const sourceLabel = (sourceTrace: string | undefined): string => {
  if (sourceTrace === "azure_functions") {
    return "Azure Functions";
  }
  if (sourceTrace === "google_borg") {
    return "Google Borg";
  }
  return "Unknown source";
};

export function ExperimentWorkspace({ client, mapFactory }: ExperimentWorkspaceProps): JSX.Element {
  const [version, setVersion] = useState(0);

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
  const currentRun = snapshot.run;
  const provenanceNotes = currentRun?.provenance_notes ?? [];
  const comparisonByRunId = new Map(
    snapshot.runs.map((run) => [
      run.run_id,
      {
        run_id: run.run_id,
        policy: run.scheduler_policy,
        combined_score: run.summary?.combined_score ?? null,
        component_metrics: run.summary?.component_metrics ?? {},
      },
    ]),
  );

  if (currentRun) {
    comparisonByRunId.set(currentRun.run_id, {
      run_id: currentRun.run_id,
      policy: currentRun.scheduler_policy,
      combined_score: currentRun.summary?.combined_score ?? null,
      component_metrics: currentRun.summary?.component_metrics ?? {},
    });
  }

  const comparisonRuns = [...comparisonByRunId.values()];

  return (
    <main className="experiment-workspace" data-version={version}>
      <header>
        <h2>Experiment workspace</h2>
        {currentRun ? <p>{sourceLabel(currentRun.source_trace)}</p> : <p>No run selected</p>}
      </header>

      <ExperimentMap mapFactory={mapFactory} />
      <ExperimentMetrics />
      <ExperimentComparison runs={comparisonRuns} />
      <ExperimentProvenance notes={provenanceNotes} />
    </main>
  );
}
