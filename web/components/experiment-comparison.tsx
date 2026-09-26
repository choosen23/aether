import type { JSX } from "react";

import { summarizeTierClusters } from "../lib/simulation-geometry";

type ComparisonRun = {
  run_id: string;
  policy: string;
  combined_score: number | null;
  component_metrics: Record<string, number>;
};

type ComparisonMapView = {
  topology_id: string;
  zoom: number;
  nodes: Array<{
    node_id: string;
    tier: string;
    latitude: number;
    longitude: number;
  }>;
};

type ExperimentComparisonProps = {
  runs: readonly ComparisonRun[];
  mapView?: ComparisonMapView;
};

export function ExperimentComparison({ runs, mapView }: ExperimentComparisonProps): JSX.Element {
  const showClusters = mapView?.topology_id === "eu-medium-v1" && mapView.zoom < 5;
  const farEdgeClusters = showClusters
    ? summarizeTierClusters(mapView.nodes).find((summary) => summary.tier === "far_edge")?.nodeCount ?? 0
    : 0;

  return (
    <section aria-label="Experiment comparison">
      <h3>Combined score</h3>
      <ul>
        {runs.map((run) => (
          <li key={run.run_id}>
            <strong>{run.policy}</strong> — {run.combined_score ?? "n/a"}
            {Object.entries(run.component_metrics).map(([metric, value]) => (
              <div key={metric}>{formatMetricLabel(metric)}: {formatMetricValue(metric, value)}</div>
            ))}
          </li>
        ))}
      </ul>
      {showClusters ? <p>Far edge clusters: {farEdgeClusters}</p> : null}
    </section>
  );
}

function formatMetricLabel(metric: string): string {
  if (metric.startsWith("p95_")) {
    const raw = metric.slice(4);
    const normalized = raw.replace("end_to_end", "end-to-end").replaceAll("_", " ");
    return `P95 ${normalized}`;
  }
  return metric
    .replaceAll("_", " ")
    .replace(/\b\w/g, (segment) => segment.toUpperCase());
}

function formatMetricValue(metric: string, value: number): string {
  if (metric.endsWith("_ms")) {
    return `${value} ms`;
  }
  return String(value);
}
