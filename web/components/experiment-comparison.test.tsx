import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ExperimentComparison } from "./experiment-comparison";

describe("ExperimentComparison", () => {
  it("renders component metrics beside the combined score", () => {
    render(
      <ExperimentComparison
        runs={[
          {
            run_id: "run-azure-balanced",
            policy: "balanced",
            combined_score: 0.82,
            component_metrics: {
              completion_rate: 0.98,
              deadline_miss_rate: 0.04,
              p95_end_to_end_latency_ms: 1250,
            },
          },
        ]}
      />,
    );

    expect(screen.getByText(/Combined score/i)).toBeInTheDocument();
    expect(screen.getByText(/Completion rate/i)).toBeInTheDocument();
    expect(screen.getByText(/Deadline miss rate/i)).toBeInTheDocument();
    expect(screen.getByText(/P95 end-to-end latency/i)).toBeInTheDocument();
  });

  it("aggregates eu-medium-v1 nodes when zoomed out", () => {
    render(
      <ExperimentComparison
        runs={[]}
        mapView={{
          topology_id: "eu-medium-v1",
          zoom: 3,
          nodes: [
            { node_id: "far-01", tier: "far_edge", latitude: 48.8566, longitude: 2.3522 },
            { node_id: "far-02", tier: "far_edge", latitude: 48.85, longitude: 2.35 },
          ],
        }}
      />,
    );

    expect(screen.getByText(/Far edge clusters/i)).toBeInTheDocument();
  });
});
