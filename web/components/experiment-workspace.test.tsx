import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";

import type { SimulationClient } from "../lib/simulation-client";
import type { SimulationStoreSnapshot } from "../lib/simulation-contracts";
import { ExperimentWorkspace } from "./experiment-workspace";

class FakeClient implements SimulationClient {
  start(): void {}

  stop(): void {}

  subscribe(): () => void {
    return () => undefined;
  }

  getSnapshot(): SimulationStoreSnapshot {
    return {
      runs: [
        {
          run_id: "run-1",
          scenario_id: "azure-functions-eu-small",
          source_trace: "azure_functions" as const,
          scheduler_policy: "balanced",
          status: "completed" as const,
          sequence: 12,
          summary: {
            component_metrics: {
              completion_rate: 1,
            },
            combined_score: 0.91,
          },
        },
        {
          run_id: "run-2",
          scheduler_policy: "cloud_only",
          status: "completed" as const,
          sequence: 10,
          summary: {
            component_metrics: {
              deadline_miss_rate: 0.03,
            },
            combined_score: 0.75,
          },
        },
      ],
      run: {
        run_id: "run-1",
        scenario_id: "azure-functions-eu-small",
        source_trace: "azure_functions" as const,
        scheduler_policy: "balanced",
        status: "completed" as const,
        sequence: 12,
        provenance_notes: ["modelled origin assignment"],
        summary: {
          component_metrics: {
            completion_rate: 1,
            p95_end_to_end_latency_ms: 1200,
          },
          combined_score: 0.91,
        },
      },
      streamConnected: true,
      lastHeartbeatAt: "2026-09-25T09:10:00Z",
    };
  }
}

it("shows provenance and no mutating controls", () => {
  const fakeClient = new FakeClient();
  const fakeMapFactory = () => ({ destroy: () => undefined });

  render(<ExperimentWorkspace client={fakeClient} mapFactory={fakeMapFactory} />);

  expect(screen.getByText("Azure Functions")).toBeInTheDocument();
  expect(screen.getByText("modelled origin assignment")).toBeInTheDocument();
  expect(screen.getByText(/Completion rate/i)).toBeInTheDocument();
  expect(screen.getByText(/Deadline miss rate/i)).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /start|pause|cancel/i })).not.toBeInTheDocument();
});
