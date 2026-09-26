import { describe, expect, it } from "vitest";

import { SimulationStore } from "./simulation-store";

const runUpdate = (sequence: number, status: "running" | "paused") => ({
  message_type: "simulation.run.updated" as const,
  schema_version: 1 as const,
  sent_at: "2026-09-25T09:10:00Z",
  run: {
    run_id: "run-1",
    scenario_id: "azure-functions-eu-small",
    source_trace: "azure_functions" as const,
    scheduler_policy: "balanced",
    sequence,
    status,
  },
});

describe("SimulationStore", () => {
  it("ignores an older run sequence", () => {
    const store = new SimulationStore();

    store.apply(runUpdate(8, "running"));
    store.apply(runUpdate(7, "paused"));

    expect(store.getSnapshot().run?.sequence).toBe(8);
    expect(store.getSnapshot().run?.status).toBe("running");
  });
});
