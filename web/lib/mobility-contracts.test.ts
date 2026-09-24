import { describe, expect, it } from "vitest";

import { assertMobilityStreamMessage, parseMobilitySnapshot } from "./mobility-contracts";

describe("mobility contracts", () => {
  it("parses snapshot payload", () => {
    const parsed = parseMobilitySnapshot({
      generated_at: "2026-09-24T09:10:00Z",
      source_status: {
        source: "opensky",
        status: "live",
        last_observation_at: "2026-09-24T09:10:00Z",
        age_seconds: 0,
      },
      cells: [],
    });
    expect(parsed.cells).toEqual([]);
  });

  it("rejects stream payloads without a message_type", () => {
    expect(() => assertMobilityStreamMessage({})).toThrow(
      "mobility stream message has no message_type",
    );
  });
});
