import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { assertStreamMessage, parseAircraftPositionEvent } from "./contracts";

const fixture = JSON.parse(
  readFileSync("../tests/contract/examples/aircraft-position.v1.json", "utf8"),
) as unknown;

describe("contracts", () => {
  it("parses the canonical aircraft event", () => {
    const event = parseAircraftPositionEvent(fixture);
    expect(event.aircraft.icao24).toBe("3c6444");
    expect(event.schema_version).toBe(1);
  });

  it("rejects stream payloads without a message_type", () => {
    expect(() => assertStreamMessage({})).toThrow("stream message has no message_type");
  });
});
