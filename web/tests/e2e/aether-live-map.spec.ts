import { expect, test } from "@playwright/test";

test("shows and updates a fixture aircraft", async ({ page }) => {
  await page.addInitScript(() => {
    const snapshot = {
      generated_at: "2026-09-24T09:10:00Z",
      source_status: {
        source: "opensky",
        status: "live",
        last_observation_at: "2026-09-24T09:10:00Z",
        age_seconds: 0,
      },
      aircraft: [{
        icao24: "3c6444",
        callsign: "DLH4YA",
        origin_country: "Germany",
        position: { latitude: 50.04, longitude: 8.56, barometric_altitude_m: 10668, on_ground: false },
        motion: { ground_speed_mps: 232, track_degrees: 271, vertical_rate_mps: -1.2 },
        last_contact_at: "2026-09-24T09:10:00Z",
        position_source: "ads_b",
        event_id: "fixture-event",
        observed_at: "2026-09-24T09:10:00Z",
      }],
    };
    const nativeFetch = window.fetch.bind(window);
    window.fetch = (input, init) => {
      if (String(input).includes("/api/v1/aircraft")) {
        return Promise.resolve(new Response(JSON.stringify(snapshot), {
          status: 200,
          headers: { "content-type": "application/json" },
        }));
      }
      return nativeFetch(input, init);
    };

    const NativeWebSocket = window.WebSocket;
    const FixtureWebSocket = new Proxy(NativeWebSocket, {
      construct(Target, args) {
        const url = String(args[0]);
        if (!url.includes("/api/v1/stream/aircraft")) {
          return Reflect.construct(Target, args);
        }

        const socket = {
          readyState: NativeWebSocket.OPEN,
          onopen: null as (() => void) | null,
          onmessage: null as ((event: MessageEvent) => void) | null,
          onclose: null as (() => void) | null,
          close() { this.onclose?.(); },
        };
        window.setTimeout(() => socket.onopen?.(), 0);
        return socket;
      },
    });
    Object.defineProperty(window, "WebSocket", { value: FixtureWebSocket });
  });

  await page.route("https://tiles.openfreemap.org/styles/dark", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        version: 8,
        sources: {},
        layers: [{ id: "background", type: "background", paint: { "background-color": "#10283A" } }],
      }),
    });
  });

  await page.goto("/");
  await expect(page.getByText("Stream connected")).toBeVisible();
  await expect(page.getByText("OpenSky", { exact: true })).toBeVisible();
  await expect(page.getByTestId("aircraft-count")).toHaveText("1");
  await expect(page.getByLabel("Airspace map")).toBeVisible();
  await expect(page.getByLabel("Airspace map").locator("canvas")).toBeVisible();
  await expect(page.locator(".map-alert")).toHaveCount(0);
});
