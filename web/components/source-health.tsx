import type { JSX } from "react";

type SourceHealthProps = {
  providerName: string;
  sourceStatus: "live" | "stale" | "offline";
  streamConnected: boolean;
};

export function SourceHealth({
  providerName,
  sourceStatus,
  streamConnected,
}: SourceHealthProps): JSX.Element {
  return (
    <section aria-live="polite" className="ops-panel source-health">
      <div className="panel-heading">
        <h2>Source health</h2>
        <span className={`status-light status-${sourceStatus}`} aria-hidden="true" />
      </div>
      <dl>
        <div><dt>Provider</dt><dd>{providerName}</dd></div>
        <div><dt>Telemetry</dt><dd>{sourceStatus}</dd></div>
        <div><dt>Gateway</dt><dd>{streamConnected ? "Stream connected" : "Stream disconnected"}</dd></div>
      </dl>
    </section>
  );
}
