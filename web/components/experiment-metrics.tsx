import type { JSX } from "react";

type ExperimentMetricsProps = {
  title?: string;
};

export function ExperimentMetrics({ title = "Metrics" }: ExperimentMetricsProps): JSX.Element {
  return (
    <section aria-label="Experiment metrics">
      <h3>{title}</h3>
      <p>Component metrics are available in comparison mode.</p>
    </section>
  );
}
