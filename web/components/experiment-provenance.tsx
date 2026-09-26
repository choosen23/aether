import type { JSX } from "react";

type ExperimentProvenanceProps = {
  notes: readonly string[];
};

export function ExperimentProvenance({ notes }: ExperimentProvenanceProps): JSX.Element {
  return (
    <section aria-label="Experiment provenance">
      <h3>Provenance</h3>
      <ul>
        {notes.map((note) => (
          <li key={note}>{note}</li>
        ))}
      </ul>
    </section>
  );
}
