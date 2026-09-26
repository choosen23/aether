import type { JSX } from "react";

type ExperimentMapProps = {
  mapFactory: () => { destroy: () => void };
};

export function ExperimentMap({ mapFactory }: ExperimentMapProps): JSX.Element {
  try {
    const map = mapFactory();
    map.destroy();
    return <section aria-label="Experiment map">Map ready</section>;
  } catch (error) {
    const message = error instanceof Error ? error.message : "map failed";
    return <section aria-label="Experiment map">Map unavailable: {message}</section>;
  }
}
