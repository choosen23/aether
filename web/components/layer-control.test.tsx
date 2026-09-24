import { render, screen } from "@testing-library/react";
import { fireEvent } from "@testing-library/react";
import { expect, it } from "vitest";

import { LayerControl } from "./layer-control";

it("switches to modelled demand", async () => {
  const seen: string[] = [];
  render(<LayerControl mode="aircraft" onChange={(mode) => seen.push(mode)} />);

  fireEvent.click(screen.getByRole("radio", { name: "Modelled demand" }));
  expect(seen).toEqual(["demand"]);
});
