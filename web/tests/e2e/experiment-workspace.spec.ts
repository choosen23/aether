import { expect, test } from "@playwright/test";

test("shows experiments workspace toggle", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("button", { name: "Live operations" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Experiments" })).toBeVisible();

  await page.getByRole("button", { name: "Experiments" }).click();
  await expect(page.getByText("Experiment workspace")).toBeVisible();
});
