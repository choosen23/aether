import js from "@eslint/js";

export default [
  {
    ...js.configs.recommended,
    files: ["**/*.{js,mjs,cjs}"],
  },
  {
    ignores: [
      ".next/**",
      "node_modules/**",
      "public/maplibre-gl-shared.mjs",
      "public/maplibre-gl-worker.mjs",
    ],
  },
];
