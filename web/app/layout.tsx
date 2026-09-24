import "./globals.css";
import "maplibre-gl/dist/maplibre-gl.css";

import type { JSX, ReactNode } from "react";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Aether — European airspace twin",
  description: "Live OpenSky aircraft telemetry rendered through Aether's event pipeline.",
};

type RootLayoutProps = {
  children: ReactNode;
};

export default function RootLayout({ children }: RootLayoutProps): JSX.Element {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
