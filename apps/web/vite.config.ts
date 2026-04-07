import fs from "node:fs";
import path from "node:path";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

function readDotEnvDevValue(names: string[]): string {
  const candidates = [
    path.resolve(process.cwd(), ".env.dev"),
    path.resolve(process.cwd(), "../.env.dev"),
    path.resolve(process.cwd(), "../../.env.dev"),
    path.resolve(process.cwd(), "../../../.env.dev"),
  ];
  for (const fp of candidates) {
    try {
      if (!fs.existsSync(fp)) continue;
      const raw = fs.readFileSync(fp, "utf8");
      for (const name of names) {
        const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const m = raw.match(new RegExp(`^\\s*(?:export\\s+)?${escaped}\\s*=\\s*(.+)\\s*$`, "m"));
        if (!m) continue;
        const v = m[1].trim().replace(/^['"]|['"]$/g, "");
        if (v.length > 0) return v;
      }
    } catch {
      /* ignore parse/read errors */
    }
  }
  return "";
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const cesiumToken =
    process.env.CESIUM_TOKEN ??
    process.env.VITE_CESIUM_TOKEN ??
    env.CESIUM_TOKEN ??
    env.VITE_CESIUM_TOKEN ??
    readDotEnvDevValue(["CESIUM_TOKEN", "VITE_CESIUM_TOKEN"]);
  const clerkPublishableKey =
    process.env.VITE_CLERK_PUBLISHABLE_KEY ??
    process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY ??
    env.VITE_CLERK_PUBLISHABLE_KEY ??
    env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY ??
    readDotEnvDevValue(["VITE_CLERK_PUBLISHABLE_KEY", "NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY"]);
  return {
    plugins: [react()],
    envPrefix: ["VITE_", "NEXT_PUBLIC_", "CESIUM_"],
    define: {
      __CESIUM_TOKEN__: JSON.stringify(cesiumToken || ""),
      __CLERK_PUBLISHABLE_KEY__: JSON.stringify(clerkPublishableKey || ""),
    },
    server: {
      port: 5174,
      strictPort: true,
      proxy: {
        "/api": {
          target: "http://127.0.0.1:3000",
          changeOrigin: true,
          rewrite: (p) => p.replace(/^\/api/, ""),
        },
      },
    },
  };
});
