import fs from "node:fs";
import path from "node:path";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

function readCesiumTokenFromDotEnvDev(): string {
  const candidates = [
    path.resolve(process.cwd(), ".env.dev"),
    path.resolve(process.cwd(), "../../.env.dev"),
  ];
  for (const fp of candidates) {
    try {
      if (!fs.existsSync(fp)) continue;
      const raw = fs.readFileSync(fp, "utf8");
      const m = raw.match(/^\s*CESIUM_TOKEN\s*=\s*(.+)\s*$/m);
      if (!m) continue;
      const v = m[1].trim().replace(/^['"]|['"]$/g, "");
      if (v.length > 0) return v;
    } catch {
      /* ignore parse/read errors */
    }
  }
  return "";
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const cesiumToken =
    env.CESIUM_TOKEN ??
    env.VITE_CESIUM_TOKEN ??
    readCesiumTokenFromDotEnvDev();
  return {
    plugins: [react()],
    envPrefix: ["VITE_", "CESIUM_"],
    define: {
      __CESIUM_TOKEN__: JSON.stringify(cesiumToken || ""),
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
