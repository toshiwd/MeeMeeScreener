import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { readFileSync } from "node:fs";

const root = fileURLToPath(new URL(".", import.meta.url));
const packageJson = JSON.parse(
  readFileSync(resolve(root, "package.json"), "utf-8")
) as { version?: string };
const appVersion = packageJson.version ?? "0.0.0";

export default defineConfig({
  root,
  appType: "spa",
  plugins: [react()],
  define: {
    __APP_VERSION__: JSON.stringify(appVersion)
  },
  server: {
    port: 5173,
    proxy: {
      "/api": process.env.VITE_API_PROXY_TARGET || "http://localhost:8000"
    }
  },
  build: {
    rollupOptions: {
      input: resolve(root, "index.html")
    }
  }
});
