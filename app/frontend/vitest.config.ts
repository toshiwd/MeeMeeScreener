import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = fileURLToPath(new URL(".", import.meta.url));
const packageJson = JSON.parse(readFileSync(resolve(root, "package.json"), "utf-8")) as {
  version?: string;
};

export default defineConfig({
  root,
  plugins: [react()],
  define: {
    __APP_VERSION__: JSON.stringify(packageJson.version ?? "0.0.0")
  },
  test: {
    environment: "jsdom",
    globals: true,
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    exclude: ["e2e/**", "dist/**", "node_modules/**"]
  }
});
