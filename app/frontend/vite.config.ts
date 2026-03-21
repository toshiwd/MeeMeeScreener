import { defineConfig, type Plugin, type PreviewServer, type ViteDevServer } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { readFileSync } from "node:fs";

const root = fileURLToPath(new URL(".", import.meta.url));
const packageJson = JSON.parse(
  readFileSync(resolve(root, "package.json"), "utf-8")
) as { version?: string };
const appVersion = packageJson.version ?? "0.0.0";

const hasFileExtension = (pathname: string) => /\.[a-zA-Z0-9]+$/.test(pathname);

const shouldBypassRewrite = (pathname: string) => {
  if (!pathname) return true;
  if (pathname.startsWith("/api/")) return true;
  if (pathname.startsWith("/src/")) return true;
  if (pathname.startsWith("/@")) return true;
  if (pathname.startsWith("/node_modules/")) return true;
  if (pathname.startsWith("/assets/")) return true;
  if (pathname === "/favicon.ico") return true;
  return hasFileExtension(pathname);
};

const createSpaRewritePlugin = (): Plugin => {
  const rewrite = (req: Parameters<ViteDevServer["middlewares"]["use"]>[0], next: () => void) => {
    const rawUrl = req.url;
    if (!rawUrl) {
      next();
      return;
    }
    const parsed = new URL(rawUrl, "http://localhost");
    const { pathname, search } = parsed;
    if (shouldBypassRewrite(pathname)) {
      next();
      return;
    }

    if (pathname === "/tradex" || pathname.startsWith("/tradex/")) {
      req.url = `/tradex/index.html${search}`;
      next();
      return;
    }

    req.url = `/index.html${search}`;
    next();
  };

  return {
    name: "meemee-tradex-spa-rewrite",
    configureServer(server) {
      server.middlewares.use((req, _res, next) => rewrite(req, next));
    },
    configurePreviewServer(server: PreviewServer) {
      server.middlewares.use((req, _res, next) => rewrite(req, next));
    }
  };
};

export default defineConfig({
  root,
  appType: "mpa",
  plugins: [react(), createSpaRewritePlugin()],
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
      input: {
        main: resolve(root, "index.html"),
        tradex: resolve(root, "tradex/index.html")
      }
    }
  }
});
