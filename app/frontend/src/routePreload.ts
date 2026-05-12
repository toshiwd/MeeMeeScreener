import { lazy } from "react";
import type { ComponentType } from "react";
import { recordPerfEvent } from "./perfDiagnostics";

type RouteKey =
  | "/"
  | "/ranking"
  | "/favorites"
  | "/candidates"
  | "/positions"
  | "/market"
  | "/ranking/tracking"
  | "/detail/:code"
  | "/detail-v2/:code"
  | "/practice/:code";

type RouteModule = { default: ComponentType<any> };

const routeImporters: Record<RouteKey, () => Promise<RouteModule>> = {
  "/": () => import("./routes/GridView"),
  "/ranking": () => import("./routes/RankingView"),
  "/favorites": () => import("./routes/FavoritesView"),
  "/candidates": () => import("./routes/CandidatesView"),
  "/positions": () => import("./routes/PositionsView"),
  "/market": () => import("./routes/MarketView"),
  "/ranking/tracking": () => import("./routes/TrackingView"),
  "/detail/:code": () => import("./routes/DetailView"),
  "/detail-v2/:code": () => import("./routes/DetailView"),
  "/practice/:code": () => import("./routes/PracticeView"),
};

const routeLabels: Record<RouteKey, string> = {
  "/": "grid",
  "/ranking": "ranking",
  "/favorites": "favorites",
  "/candidates": "candidates",
  "/positions": "positions",
  "/market": "market",
  "/ranking/tracking": "tracking",
  "/detail/:code": "detail",
  "/detail-v2/:code": "detail-v2",
  "/practice/:code": "practice",
};

const routeModulePromises = new Map<RouteKey, Promise<RouteModule>>();

const normalizeRouteKey = (path: string): RouteKey | null => {
  if (!path) return null;
  if (path.startsWith("/detail-v2/")) return "/detail-v2/:code";
  if (path.startsWith("/detail/")) return "/detail/:code";
  if (path.startsWith("/practice/")) return "/practice/:code";
  if (path.startsWith("/ranking/tracking")) return "/ranking/tracking";
  if (
    path === "/" ||
    path === "/ranking" ||
    path === "/favorites" ||
    path === "/candidates" ||
    path === "/positions" ||
    path === "/market"
  ) {
    return path;
  }
  return null;
};

const loadRouteModule = (key: RouteKey) => {
  const existing = routeModulePromises.get(key);
  if (existing) return existing;
  recordPerfEvent("route_chunk_load_start", {
    route: key,
    label: routeLabels[key],
  });
  const promise = routeImporters[key]()
    .then((module) => {
      recordPerfEvent("route_chunk_load_end", {
        route: key,
        label: routeLabels[key],
      });
      return module;
    })
    .catch((error) => {
      recordPerfEvent("route_chunk_load_failed", {
        route: key,
        label: routeLabels[key],
        message: error instanceof Error ? error.message : String(error),
      });
      throw error;
    });
  routeModulePromises.set(key, promise);
  return promise;
};

export const preloadRoute = (path: string) => {
  const key = normalizeRouteKey(path);
  if (!key) return null;
  return loadRouteModule(key);
};

export const getPrimaryPreloadRoutes = (): RouteKey[] => [
  "/ranking",
  "/favorites",
  "/market",
  "/positions",
  "/candidates",
];

export const preloadPrimaryRoutes = () => {
  const routes = getPrimaryPreloadRoutes();
  routes.forEach((route) => {
    void loadRouteModule(route).catch(() => undefined);
  });
};

export const GridRoute = lazy(() => loadRouteModule("/"));
export const RankingRoute = lazy(() => loadRouteModule("/ranking"));
export const FavoritesRoute = lazy(() => loadRouteModule("/favorites"));
export const CandidatesRoute = lazy(() => loadRouteModule("/candidates"));
export const PositionsRoute = lazy(() => loadRouteModule("/positions"));
export const MarketRoute = lazy(() => loadRouteModule("/market"));
export const TrackingRoute = lazy(() => loadRouteModule("/ranking/tracking"));
export const DetailRoute = lazy(() => loadRouteModule("/detail/:code"));
export const DetailV2Route = lazy(() => loadRouteModule("/detail-v2/:code"));
export const PracticeRoute = lazy(() => loadRouteModule("/practice/:code"));
