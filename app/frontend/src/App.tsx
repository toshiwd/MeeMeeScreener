import { Suspense, useEffect, useLayoutEffect } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { BackendReadyProvider } from "./backendReady";
import { AiExplainProvider } from "./features/aiExplain/AiExplainProvider";
import GlobalTxtUpdateStatus from "./components/GlobalTxtUpdateStatus";
import {
  installPerfDiagnosticsMainThreadMonitor,
  isPerfDiagnosticsEnabled,
  recordPerfEvent,
  updatePerfDiagnosticsRoute,
} from "./perfDiagnostics";
import {
  CandidatesRoute,
  DetailRoute,
  FavoritesRoute,
  GridRoute,
  MarketRoute,
  PositionsRoute,
  PracticeRoute,
  preloadPrimaryRoutes,
  RankingRoute,
  TrackingRoute,
} from "./routePreload";
import { applyTheme, getStoredTheme } from "./utils/theme";

function RouteTransitionFallback() {
  useEffect(() => {
    const startedAt = performance.now();
    recordPerfEvent("route_fallback_shown");
    return () => {
      recordPerfEvent("route_fallback_hidden", {
        visibleMs: Number((performance.now() - startedAt).toFixed(1)),
      });
    };
  }, []);

  return (
    <div className="route-transition-indicator" role="status" aria-live="polite">
      画面を切替中...
    </div>
  );
}

function AppRoutes() {
  const location = useLocation();

  useEffect(() => {
    updatePerfDiagnosticsRoute(location.pathname);
    recordPerfEvent("route_match_changed", {
      route: location.pathname,
    });
    const rafId = window.requestAnimationFrame(() => {
      recordPerfEvent("route_first_paint_ready", {
        route: location.pathname,
      });
    });
    return () => window.cancelAnimationFrame(rafId);
  }, [location.pathname]);

  useEffect(() => {
    if (!isPerfDiagnosticsEnabled()) return undefined;

    const describeTarget = (target: EventTarget | null) => {
      if (!(target instanceof HTMLElement)) return null;
      const actionable = target.closest("a,button,[role='tab']") as HTMLElement | null;
      if (!actionable) return null;
      const label = actionable.textContent?.trim()?.replace(/\s+/g, " ").slice(0, 80) ?? "";
      const href =
        actionable instanceof HTMLAnchorElement
          ? new URL(actionable.href, window.location.origin).pathname
          : actionable.getAttribute("data-route");
      return {
        tag: actionable.tagName.toLowerCase(),
        label,
        href,
      };
    };

    const handlePointer = (event: Event) => {
      const meta = describeTarget(event.target);
      if (!meta) return;
      recordPerfEvent(`ui_${event.type}`, meta);
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (!["Enter", " ", "Tab", "ArrowLeft", "ArrowRight"].includes(event.key)) return;
      const meta = describeTarget(event.target);
      recordPerfEvent("ui_keydown", {
        key: event.key,
        ...(meta ?? {}),
      });
    };

    document.addEventListener("pointerdown", handlePointer, true);
    document.addEventListener("click", handlePointer, true);
    document.addEventListener("keydown", handleKeyDown, true);
    return () => {
      document.removeEventListener("pointerdown", handlePointer, true);
      document.removeEventListener("click", handlePointer, true);
      document.removeEventListener("keydown", handleKeyDown, true);
    };
  }, []);

  return (
    <Suspense fallback={<RouteTransitionFallback />}>
      <Routes>
        <Route path="/" element={<GridRoute />} />
        <Route path="/ranking" element={<RankingRoute />} />
        <Route path="/favorites" element={<FavoritesRoute />} />
        <Route path="/candidates" element={<CandidatesRoute />} />
        <Route path="/positions" element={<PositionsRoute />} />
        <Route path="/market" element={<MarketRoute />} />
        <Route path="/ranking/tracking" element={<TrackingRoute />} />
        <Route path="/tracking" element={<Navigate to="/ranking/tracking" replace />} />
        <Route path="/detail/:code" element={<DetailRoute />} />
        <Route path="/practice/:code" element={<PracticeRoute />} />
      </Routes>
    </Suspense>
  );
}

export default function App() {
  useLayoutEffect(() => {
    const theme = getStoredTheme();
    applyTheme(theme);
  }, []);

  useEffect(() => {
    const cleanup = installPerfDiagnosticsMainThreadMonitor();
    return () => cleanup();
  }, []);

  useEffect(() => {
    const idleHandle =
      "requestIdleCallback" in window
        ? window.requestIdleCallback(() => preloadPrimaryRoutes())
        : window.setTimeout(() => preloadPrimaryRoutes(), 300);
    return () => {
      if ("cancelIdleCallback" in window && typeof idleHandle === "number") {
        window.cancelIdleCallback(idleHandle);
      } else {
        window.clearTimeout(idleHandle as number);
      }
    };
  }, []);

  return (
    <BackendReadyProvider>
      <AiExplainProvider>
        <GlobalTxtUpdateStatus />
        <AppRoutes />
      </AiExplainProvider>
    </BackendReadyProvider>
  );
}
