type PerfEventPayload = Record<string, unknown> | undefined;

export type PerfEventRecord = {
  seq: number;
  time: string;
  timeMs: number;
  route: string;
  eventType: string;
  payload?: Record<string, unknown>;
};

type DiagnosticsSnapshot = {
  currentRoute: string;
  visibleCode: string | null;
  backendState: Record<string, unknown> | null;
  lastApiError: Record<string, unknown> | null;
  lastApiSuccess: Record<string, unknown> | null;
};

type DiagnosticsApi = {
  export_perf_diagnostics?: (payload?: unknown) => Promise<unknown>;
  clear_perf_diagnostics?: () => Promise<unknown>;
  open_perf_diagnostics_dir?: () => Promise<unknown>;
};

type PerfDiagnosticsBridge = {
  enabled: () => boolean;
  export: () => Promise<unknown>;
  clear: () => Promise<unknown>;
  openDir: () => Promise<unknown>;
  getEvents: () => PerfEventRecord[];
  enable: () => void;
  disable: () => void;
};

const PERF_DIAGNOSTICS_FLAG_KEY = "meemeePerfDiagnosticsEnabled";
const PERF_DIAGNOSTICS_BUFFER_LIMIT = 4000;
const EVENT_LOOP_LAG_INTERVAL_MS = 500;
const EVENT_LOOP_LAG_THRESHOLD_MS = 150;
const RAF_GAP_THRESHOLD_MS = 250;

let seq = 0;
let buffer: PerfEventRecord[] = [];
let globalsInstalled = false;
let mainThreadMonitorCleanup: (() => void) | null = null;
const initialRoutePath = () => {
  if (typeof window === "undefined") return "/";
  return window.location?.pathname ?? "/";
};
let snapshot: DiagnosticsSnapshot = {
  currentRoute: initialRoutePath(),
  visibleCode: null,
  backendState: null,
  lastApiError: null,
  lastApiSuccess: null,
};

const isBrowser = () => typeof window !== "undefined";

const currentRoutePath = () => {
  if (!isBrowser()) return "/";
  return snapshot.currentRoute || window.location?.pathname || "/";
};

const normalizeFlag = (value: string | null) => {
  if (!value) return false;
  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
};

const currentPywebviewApi = (): DiagnosticsApi | null => {
  if (!isBrowser()) return null;
  return window.pywebview?.api ?? null;
};

const trimBuffer = () => {
  if (buffer.length <= PERF_DIAGNOSTICS_BUFFER_LIMIT) return;
  buffer = buffer.slice(buffer.length - PERF_DIAGNOSTICS_BUFFER_LIMIT);
};

const serializeJsonl = (events: PerfEventRecord[]) =>
  events.map((entry) => JSON.stringify(entry)).join("\n");

const installGlobals = () => {
  if (!isBrowser() || globalsInstalled) return;
  globalsInstalled = true;
  window.MeeMeePerfDiagnostics = {
    enabled: () => isPerfDiagnosticsEnabled(),
    export: () => exportPerfDiagnostics(),
    clear: () => clearPerfDiagnostics(),
    openDir: () => openPerfDiagnosticsDir(),
    getEvents: () => [...buffer],
    enable: () => {
      window.localStorage.setItem(PERF_DIAGNOSTICS_FLAG_KEY, "1");
    },
    disable: () => {
      window.localStorage.removeItem(PERF_DIAGNOSTICS_FLAG_KEY);
    },
  } satisfies PerfDiagnosticsBridge;
};

export const isPerfDiagnosticsEnabled = () => {
  if (!isBrowser()) return false;
  try {
    return normalizeFlag(window.localStorage.getItem(PERF_DIAGNOSTICS_FLAG_KEY));
  } catch {
    return false;
  }
};

export const recordPerfEvent = (eventType: string, payload?: PerfEventPayload) => {
  if (!isPerfDiagnosticsEnabled()) return null;
  installGlobals();
  const event: PerfEventRecord = {
    seq: ++seq,
    time: new Date().toISOString(),
    timeMs: Date.now(),
    route: currentRoutePath(),
    eventType,
  };
  if (payload && Object.keys(payload).length > 0) {
    event.payload = payload;
  }
  buffer.push(event);
  trimBuffer();
  return event;
};

export const updatePerfDiagnosticsSnapshot = (partial: Partial<DiagnosticsSnapshot>) => {
  snapshot = {
    ...snapshot,
    ...partial,
  };
};

export const updatePerfDiagnosticsRoute = (route: string) => {
  const visibleCode = route.startsWith("/detail/") ? route.split("/").at(-1) ?? null : null;
  updatePerfDiagnosticsSnapshot({
    currentRoute: route,
    visibleCode,
  });
};

const buildExportPayload = () => {
  const now = new Date().toISOString();
  return {
    exportedAt: now,
    currentRoute: snapshot.currentRoute,
    visibleCode: snapshot.visibleCode,
    backendState: snapshot.backendState,
    lastApiError: snapshot.lastApiError,
    lastApiSuccess: snapshot.lastApiSuccess,
    files: [
      {
        name: `frontend-events-${Date.now()}.jsonl`,
        content: serializeJsonl(buffer),
      },
      {
        name: `frontend-state-${Date.now()}.json`,
        content: JSON.stringify(
          {
            exportedAt: now,
            snapshot,
            eventCount: buffer.length,
          },
          null,
          2
        ),
      },
    ],
  };
};

const downloadFallback = (filename: string, content: string, mimeType: string) => {
  if (!isBrowser()) return;
  const blob = new Blob([content], { type: mimeType });
  const url = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  window.URL.revokeObjectURL(url);
};

export const exportPerfDiagnostics = async () => {
  const payload = buildExportPayload();
  recordPerfEvent("diagnostics_export_requested", { eventCount: buffer.length });
  const api = currentPywebviewApi();
  if (api?.export_perf_diagnostics) {
    return await api.export_perf_diagnostics(payload);
  }
  downloadFallback(
    `frontend-events-${Date.now()}.jsonl`,
    serializeJsonl(buffer),
    "application/jsonl"
  );
  downloadFallback(
    `frontend-state-${Date.now()}.json`,
    JSON.stringify(
      {
        snapshot,
        eventCount: buffer.length,
      },
      null,
      2
    ),
    "application/json"
  );
  return { success: true, mode: "download" };
};

export const clearPerfDiagnostics = async () => {
  buffer = [];
  seq = 0;
  recordPerfEvent("diagnostics_cleared");
  const api = currentPywebviewApi();
  if (api?.clear_perf_diagnostics) {
    return await api.clear_perf_diagnostics();
  }
  return { success: true };
};

export const openPerfDiagnosticsDir = async () => {
  const api = currentPywebviewApi();
  if (api?.open_perf_diagnostics_dir) {
    return await api.open_perf_diagnostics_dir();
  }
  return false;
};

export const installPerfDiagnosticsMainThreadMonitor = () => {
  installGlobals();
  if (!isPerfDiagnosticsEnabled() || !isBrowser()) {
    return () => undefined;
  }
  if (mainThreadMonitorCleanup) return mainThreadMonitorCleanup;

  let active = true;
  let lastIntervalAt = performance.now();
  const intervalId = window.setInterval(() => {
    const now = performance.now();
    const lagMs = now - lastIntervalAt - EVENT_LOOP_LAG_INTERVAL_MS;
    if (lagMs > EVENT_LOOP_LAG_THRESHOLD_MS) {
      recordPerfEvent("main_thread_interval_lag", {
        lagMs: Number(lagMs.toFixed(1)),
      });
    }
    lastIntervalAt = now;
  }, EVENT_LOOP_LAG_INTERVAL_MS);

  let rafId = 0;
  let lastRafAt = performance.now();
  const tick = (time: number) => {
    if (!active) return;
    const gapMs = time - lastRafAt;
    if (gapMs > RAF_GAP_THRESHOLD_MS) {
      recordPerfEvent("main_thread_raf_gap", {
        gapMs: Number(gapMs.toFixed(1)),
      });
    }
    lastRafAt = time;
    rafId = window.requestAnimationFrame(tick);
  };
  rafId = window.requestAnimationFrame(tick);

  mainThreadMonitorCleanup = () => {
    active = false;
    window.clearInterval(intervalId);
    window.cancelAnimationFrame(rafId);
    mainThreadMonitorCleanup = null;
  };
  return mainThreadMonitorCleanup;
};

export const getPerfDiagnosticsEvents = () => [...buffer];

installGlobals();
