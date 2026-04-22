import { captureWindowBlob, saveBlobToPerfDiagnostics } from "./windowScreenshot";
import {
  exportPerfDiagnostics,
  getPerfDiagnosticsEvents,
  recordPerfEvent,
  updatePerfDiagnosticsSnapshot,
} from "../perfDiagnostics";

type FatalSource =
  | "error-boundary"
  | "window-error"
  | "unhandledrejection"
  | "boot-window-error"
  | "boot-unhandledrejection"
  | string;

type FatalDiagnosticsInput = {
  source: FatalSource;
  message?: string;
  stack?: string | null;
  componentStack?: string | null;
  reason?: unknown;
  error?: Error | null;
  href?: string;
  route?: string;
  timestamp?: string;
};

type FatalDiagnosticsSummary = {
  bundleId: string;
  source: string;
  message: string;
  route: string;
  href: string;
  timestamp: string;
  stack: string | null;
  componentStack: string | null;
  reasonText: string | null;
  userAgent: string;
  eventCount: number;
  screenshot: {
    success: boolean;
    fileName?: string;
    savedPath?: string;
    savedDir?: string;
    error?: string;
  } | null;
};

type FatalDiagnosticsCaptureResult = {
  success: boolean;
  bundleId?: string;
  deduped?: boolean;
  summary?: FatalDiagnosticsSummary;
  screenshot?: FatalDiagnosticsSummary["screenshot"];
  exportResult?: unknown;
  error?: string;
};

type FatalDiagnosticsBridge = {
  enabled: () => boolean;
  install: () => void;
  report: (input: FatalDiagnosticsInput) => Promise<FatalDiagnosticsCaptureResult>;
  flushQueued: () => Promise<void>;
};

const FATAL_DIAGNOSTICS_QUEUE_KEY = "__meemeeFatalDiagnosticsQueue";
const FATAL_DIAGNOSTICS_REPORTER_KEY = "__meemeeReportFatalDiagnostics";
const FATAL_DIAGNOSTICS_DEDUPE_WINDOW_MS = 10_000;

let bridgeInstalled = false;
let reporterSeq = 0;
const recentFingerprints = new Map<string, number>();

const isBrowser = () => typeof window !== "undefined";

const normalizeText = (value: unknown): string => {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (value instanceof Error) {
    return value.message || value.name || "Error";
  }
  if (typeof value === "object") {
    const maybeError = value as { message?: unknown; name?: unknown };
    if (typeof maybeError.message === "string" && maybeError.message.trim()) {
      return maybeError.message;
    }
    if (typeof maybeError.name === "string" && maybeError.name.trim()) {
      return maybeError.name;
    }
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const normalizeStack = (value: unknown): string | null => {
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  return null;
};

const normalizeError = (input: FatalDiagnosticsInput) => {
  const reasonValue = input.reason ?? input.error ?? null;
  const reasonText = reasonValue == null ? null : normalizeText(reasonValue);
  const error = input.error ?? (reasonValue instanceof Error ? reasonValue : null);
  const message =
    input.message?.trim() ||
    error?.message?.trim() ||
    reasonText?.trim() ||
    "Unknown fatal error";
  const stack = normalizeStack(input.stack ?? error?.stack ?? (reasonValue as { stack?: unknown } | null)?.stack);
  const componentStack = normalizeStack(input.componentStack);
  const route = input.route?.trim() || window.location?.pathname || "/";
  const href = input.href?.trim() || window.location?.href || "";
  const timestamp = input.timestamp?.trim() || new Date().toISOString();
  return {
    source: input.source,
    message,
    stack,
    componentStack,
    reasonText,
    route,
    href,
    timestamp,
  };
};

const buildFingerprint = (summary: ReturnType<typeof normalizeError>) => {
  const stackHead = summary.stack?.split("\n", 1)[0] ?? "";
  const componentHead = summary.componentStack?.split("\n", 1)[0] ?? "";
  return [
    summary.source,
    summary.route,
    summary.message,
    stackHead,
    componentHead,
    summary.reasonText ?? "",
  ].join("|");
};

const shouldCapture = (fingerprint: string) => {
  const now = Date.now();
  for (const [key, lastAt] of recentFingerprints.entries()) {
    if (now - lastAt > FATAL_DIAGNOSTICS_DEDUPE_WINDOW_MS) {
      recentFingerprints.delete(key);
    }
  }
  const lastAt = recentFingerprints.get(fingerprint);
  if (lastAt != null && now - lastAt < FATAL_DIAGNOSTICS_DEDUPE_WINDOW_MS) {
    return false;
  }
  recentFingerprints.set(fingerprint, now);
  return true;
};

const buildBundleId = () => {
  reporterSeq += 1;
  const stamp = Date.now();
  return `fatal-${stamp}-${reporterSeq.toString().padStart(3, "0")}`;
};

const buildScreenshotName = (bundleId: string) => `frontend-fatal-${bundleId}.png`;

const buildManifestName = (bundleId: string) => `frontend-fatal-${bundleId}.json`;

const sanitizeCode = (route: string) =>
  route.replace(/[^a-zA-Z0-9/_-]+/g, "_").replace(/[\/]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 48) ||
  "root";

const getQueue = (): FatalDiagnosticsInput[] => {
  if (!isBrowser()) return [];
  const queue = (window as Window & Record<string, unknown>)[FATAL_DIAGNOSTICS_QUEUE_KEY];
  return Array.isArray(queue) ? (queue as FatalDiagnosticsInput[]) : [];
};

const setQueue = (items: FatalDiagnosticsInput[]) => {
  if (!isBrowser()) return;
  (window as Window & Record<string, unknown>)[FATAL_DIAGNOSTICS_QUEUE_KEY] = items;
};

const setReporter = (reporter: (input: FatalDiagnosticsInput) => void) => {
  if (!isBrowser()) return;
  (window as Window & Record<string, unknown>)[FATAL_DIAGNOSTICS_REPORTER_KEY] = reporter;
};

const currentReporter = () => {
  if (!isBrowser()) return null;
  const reporter = (window as Window & Record<string, unknown>)[FATAL_DIAGNOSTICS_REPORTER_KEY];
  return typeof reporter === "function" ? (reporter as (input: FatalDiagnosticsInput) => void) : null;
};

export const reportFrontendFatalDiagnostics = async (
  input: FatalDiagnosticsInput
): Promise<FatalDiagnosticsCaptureResult> => {
  if (!isBrowser()) {
    return { success: false, error: "Browser context not available" };
  }

  const summaryBase = normalizeError(input);
  const fingerprint = buildFingerprint(summaryBase);
  if (!shouldCapture(fingerprint)) {
    return { success: true, deduped: true };
  }

  const bundleId = buildBundleId();
  const screenshotName = buildScreenshotName(bundleId);
  const manifestName = buildManifestName(bundleId);
  const eventCount = getPerfDiagnosticsEvents().length;

  updatePerfDiagnosticsSnapshot({
    currentRoute: summaryBase.route,
    lastApiError: {
      kind: "frontend_fatal",
      source: summaryBase.source,
      message: summaryBase.message,
      bundleId,
    },
  });
  recordPerfEvent("frontend_fatal_diagnostics_start", {
    bundleId,
    source: summaryBase.source,
    route: summaryBase.route,
  });

  let screenshot: FatalDiagnosticsSummary["screenshot"] = null;
  try {
    const capture = await captureWindowBlob({
      screenType: "Fatal",
      code: sanitizeCode(summaryBase.route),
    });
    if (capture.success && capture.blob) {
      const saveResult = await saveBlobToPerfDiagnostics(capture.blob, screenshotName);
      screenshot = {
        success: Boolean(saveResult.success),
        fileName: saveResult.fileName ?? screenshotName,
        savedPath: saveResult.savedPath,
        savedDir: saveResult.savedDir,
        error: saveResult.error,
      };
    } else {
      screenshot = {
        success: false,
        fileName: screenshotName,
        error: capture.error,
      };
    }
  } catch (error) {
    screenshot = {
      success: false,
      fileName: screenshotName,
      error: normalizeText(error),
    };
  }

  const summary: FatalDiagnosticsSummary = {
    bundleId,
    source: summaryBase.source,
    message: summaryBase.message,
    route: summaryBase.route,
    href: summaryBase.href,
    timestamp: summaryBase.timestamp,
    stack: summaryBase.stack,
    componentStack: summaryBase.componentStack,
    reasonText: summaryBase.reasonText,
    userAgent: navigator.userAgent,
    eventCount,
    screenshot,
  };

  const exportResult = await exportPerfDiagnostics({
    files: [
      {
        name: manifestName,
        content: JSON.stringify(summary, null, 2),
      },
    ],
  });

  recordPerfEvent("frontend_fatal_diagnostics_complete", {
    bundleId,
    source: summary.source,
    route: summary.route,
    screenshotSaved: Boolean(screenshot?.success),
  });

  return {
    success: true,
    bundleId,
    summary,
    screenshot,
    exportResult,
  };
};

export const installFrontendFatalDiagnosticsBridge = (): FatalDiagnosticsBridge => {
  if (!isBrowser()) {
    return {
      enabled: () => false,
      install: () => undefined,
      report: reportFrontendFatalDiagnostics,
      flushQueued: async () => undefined,
    };
  }

  if (bridgeInstalled) {
    return {
      enabled: () => true,
      install: () => undefined,
      report: reportFrontendFatalDiagnostics,
      flushQueued: async () => undefined,
    };
  }

  bridgeInstalled = true;
  const initialQueue = getQueue();
  setQueue([]);
  setReporter((input) => {
    void reportFrontendFatalDiagnostics(input);
  });

  const queued = [...initialQueue];
  void Promise.allSettled(queued.map((input) => reportFrontendFatalDiagnostics(input)));

  return {
    enabled: () => true,
    install: () => undefined,
    report: reportFrontendFatalDiagnostics,
    flushQueued: async () => undefined,
  };
};

export const queueFrontendFatalDiagnostics = (input: FatalDiagnosticsInput) => {
  if (!isBrowser()) return;
  const reporter = currentReporter();
  if (reporter) {
    reporter(input);
    return;
  }
  setQueue([...getQueue(), input]);
};
