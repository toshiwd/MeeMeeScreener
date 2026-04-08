import axios from "axios";
import type { AxiosError, AxiosRequestConfig } from "axios";
import type { ApiErrorInfo } from "./apiErrors";
import { recordPerfEvent, updatePerfDiagnosticsSnapshot } from "./perfDiagnostics";

let apiErrorReporter: ((info: ApiErrorInfo) => void) | null = null;
let apiSuccessReporter:
  | ((info: { url: string; method: string; status: number; time: string }) => void)
  | null = null;

export const setApiErrorReporter = (reporter: (info: ApiErrorInfo) => void) => {
  apiErrorReporter = reporter;
};

export const setApiSuccessReporter = (
  reporter: ((info: { url: string; method: string; status: number; time: string }) => void) | null
) => {
  apiSuccessReporter = reporter;
};

// Add type definition for the injected config
declare global {
  interface Window {
    MEEMEE_API_BASE?: string;
  }
}

type MeeMeeAxiosConfig = AxiosRequestConfig & {
  __meemeeRequestMeta?: {
    startedAt: number;
    requestId: string;
  };
};

const buildClientRequestId = () => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `req-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
};

export const api = axios.create({
  baseURL: window.MEEMEE_API_BASE || "/api",
  timeout: 15000
});

const resolveUrl = (config: AxiosRequestConfig) => {
  const base = config.baseURL ?? "";
  const url = config.url ?? "";
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (!base) return url;
  return `${base}${url}`;
};

const formatResponseBody = (value: unknown) => {
  if (value == null) return null;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
};

const extractRequestId = (
  headers: Record<string, unknown> | undefined,
  data: unknown
): string | null =>
  (headers?.["x-request-id"] as string | undefined) ||
  (headers?.["x-trace-id"] as string | undefined) ||
  (data as { trace_id?: string } | undefined)?.trace_id ||
  null;

api.interceptors.request.use((config) => {
  const nextConfig = config as MeeMeeAxiosConfig;
  const startedAt = performance.now();
  const requestId = buildClientRequestId();
  nextConfig.__meemeeRequestMeta = {
    startedAt,
    requestId,
  };
  recordPerfEvent("api_request_start", {
    url: resolveUrl(config),
    method: (config.method ?? "get").toUpperCase(),
    requestId,
  });
  return nextConfig;
});

api.interceptors.response.use(
  (response) => {
    const config = (response.config ?? {}) as MeeMeeAxiosConfig;
    const requestMeta = config.__meemeeRequestMeta;
    const requestId =
      extractRequestId(response.headers, response.data) ?? requestMeta?.requestId ?? null;
    const durationMs =
      requestMeta != null ? Number((performance.now() - requestMeta.startedAt).toFixed(1)) : null;
    if (response.status >= 400) {
      const info: ApiErrorInfo = {
        url: resolveUrl(response.config ?? {}),
        method: (response.config?.method ?? "get").toUpperCase(),
        status: response.status ?? null,
        response: formatResponseBody(response.data),
        requestId,
        time: new Date().toISOString()
      };
      updatePerfDiagnosticsSnapshot({ lastApiError: info });
      recordPerfEvent("api_request_failed", {
        url: info.url,
        method: info.method,
        status: info.status,
        requestId,
        durationMs,
      });
      if (apiErrorReporter) {
        apiErrorReporter(info);
      }
    } else if (apiSuccessReporter) {
      const successInfo = {
        url: resolveUrl(response.config ?? {}),
        method: (response.config?.method ?? "get").toUpperCase(),
        status: response.status ?? 0,
        time: new Date().toISOString()
      };
      updatePerfDiagnosticsSnapshot({ lastApiSuccess: successInfo });
      recordPerfEvent("api_request_end", {
        url: successInfo.url,
        method: successInfo.method,
        status: successInfo.status,
        requestId,
        durationMs,
      });
      apiSuccessReporter(successInfo);
    } else {
      recordPerfEvent("api_request_end", {
        url: resolveUrl(response.config ?? {}),
        method: (response.config?.method ?? "get").toUpperCase(),
        status: response.status ?? 0,
        requestId,
        durationMs,
      });
    }
    return response;
  },
  (error: AxiosError) => {
    const config: MeeMeeAxiosConfig = (error.config ?? {}) as MeeMeeAxiosConfig;
    const response = error.response;
    const requestMeta = config.__meemeeRequestMeta;
    const requestId =
      extractRequestId(response?.headers, response?.data) ?? requestMeta?.requestId ?? null;
    const durationMs =
      requestMeta != null ? Number((performance.now() - requestMeta.startedAt).toFixed(1)) : null;
    const info: ApiErrorInfo = {
      url: resolveUrl(config),
      method: (config.method ?? "get").toUpperCase(),
      status: response?.status ?? null,
      response: response ? formatResponseBody(response.data) : error.message,
      requestId,
      time: new Date().toISOString()
    };
    updatePerfDiagnosticsSnapshot({ lastApiError: info });
    recordPerfEvent("api_request_failed", {
      url: info.url,
      method: info.method,
      status: info.status,
      requestId,
      durationMs,
      message: error.message,
    });
    if (apiErrorReporter) {
      apiErrorReporter(info);
    }
    return Promise.reject(error);
  }
);
