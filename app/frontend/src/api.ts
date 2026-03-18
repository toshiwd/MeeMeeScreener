import axios from "axios";
import type { AxiosError, AxiosRequestConfig } from "axios";
import type { ApiErrorInfo } from "./apiErrors";

let apiErrorReporter: ((info: ApiErrorInfo) => void) | null = null;

export const setApiErrorReporter = (reporter: (info: ApiErrorInfo) => void) => {
  apiErrorReporter = reporter;
};

// Add type definition for the injected config
declare global {
  interface Window {
    MEEMEE_API_BASE?: string;
  }
}

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

api.interceptors.response.use(
  (response) => {
    if (response.status >= 400) {
      const info: ApiErrorInfo = {
        url: resolveUrl(response.config ?? {}),
        method: (response.config?.method ?? "get").toUpperCase(),
        status: response.status ?? null,
        response: formatResponseBody(response.data),
        requestId: extractRequestId(response.headers, response.data),
        time: new Date().toISOString()
      };
      if (apiErrorReporter) {
        apiErrorReporter(info);
      }
    }
    return response;
  },
  (error: AxiosError) => {
    const config: AxiosRequestConfig = error.config ?? {};
    const response = error.response;
    const info: ApiErrorInfo = {
      url: resolveUrl(config),
      method: (config.method ?? "get").toUpperCase(),
      status: response?.status ?? null,
      response: response ? formatResponseBody(response.data) : error.message,
      requestId: extractRequestId(response?.headers, response?.data),
      time: new Date().toISOString()
    };
    if (apiErrorReporter) {
      apiErrorReporter(info);
    }
    return Promise.reject(error);
  }
);
