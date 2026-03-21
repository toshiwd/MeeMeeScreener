import { attachOperatorConsoleHeader } from "../utils/operatorConsole";

type AnyRecord = Record<string, unknown>;

const parseJson = (text: string): unknown => {
  if (!text) return null;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
};

export async function tradexFetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = attachOperatorConsoleHeader(init?.headers);
  if (init?.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(url, { ...init, headers });
  const payload = parseJson(await response.text());
  if (!response.ok) {
    const reason = payload && typeof payload === "object" ? (payload as AnyRecord).reason : null;
    const detail = payload && typeof payload === "object" ? (payload as AnyRecord).detail : null;
    const message = typeof detail === "string" ? detail : typeof reason === "string" ? reason : `${response.status} ${response.statusText}`;
    throw new Error(message);
  }
  return payload as T;
}

export async function tradexFetchJsonWithRetry<T>(url: string, init?: RequestInit, retries = 1): Promise<T> {
  let attempt = 0;
  let lastError: unknown = null;
  while (attempt <= retries) {
    try {
      return await tradexFetchJson<T>(url, init);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message.toLowerCase() : "";
      const retryable = message.includes("503") || message.includes("temporary") || message.includes("retry") || message.includes("locked");
      if (attempt >= retries || !retryable) throw error;
      await new Promise((resolve) => window.setTimeout(resolve, 250 * (attempt + 1)));
    }
    attempt += 1;
  }
  throw lastError instanceof Error ? lastError : new Error("request failed");
}

