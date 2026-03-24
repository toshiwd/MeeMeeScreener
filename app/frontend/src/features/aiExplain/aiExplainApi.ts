import { api } from "../../api";

export type AiExplainMode = "explain" | "compare" | "summarize";
export type AiExplainScreenType = "ranking" | "detail" | "compare";
export type AiExplainAnswerLength = "short" | "medium" | "long";

export type AiExplainError = {
  kind: string;
  message: string;
};

export type AiExplainSettings = {
  uiVisible: boolean;
  enabled: boolean;
  providerLabel: string;
  providerType: string;
  endpointUrl: string;
  model: string;
  credentialName: string;
  sendImages: boolean;
  answerLength: AiExplainAnswerLength;
  dailyLimit: number;
  compareEnabled: boolean;
  debugEnabled: boolean;
};

export type AiExplainSettingsState = {
  settings: AiExplainSettings;
  providerReady: boolean;
  credentialConfigured: boolean;
  canShowUi: boolean;
  canUse: boolean;
};

export type AiExplainSettingsDraft = AiExplainSettings & {
  authSecret?: string | null;
  clearAuthSecret?: boolean;
};

export type AiExplainRequest = {
  mode: AiExplainMode;
  screenType: AiExplainScreenType;
  userQuestion: string;
  snapshot: Record<string, unknown>;
  images?: string[];
  stream?: boolean;
};

export type AiExplainResponse = {
  answer: string;
  cached: boolean;
  provider: string;
  model: string;
  latencyMs: number;
  error: AiExplainError | null;
};

export type AiExplainStreamHandlers = {
  onDelta?: (delta: string) => void;
};

export const defaultAiExplainSettings = (): AiExplainSettings => ({
  uiVisible: false,
  enabled: false,
  providerLabel: "sakura",
  providerType: "openai_compatible",
  endpointUrl: "",
  model: "",
  credentialName: "sakura",
  sendImages: true,
  answerLength: "short",
  dailyLimit: 20,
  compareEnabled: true,
  debugEnabled: false,
});

export const loadAiExplainSettings = async (): Promise<AiExplainSettingsState> => {
  const res = await api.get("/ai-explain/settings");
  return res.data as AiExplainSettingsState;
};

export const saveAiExplainSettings = async (
  settings: AiExplainSettingsDraft
): Promise<AiExplainSettingsState> => {
  const res = await api.put("/ai-explain/settings", settings);
  return res.data as AiExplainSettingsState;
};

export const requestAiExplain = async (
  request: AiExplainRequest,
  signal?: AbortSignal
): Promise<AiExplainResponse> => {
  const res = await api.post("/ai-explain", request, { signal });
  return res.data as AiExplainResponse;
};

const resolveAiExplainUrl = (path: string) => {
  const base = api.defaults.baseURL ?? "";
  if (/^https?:\/\//i.test(path)) return path;
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (!base) return normalizedPath;
  if (/^https?:\/\//i.test(base)) {
    const normalizedBase = base.endsWith("/") ? base : `${base}/`;
    return new URL(normalizedPath.replace(/^\//, ""), normalizedBase).toString();
  }
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  return `${normalizedBase}${normalizedPath}`;
};

type SseEvent = {
  event?: string;
  data?: string;
};

const parseSseFrame = (frame: string): SseEvent | null => {
  const lines = frame.split(/\r?\n/);
  const event: SseEvent = {};
  const dataLines: string[] = [];
  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    if (!line || line.startsWith(":")) continue;
    if (line.startsWith("event:")) {
      event.event = line.slice(6).trim();
      continue;
    }
    if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).trimStart());
      continue;
    }
  }
  if (dataLines.length === 0) return event.event ? event : null;
  event.data = dataLines.join("\n");
  return event;
};

export const streamAiExplain = async (
  request: AiExplainRequest,
  signal?: AbortSignal,
  handlers?: AiExplainStreamHandlers
): Promise<AiExplainResponse> => {
  const url = resolveAiExplainUrl("/ai-explain");
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
    },
    body: JSON.stringify({ ...request, stream: true }),
    signal,
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `ai_explain_stream_http_${response.status}`);
  }
  if (!response.body) {
    throw new Error("ai_explain_stream_body_missing");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  const finalize = (payload: unknown): AiExplainResponse => payload as AiExplainResponse;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");

      while (true) {
        const separatorIndex = buffer.indexOf("\n\n");
        if (separatorIndex < 0) break;
        const frame = buffer.slice(0, separatorIndex);
        buffer = buffer.slice(separatorIndex + 2);
        const event = parseSseFrame(frame);
        if (!event) continue;
        if (!event.data) continue;
        const parsed = JSON.parse(event.data) as AiExplainResponse & { type?: string; delta?: string };
        if ((parsed.type ?? event.event) === "delta") {
          if (typeof parsed.delta === "string" && parsed.delta) {
            handlers?.onDelta?.(parsed.delta);
          }
          continue;
        }
        return finalize(parsed);
      }
    }
  } finally {
    reader.releaseLock();
  }

  throw new Error("ai_explain_stream_ended_unexpectedly");
};
