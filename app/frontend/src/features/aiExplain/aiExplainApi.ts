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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === "object" && !Array.isArray(value);

const boolValue = (value: unknown, fallback: boolean) =>
  typeof value === "boolean" ? value : fallback;

const textValue = (value: unknown, fallback: string) =>
  typeof value === "string" ? value : fallback;

const numberValue = (value: unknown, fallback: number) =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;

const answerLengthValue = (value: unknown, fallback: AiExplainAnswerLength): AiExplainAnswerLength =>
  value === "short" || value === "medium" || value === "long" ? value : fallback;

const normalizeAiExplainSettingsState = (payload: unknown): AiExplainSettingsState => {
  const root = isRecord(payload) ? payload : {};
  const rawSettings = isRecord(root.settings) ? root.settings : {};
  const defaults = defaultAiExplainSettings();
  const settings: AiExplainSettings = {
    uiVisible: boolValue(rawSettings.uiVisible, defaults.uiVisible),
    enabled: boolValue(rawSettings.enabled, defaults.enabled),
    providerLabel: textValue(rawSettings.providerLabel, defaults.providerLabel),
    providerType: textValue(rawSettings.providerType, defaults.providerType),
    endpointUrl: textValue(rawSettings.endpointUrl, defaults.endpointUrl),
    model: textValue(rawSettings.model, defaults.model),
    credentialName: textValue(rawSettings.credentialName, defaults.credentialName),
    sendImages: boolValue(rawSettings.sendImages, defaults.sendImages),
    answerLength: answerLengthValue(rawSettings.answerLength, defaults.answerLength),
    dailyLimit: numberValue(rawSettings.dailyLimit, defaults.dailyLimit),
    compareEnabled: boolValue(rawSettings.compareEnabled, defaults.compareEnabled),
    debugEnabled: boolValue(rawSettings.debugEnabled, defaults.debugEnabled),
  };
  return {
    settings,
    providerReady: boolValue(root.providerReady, false),
    credentialConfigured: boolValue(root.credentialConfigured, false),
    canShowUi: boolValue(root.canShowUi, false),
    canUse: boolValue(root.canUse, false),
  };
};

export const loadAiExplainSettings = async (): Promise<AiExplainSettingsState> => {
  const res = await api.get("/ai-explain/settings");
  return normalizeAiExplainSettingsState(res.data);
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
  const handleEvent = (event: SseEvent | null): AiExplainResponse | null => {
    if (!event?.data) return null;
    const parsed = JSON.parse(event.data) as AiExplainResponse & { type?: string; delta?: string };
    if ((parsed.type ?? event.event) === "delta") {
      if (typeof parsed.delta === "string" && parsed.delta) {
        handlers?.onDelta?.(parsed.delta);
      }
      return null;
    }
    return finalize(parsed);
  };

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
        const result = handleEvent(parseSseFrame(frame));
        if (result) return result;
      }
    }
    const trailingFrame = buffer.trim();
    if (trailingFrame) {
      const result = handleEvent(parseSseFrame(trailingFrame));
      if (result) return result;
    }
  } finally {
    reader.releaseLock();
  }

  throw new Error("ai_explain_stream_ended_unexpectedly");
};
