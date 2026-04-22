export type BatchBarsRequestTimeframe = "daily" | "weekly" | "monthly";

export type BatchBarsRequestPayload = {
  codes: string[];
  timeframes: BatchBarsRequestTimeframe[];
  limit: number;
  timeframeLimits?: Partial<Record<BatchBarsRequestTimeframe, number>>;
  includeProvisional: boolean;
  includeBoxes?: boolean;
  asof?: string;
};

export type BatchBarsFramePayload = {
  bars?: number[][];
  boxes?: import("../../storeTypes").Box[];
  provenance?: import("../../storeTypes").ChartDataProvenance | null;
};

export type BatchBarsResponseMeta = {
  data_version?: string | null;
  fetched_at?: string | null;
};

export type BatchBarsV3Response = {
  items?: Record<
    string,
    Partial<Record<BatchBarsRequestTimeframe, BatchBarsFramePayload>>
  >;
  meta?: BatchBarsResponseMeta | null;
};

type Params = {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  includeBoxes?: boolean;
  asof?: string | null;
};

export const buildSingleBatchBarsRequestPayload = ({
  code,
  timeframe,
  limit,
  includeBoxes,
  asof,
}: Params): BatchBarsRequestPayload => {
  const payload: BatchBarsRequestPayload = {
    codes: [code],
    timeframes: [timeframe],
    limit,
    includeProvisional: true,
    includeBoxes: typeof includeBoxes === "boolean" ? includeBoxes : false,
  };
  if (typeof asof === "string" && asof.trim()) {
    payload.asof = asof.trim();
  }
  return payload;
};

type DetailParams = {
  code: string;
  dailyLimit: number;
  weeklyLimit: number;
  monthlyLimit: number;
  asof?: string | null;
};

type ScopedDetailParams = DetailParams & {
  timeframes?: BatchBarsRequestTimeframe[];
  includeBoxes?: boolean;
};

const DEFAULT_DETAIL_TIMEFRAMES: BatchBarsRequestTimeframe[] = ["daily", "weekly", "monthly"];

const normalizeDetailTimeframes = (timeframes?: BatchBarsRequestTimeframe[]) => {
  const candidate = Array.isArray(timeframes) && timeframes.length > 0 ? timeframes : DEFAULT_DETAIL_TIMEFRAMES;
  return Array.from(new Set(candidate.filter((timeframe): timeframe is BatchBarsRequestTimeframe =>
    timeframe === "daily" || timeframe === "weekly" || timeframe === "monthly"
  )));
};

export const buildScopedDetailBatchBarsRequestPayload = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
  timeframes,
  includeBoxes,
}: ScopedDetailParams): BatchBarsRequestPayload => {
  const resolvedTimeframes = normalizeDetailTimeframes(timeframes);
  const timeframeLimits = {
    daily: dailyLimit,
    weekly: weeklyLimit,
    monthly: monthlyLimit,
  } satisfies Partial<Record<BatchBarsRequestTimeframe, number>>;
  const resolvedLimit = Math.max(
    ...resolvedTimeframes.map((timeframe) => timeframeLimits[timeframe] ?? 0),
    0
  );
  const payload: BatchBarsRequestPayload = {
    codes: [code],
    timeframes: resolvedTimeframes,
    limit: resolvedLimit,
    timeframeLimits,
    includeProvisional: true,
    includeBoxes: typeof includeBoxes === "boolean" ? includeBoxes : true,
  };
  if (typeof asof === "string" && asof.trim()) {
    payload.asof = asof.trim();
  }
  return payload;
};

export const buildDetailBatchBarsRequestPayload = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
}: DetailParams): BatchBarsRequestPayload => {
  return buildScopedDetailBatchBarsRequestPayload({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
    timeframes: DEFAULT_DETAIL_TIMEFRAMES,
    includeBoxes: true,
  });
};

type DetailPrefetchParams = DetailParams & {
  includeBoxes?: boolean;
};

export const buildDetailPrefetchBatchBarsRequestPayload = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
  includeBoxes,
}: DetailPrefetchParams): BatchBarsRequestPayload => {
  return buildScopedDetailBatchBarsRequestPayload({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
    timeframes: DEFAULT_DETAIL_TIMEFRAMES,
    includeBoxes: typeof includeBoxes === "boolean" ? includeBoxes : false,
  });
};

export const postDetailBatchBarsRequest = async (
  payload: BatchBarsRequestPayload,
  signal?: AbortSignal
) => {
  const [{ api }, { BATCH_REQUEST_TIMEOUT_MS, BATCH_RETRY_DELAYS_MS, isRetriableBatchError, sleepMs }] =
    await Promise.all([import("../../api"), import("../../storeHelpers")]);
  let attempt = 0;
  while (true) {
    try {
      return await api.post("/batch_bars_v3", payload, {
        signal,
        timeout: BATCH_REQUEST_TIMEOUT_MS,
      });
    } catch (error) {
      const canRetry =
        attempt < BATCH_RETRY_DELAYS_MS.length && isRetriableBatchError(error);
      if (!canRetry) throw error;
      const retryDelay =
        BATCH_RETRY_DELAYS_MS[attempt] ??
        BATCH_RETRY_DELAYS_MS[BATCH_RETRY_DELAYS_MS.length - 1] ??
        0;
      attempt += 1;
      await sleepMs(retryDelay);
    }
  }
};
