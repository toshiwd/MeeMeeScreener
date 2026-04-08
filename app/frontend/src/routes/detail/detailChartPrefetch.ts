import type { Box } from "../../storeTypes";
import {
  applyChartDataVersion,
  getPersistentChartFrame,
  setPersistentChartFrame,
  subscribeToChartDataVersionChange,
} from "../../persistentChartCache";
import {
  buildDetailBatchBarsRequestPayload,
  postDetailBatchBarsRequest,
  type BatchBarsFramePayload,
  type BatchBarsRequestTimeframe,
  type BatchBarsV3Response,
} from "./batchBarsRequest";
import { recordPerfEvent } from "../../perfDiagnostics";

export type ChartPrefetchEntry = {
  rows: number[][];
  boxes: Box[];
  fetchedAt: number;
  dataVersion: string | null;
};

export type ChartPrefetchFrames = {
  daily: ChartPrefetchEntry | null;
  weekly: ChartPrefetchEntry | null;
  monthly: ChartPrefetchEntry | null;
};

type DetailChartPrefetchRequest = {
  code: string;
  dailyLimit: number;
  weeklyLimit: number;
  monthlyLimit: number;
  asof?: string | null;
};

type PrefetchOptions = {
  forceNetwork?: boolean;
  signal?: AbortSignal;
};

type DetailChartPrefetchBatchResult = Record<string, ChartPrefetchFrames>;

const CHART_PREFETCH_TTL_MS = 60_000;
const chartPrefetchCache = new Map<string, ChartPrefetchEntry>();
const chartPrefetchInFlight = new Map<string, Promise<ChartPrefetchFrames>>();
const chartPrefetchBatchInFlight = new Map<string, Promise<DetailChartPrefetchBatchResult>>();
const chartPrefetchPendingByKey = new Map<string, Promise<ChartPrefetchFrames>>();
const chartPrefetchLastNetworkAt = new Map<string, number>();

const normalizeAsof = (value?: string | null) =>
  typeof value === "string" && value.trim() ? value.trim() : null;

const emptyFramePayload = (): BatchBarsFramePayload => ({
  bars: [],
  boxes: [],
});

const buildChartPrefetchKey = (
  symbol: string,
  timeframe: BatchBarsRequestTimeframe,
  limit: number,
  includeBoxes: boolean,
  asof?: string | null
) => `${symbol}|${timeframe}|${limit}|boxes:${includeBoxes ? 1 : 0}|${normalizeAsof(asof) ?? ""}`;

export const buildDetailPrefetchKey = (
  symbol: string,
  dailyLimit: number,
  weeklyLimit: number,
  monthlyLimit: number,
  asof?: string | null
) => `detail|${symbol}|${dailyLimit}|${weeklyLimit}|${monthlyLimit}|${normalizeAsof(asof) ?? ""}`;

const registerPendingDetailPrefetch = (key: string, promise: Promise<ChartPrefetchFrames>) => {
  chartPrefetchPendingByKey.set(key, promise);
  promise.finally(() => {
    if (chartPrefetchPendingByKey.get(key) === promise) {
      chartPrefetchPendingByKey.delete(key);
    }
  });
  return promise;
};

const getPendingDetailPrefetch = (key: string) => chartPrefetchPendingByKey.get(key) ?? null;

const includeBoxesForTimeframe = (timeframe: BatchBarsRequestTimeframe) => timeframe === "monthly";

const readMemoryChartPrefetch = (
  symbol: string,
  timeframe: BatchBarsRequestTimeframe,
  limit: number,
  asof?: string | null
) => {
  const includeBoxes = includeBoxesForTimeframe(timeframe);
  const key = buildChartPrefetchKey(symbol, timeframe, limit, includeBoxes, asof);
  const cached = chartPrefetchCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.fetchedAt > CHART_PREFETCH_TTL_MS) {
    chartPrefetchCache.delete(key);
    return null;
  }
  return cached;
};

const writeMemoryChartPrefetch = ({
  code,
  timeframe,
  limit,
  asof,
  rows,
  boxes,
  fetchedAt,
  dataVersion,
}: {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  asof?: string | null;
  rows: number[][];
  boxes?: Box[];
  fetchedAt?: number;
  dataVersion?: string | null;
}) => {
  const includeBoxes = includeBoxesForTimeframe(timeframe);
  chartPrefetchCache.set(buildChartPrefetchKey(code, timeframe, limit, includeBoxes, asof), {
    rows,
    boxes: boxes ?? [],
    fetchedAt: fetchedAt ?? Date.now(),
    dataVersion: dataVersion ?? null,
  });
};

const persistFramePayload = async ({
  code,
  timeframe,
  limit,
  asof,
  payload,
  fetchedAt,
  dataVersion,
}: {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  asof?: string | null;
  payload: BatchBarsFramePayload;
  fetchedAt: number;
  dataVersion: string | null;
}) => {
  if (!dataVersion) return;
  await setPersistentChartFrame(
    {
      code,
      timeframe,
      limit,
      asof,
      includeBoxes: includeBoxesForTimeframe(timeframe),
      dataVersion,
    },
    {
      bars: Array.isArray(payload.bars) ? payload.bars : [],
      boxes: Array.isArray(payload.boxes) ? payload.boxes : [],
      fetchedAt,
      dataVersion,
    }
  );
};

const loadFrameFromPersistentCache = async ({
  code,
  timeframe,
  limit,
  asof,
}: {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  asof?: string | null;
}) => {
  const cached = await getPersistentChartFrame({
    code,
    timeframe,
    limit,
    asof,
    includeBoxes: includeBoxesForTimeframe(timeframe),
  });
  if (!cached) return null;
  recordPerfEvent("detail_chart_seed_hit", {
    code,
    source: "persistent",
    timeframe,
  });
  writeMemoryChartPrefetch({
    code,
    timeframe,
    limit,
    asof,
    rows: cached.bars,
    boxes: cached.boxes,
    fetchedAt: cached.fetchedAt,
    dataVersion: cached.dataVersion,
  });
  return readMemoryChartPrefetch(code, timeframe, limit, asof);
};

export const readDetailChartPrefetchSync = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
}: DetailChartPrefetchRequest): ChartPrefetchFrames => ({
  daily: readMemoryChartPrefetch(code, "daily", dailyLimit, asof),
  weekly: readMemoryChartPrefetch(code, "weekly", weeklyLimit, asof),
  monthly: readMemoryChartPrefetch(code, "monthly", monthlyLimit, asof),
});

export const loadDetailChartPrefetch = async ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
}: DetailChartPrefetchRequest): Promise<ChartPrefetchFrames> => {
  const memoryFrames = readDetailChartPrefetchSync({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
  });
  if (memoryFrames.daily && memoryFrames.weekly && memoryFrames.monthly) {
    recordPerfEvent("detail_chart_seed_hit", {
      code,
      source: "memory",
    });
    return memoryFrames;
  }
  const [daily, weekly, monthly] = await Promise.all([
    memoryFrames.daily ?? loadFrameFromPersistentCache({ code, timeframe: "daily", limit: dailyLimit, asof }),
    memoryFrames.weekly ?? loadFrameFromPersistentCache({ code, timeframe: "weekly", limit: weeklyLimit, asof }),
    memoryFrames.monthly ?? loadFrameFromPersistentCache({ code, timeframe: "monthly", limit: monthlyLimit, asof }),
  ]);
  return {
    daily,
    weekly,
    monthly,
  };
};

export const hasCompleteDetailChartPrefetch = (frames?: Partial<ChartPrefetchFrames>) =>
  frames?.daily != null && frames?.weekly != null && frames?.monthly != null;

export const extractDetailChartFrames = async ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
  response,
}: DetailChartPrefetchRequest & {
  response: BatchBarsV3Response | null;
}): Promise<ChartPrefetchFrames> => {
  const fetchedAt = Date.now();
  const items = response?.items ?? {};
  const item = items[code] ?? {};
  const dailyFrame = item.daily ?? emptyFramePayload();
  const weeklyFrame = item.weekly ?? emptyFramePayload();
  const monthlyFrame = item.monthly ?? emptyFramePayload();
  const dataVersion =
    typeof response?.meta?.data_version === "string" && response.meta.data_version.trim()
      ? response.meta.data_version.trim()
      : null;

  if (dataVersion) {
    await applyChartDataVersion(dataVersion);
  }
  recordPerfEvent("detail_chart_network_refresh", {
    code,
    dataVersion,
    hasDaily: Array.isArray(dailyFrame.bars) && dailyFrame.bars.length > 0,
    hasWeekly: Array.isArray(weeklyFrame.bars) && weeklyFrame.bars.length > 0,
    hasMonthly: Array.isArray(monthlyFrame.bars) && monthlyFrame.bars.length > 0,
  });

  writeMemoryChartPrefetch({
    code,
    timeframe: "daily",
    limit: dailyLimit,
    asof,
    rows: Array.isArray(dailyFrame.bars) ? dailyFrame.bars : [],
    boxes: [],
    fetchedAt,
    dataVersion,
  });
  writeMemoryChartPrefetch({
    code,
    timeframe: "weekly",
    limit: weeklyLimit,
    asof,
    rows: Array.isArray(weeklyFrame.bars) ? weeklyFrame.bars : [],
    boxes: [],
    fetchedAt,
    dataVersion,
  });
  writeMemoryChartPrefetch({
    code,
    timeframe: "monthly",
    limit: monthlyLimit,
    asof,
    rows: Array.isArray(monthlyFrame.bars) ? monthlyFrame.bars : [],
    boxes: Array.isArray(monthlyFrame.boxes) ? monthlyFrame.boxes : [],
    fetchedAt,
    dataVersion,
  });

  await Promise.all([
    persistFramePayload({
      code,
      timeframe: "daily",
      limit: dailyLimit,
      asof,
      payload: dailyFrame,
      fetchedAt,
      dataVersion,
    }),
    persistFramePayload({
      code,
      timeframe: "weekly",
      limit: weeklyLimit,
      asof,
      payload: weeklyFrame,
      fetchedAt,
      dataVersion,
    }),
    persistFramePayload({
      code,
      timeframe: "monthly",
      limit: monthlyLimit,
      asof,
      payload: monthlyFrame,
      fetchedAt,
      dataVersion,
    }),
  ]);

  return readDetailChartPrefetchSync({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
  });
};

export const prefetchDetailChartFrames = async (
  request: DetailChartPrefetchRequest,
  options?: PrefetchOptions
) => {
  const key = buildDetailPrefetchKey(
    request.code,
    request.dailyLimit,
    request.weeklyLimit,
    request.monthlyLimit,
    request.asof
  );
  const cached = await loadDetailChartPrefetch(request);
  const lastNetworkAt = chartPrefetchLastNetworkAt.get(key) ?? 0;
  if (
    hasCompleteDetailChartPrefetch(cached) &&
    options?.forceNetwork !== true &&
    Date.now() - lastNetworkAt < CHART_PREFETCH_TTL_MS
  ) {
    recordPerfEvent("detail_chart_seed_hit", {
      code: request.code,
      source: "cache",
    });
    return cached;
  }
  const pending = getPendingDetailPrefetch(key);
  if (pending) {
    recordPerfEvent("detail_chart_seed_hit", {
      code: request.code,
      source: "pending",
    });
    return await pending;
  }
  const inFlight = chartPrefetchInFlight.get(key);
  if (inFlight) {
    return await inFlight;
  }
  const requestPromise = registerPendingDetailPrefetch(key, postDetailBatchBarsRequest(
    buildDetailBatchBarsRequestPayload({
      code: request.code,
      dailyLimit: request.dailyLimit,
      weeklyLimit: request.weeklyLimit,
      monthlyLimit: request.monthlyLimit,
      asof: request.asof,
    }),
    options?.signal
  )
    .then((res) =>
      extractDetailChartFrames({
        ...request,
        response: res.data as BatchBarsV3Response | null,
      })
    )
    .finally(() => {
      chartPrefetchInFlight.delete(key);
    }));
  chartPrefetchInFlight.set(key, requestPromise);
  chartPrefetchLastNetworkAt.set(key, Date.now());
  return await requestPromise;
};

const buildBatchInFlightKey = (requests: DetailChartPrefetchRequest[]) =>
  requests
    .map((request) =>
      buildDetailPrefetchKey(
        request.code,
        request.dailyLimit,
        request.weeklyLimit,
        request.monthlyLimit,
        request.asof
      )
    )
    .sort()
    .join("||");

const shouldRefreshFromNetwork = (
  request: DetailChartPrefetchRequest,
  cached: ChartPrefetchFrames,
  forceNetwork?: boolean
) => {
  if (!hasCompleteDetailChartPrefetch(cached)) return true;
  if (forceNetwork === true) return true;
  const key = buildDetailPrefetchKey(
    request.code,
    request.dailyLimit,
    request.weeklyLimit,
    request.monthlyLimit,
    request.asof
  );
  const lastNetworkAt = chartPrefetchLastNetworkAt.get(key) ?? 0;
  return Date.now() - lastNetworkAt >= CHART_PREFETCH_TTL_MS;
};

const buildBatchPayload = (requests: DetailChartPrefetchRequest[]) => {
  const first = requests[0];
  const payload = {
    codes: requests.map((request) => request.code),
    timeframes: ["daily", "weekly", "monthly"] as BatchBarsRequestTimeframe[],
    limit: Math.max(first.dailyLimit, first.weeklyLimit, first.monthlyLimit),
    timeframeLimits: {
      daily: first.dailyLimit,
      weekly: first.weeklyLimit,
      monthly: first.monthlyLimit,
    },
    includeProvisional: true,
    includeBoxes: true,
  } as const;
  if (normalizeAsof(first.asof)) {
    return {
      ...payload,
      asof: normalizeAsof(first.asof) ?? undefined,
    };
  }
  return payload;
};

export const prefetchDetailChartFramesBatch = async (
  requests: DetailChartPrefetchRequest[],
  options?: PrefetchOptions
): Promise<DetailChartPrefetchBatchResult> => {
  const normalizedRequests = Array.from(
    new Map(
      requests
        .filter((request) => request.code)
        .map((request) => [
          buildDetailPrefetchKey(
            request.code,
            request.dailyLimit,
            request.weeklyLimit,
            request.monthlyLimit,
            request.asof
          ),
          request,
        ])
    ).values()
  );
  if (!normalizedRequests.length) return {};

  const cachedEntries = await Promise.all(
    normalizedRequests.map(async (request) => [request.code, await loadDetailChartPrefetch(request)] as const)
  );
  const result: DetailChartPrefetchBatchResult = Object.fromEntries(cachedEntries);
  const requestsNeedingNetwork = normalizedRequests.filter((request) =>
    shouldRefreshFromNetwork(request, result[request.code], options?.forceNetwork)
  );
  if (!requestsNeedingNetwork.length) {
    return result;
  }

  const waitingRequests = requestsNeedingNetwork.filter((request) =>
    Boolean(
      getPendingDetailPrefetch(
        buildDetailPrefetchKey(
          request.code,
          request.dailyLimit,
          request.weeklyLimit,
          request.monthlyLimit,
          request.asof
        )
      )
    )
  );
  const networkRequests = requestsNeedingNetwork.filter((request) =>
    !waitingRequests.includes(request)
  );
  if (!networkRequests.length) {
    const waitedEntries = await Promise.all(
      waitingRequests.map(async (request) => {
        const key = buildDetailPrefetchKey(
          request.code,
          request.dailyLimit,
          request.weeklyLimit,
          request.monthlyLimit,
          request.asof
        );
        const pending = getPendingDetailPrefetch(key);
        return [request.code, pending ? await pending : await loadDetailChartPrefetch(request)] as const;
      })
    );
    for (const [code, frames] of waitedEntries) {
      result[code] = frames;
    }
    return result;
  }

  const batchKey = buildBatchInFlightKey(networkRequests);
  const existing = chartPrefetchBatchInFlight.get(batchKey);
  if (existing) {
    const awaited = await existing;
    if (!waitingRequests.length) return awaited;
    const waitedEntries = await Promise.all(
      waitingRequests.map(async (request) => {
        const key = buildDetailPrefetchKey(
          request.code,
          request.dailyLimit,
          request.weeklyLimit,
          request.monthlyLimit,
          request.asof
        );
        const pending = getPendingDetailPrefetch(key);
        return [request.code, pending ? await pending : await loadDetailChartPrefetch(request)] as const;
      })
    );
    for (const [code, frames] of waitedEntries) {
      awaited[code] = frames;
    }
    return awaited;
  }

  for (const request of networkRequests) {
    chartPrefetchLastNetworkAt.set(
      buildDetailPrefetchKey(
        request.code,
        request.dailyLimit,
        request.weeklyLimit,
        request.monthlyLimit,
        request.asof
      ),
      Date.now()
    );
  }

  const requestPromise = postDetailBatchBarsRequest(buildBatchPayload(networkRequests), options?.signal)
    .then(async (response) => {
      const freshEntries = await Promise.all(
        networkRequests.map(async (request) => [
          request.code,
          await extractDetailChartFrames({
            ...request,
            response: response.data as BatchBarsV3Response | null,
          }),
        ] as const)
      );
      for (const [code, frames] of freshEntries) {
        result[code] = frames;
      }
      return result;
    })
    .finally(() => {
      chartPrefetchBatchInFlight.delete(batchKey);
    });

  for (const request of networkRequests) {
    const key = buildDetailPrefetchKey(
      request.code,
      request.dailyLimit,
      request.weeklyLimit,
      request.monthlyLimit,
      request.asof
    );
    registerPendingDetailPrefetch(
      key,
      requestPromise.then((entries) => entries[request.code] ?? loadDetailChartPrefetch(request))
    );
  }

  chartPrefetchBatchInFlight.set(batchKey, requestPromise);
  const freshResult = await requestPromise;
  if (!waitingRequests.length) {
    return freshResult;
  }
  const waitedEntries = await Promise.all(
    waitingRequests.map(async (request) => {
      const key = buildDetailPrefetchKey(
        request.code,
        request.dailyLimit,
        request.weeklyLimit,
        request.monthlyLimit,
        request.asof
      );
      const pending = getPendingDetailPrefetch(key);
      return [request.code, pending ? await pending : await loadDetailChartPrefetch(request)] as const;
    })
  );
  for (const [code, frames] of waitedEntries) {
    freshResult[code] = frames;
  }
  return freshResult;
};

export const clearDetailChartPrefetchCache = () => {
  chartPrefetchCache.clear();
  chartPrefetchLastNetworkAt.clear();
  chartPrefetchPendingByKey.clear();
};

subscribeToChartDataVersionChange(() => {
  clearDetailChartPrefetchCache();
});
