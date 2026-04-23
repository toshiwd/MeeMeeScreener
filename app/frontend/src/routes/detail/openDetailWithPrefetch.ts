import { DEFAULT_LIMITS } from "./detailHelpers";
import {
  hasCompleteDetailChartPrefetch,
  prefetchDetailChartFramesBatch,
  readDetailChartPrefetchSync,
} from "./detailChartPrefetch";
import { recordPerfEvent } from "../../perfDiagnostics";

type NavigateFn = (to: string, options?: { state?: { from?: string } }) => void;

type OpenDetailWithPrefetchArgs = {
  navigate: NavigateFn;
  code: string;
  listCodes?: string[];
  backPath: string;
  asof?: string | null;
  backendReady?: boolean;
  prefetchWaitMs?: number;
};

const DETAIL_NEIGHBOR_PREFETCH_RADIUS = 4;

const waitMs = (ms: number) =>
  new Promise<void>((resolve) => {
    window.setTimeout(resolve, ms);
  });

const buildNeighborCodes = (targetCode: string, listCodes: string[]) => {
  const index = listCodes.indexOf(targetCode);
  if (index < 0) return [];
  const neighbors: string[] = [];
  for (let offset = 1; offset <= DETAIL_NEIGHBOR_PREFETCH_RADIUS; offset += 1) {
    const previous = listCodes[index - offset] ?? null;
    const next = listCodes[index + offset] ?? null;
    if (previous) {
      neighbors.push(previous);
    }
    if (next) {
      neighbors.push(next);
    }
  }
  return neighbors;
};

export const saveDetailListContext = (backPath: string, listCodes: string[]) => {
  try {
    sessionStorage.setItem("detailListBack", backPath);
    sessionStorage.setItem("detailListCodes", JSON.stringify(listCodes));
  } catch {
    // ignore storage failures
  }
};

export const openDetailWithPrefetch = async ({
  navigate,
  code,
  listCodes,
  backPath,
  asof,
  backendReady,
  prefetchWaitMs = 120,
}: OpenDetailWithPrefetchArgs) => {
  const resolvedListCodes = Array.isArray(listCodes) ? listCodes : [];
  saveDetailListContext(backPath, resolvedListCodes);
  recordPerfEvent("detail_open_with_prefetch_start", {
    code,
    listCount: resolvedListCodes.length,
    backPath,
    asof: asof ?? null,
  });
  if (backendReady && code) {
    const requestParams = {
      code,
      dailyLimit: DEFAULT_LIMITS.daily,
      weeklyLimit: DEFAULT_LIMITS.weekly,
      monthlyLimit: DEFAULT_LIMITS.monthly,
      asof,
    };
    const hasTargetSeed = hasCompleteDetailChartPrefetch(readDetailChartPrefetchSync(requestParams));
    const neighborCodes = buildNeighborCodes(code, resolvedListCodes);
    const prefetchRequests = [
      requestParams,
      ...neighborCodes.map((neighborCode) => ({
        code: neighborCode,
        dailyLimit: DEFAULT_LIMITS.daily,
        weeklyLimit: DEFAULT_LIMITS.weekly,
        monthlyLimit: DEFAULT_LIMITS.monthly,
        asof,
      })),
    ];
    const prefetchPromise = prefetchDetailChartFramesBatch(prefetchRequests).catch(() => undefined);
    if (!hasTargetSeed && prefetchWaitMs > 0) {
      recordPerfEvent("detail_open_prefetch_wait", {
        code,
        waitBudgetMs: prefetchWaitMs,
        neighborCount: neighborCodes.length,
      });
      await Promise.race([prefetchPromise, waitMs(prefetchWaitMs)]);
    }
  }
  navigate(`/detail/${code}`, { state: { from: backPath } });
};
