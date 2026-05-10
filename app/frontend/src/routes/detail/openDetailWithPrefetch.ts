import { DEFAULT_LIMITS } from "./detailHelpers";
import {
  hasCompleteDetailChartPrefetch,
  prefetchDetailChartFrames,
  prefetchDetailChartFramesBatch,
  readDetailChartPrefetchSync,
} from "./detailChartPrefetch";
import { recordPerfEvent } from "../../perfDiagnostics";
import {
  normalizeDetailBackPath,
  normalizeDetailListCodes,
  saveDetailListContext,
} from "./detailNavigationContext";

type NavigateFn = (to: string, options?: { state?: { from?: string } }) => void;

type OpenDetailWithPrefetchArgs = {
  navigate: NavigateFn;
  code: string;
  listCodes?: string[];
  backPath: string;
  asof?: string | null;
  backendReady?: boolean;
  prefetchWaitMs?: number;
  targetRoute?: "detail" | "detail-v2";
};

const DETAIL_NEIGHBOR_PREFETCH_RADIUS = 4;
let neighborPrefetchGeneration = 0;

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

export const openDetailWithPrefetch = async ({
  navigate,
  code,
  listCodes,
  backPath,
  asof,
  backendReady,
  prefetchWaitMs = 120,
  targetRoute = "detail",
}: OpenDetailWithPrefetchArgs) => {
  const currentNeighborPrefetchGeneration = ++neighborPrefetchGeneration;
  const resolvedBackPath = normalizeDetailBackPath(backPath);
  const resolvedListCodes = normalizeDetailListCodes(listCodes);
  saveDetailListContext(resolvedBackPath, resolvedListCodes);
  recordPerfEvent("detail_open_with_prefetch_start", {
    code,
    listCount: resolvedListCodes.length,
    backPath: resolvedBackPath,
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
    const neighborPrefetchRequests = neighborCodes.map((neighborCode) => ({
      code: neighborCode,
      dailyLimit: DEFAULT_LIMITS.daily,
      weeklyLimit: DEFAULT_LIMITS.weekly,
      monthlyLimit: DEFAULT_LIMITS.monthly,
      asof,
    }));
    const targetPrefetchPromise = prefetchDetailChartFrames(requestParams).catch(() => undefined);
    if (neighborPrefetchRequests.length) {
      targetPrefetchPromise.finally(() => {
        window.setTimeout(() => {
          if (currentNeighborPrefetchGeneration !== neighborPrefetchGeneration) return;
          void prefetchDetailChartFramesBatch(neighborPrefetchRequests).catch(() => undefined);
        }, 0);
      });
    }
    if (!hasTargetSeed && prefetchWaitMs > 0) {
      recordPerfEvent("detail_open_prefetch_wait", {
        code,
        waitBudgetMs: prefetchWaitMs,
        neighborCount: neighborCodes.length,
      });
      await Promise.race([targetPrefetchPromise, waitMs(prefetchWaitMs)]);
    }
  }
  const routePrefix = targetRoute === "detail-v2" ? "/detail-v2" : "/detail";
  navigate(`${routePrefix}/${code}`, { state: { from: resolvedBackPath } });
};
