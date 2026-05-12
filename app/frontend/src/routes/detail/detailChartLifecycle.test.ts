import { describe, expect, it } from "vitest";
import {
  buildDetailChartLifecycle,
  isDetailChartLifecycleSettled,
  resolveDetailChartLifecycleFrame,
} from "./detailChartLifecycle";
import type { FetchState, ParseStats } from "./detailTypes";

const parseStats = (overrides: Partial<ParseStats> = {}): ParseStats => ({
  total: 0,
  parsed: 0,
  invalidRow: 0,
  invalidTime: 0,
  invalidValue: 0,
  ...overrides,
});

const fetchState = (overrides: Partial<FetchState> = {}): FetchState => ({
  status: "idle",
  responseCount: 0,
  errorMessage: null,
  ...overrides,
});

const baseFrame = {
  timeframe: "daily" as const,
  fetch: fetchState(),
  errors: [],
  candleCount: 0,
  parseStats: parseStats(),
  loading: false,
  pendingSwap: false,
  contractArea: {
    source: null,
    source_path_or_adapter: null,
    classification: "missing" as const,
    status: "missing" as const,
    freshness_state: "missing" as const,
    right_edge_date: null,
  },
};

describe("detailChartLifecycle", () => {
  it("resolves explicit empty responses without leaving loading behind", () => {
    const frame = resolveDetailChartLifecycleFrame({
      ...baseFrame,
      fetch: fetchState({ status: "success", responseCount: 0 }),
    });

    expect(frame.status).toBe("empty");
    expect(frame.message).toBe("表示できるデータがありません");
    expect(isDetailChartLifecycleSettled(frame)).toBe(true);
  });

  it("keeps contract right-edge and classification as display metadata", () => {
    const lifecycle = buildDetailChartLifecycle({
      daily: {
        fetch: fetchState({ status: "success", responseCount: 2 }),
        errors: [],
        candleCount: 2,
        parseStats: parseStats({ total: 2, parsed: 2 }),
        loading: false,
        pendingSwap: false,
      },
      weekly: {
        fetch: fetchState({ status: "success", responseCount: 1 }),
        errors: [],
        candleCount: 1,
        parseStats: parseStats({ total: 1, parsed: 1 }),
        loading: false,
        pendingSwap: false,
      },
      monthly: {
        fetch: fetchState({ status: "success", responseCount: 1 }),
        errors: [],
        candleCount: 1,
        parseStats: parseStats({ total: 1, parsed: 1 }),
        loading: false,
        pendingSwap: false,
      },
      contract: {
        contract_version: "meemee_data_freshness_v1",
        generated_at: "2026-05-05T00:00:00Z",
        ranking: {
          source: "rankings",
          source_path_or_adapter: null,
          classification: "confirmed",
          status: "ready",
          snapshot_as_of: "2026-05-01",
          snapshot_id: "snapshot",
          freshness_state: "fresh",
        },
        detail: {
          source: "batch_bars_v3",
          source_path_or_adapter: null,
          classification: "confirmed",
          status: "ready",
        },
        charts: {
          daily: {
            source: "daily",
            source_path_or_adapter: null,
            classification: "confirmed",
            status: "ready",
            freshness_state: "fresh",
            right_edge_date: "2026-05-01",
          },
          weekly: {
            source: "weekly",
            source_path_or_adapter: null,
            classification: "mixed",
            status: "ready",
            freshness_state: "stale",
            right_edge_date: "2026-04-27",
          },
          monthly: {
            source: "monthly",
            source_path_or_adapter: null,
            classification: "provisional",
            status: "ready",
            freshness_state: "fresh",
            right_edge_date: "2026-05-01",
          },
        },
        research: {
          normal_ui_exposure_allowed: false,
          classification: "research-only",
        },
      },
    });

    expect(lifecycle.daily.status).toBe("ready");
    expect(lifecycle.daily.rightEdgeDate).toBe("2026-05-01");
    expect(lifecycle.weekly.classification).toBe("mixed");
    expect(lifecycle.weekly.freshnessState).toBe("stale");
    expect(lifecycle.monthly.classification).toBe("provisional");
  });

  it("marks parse failures as explicit errors", () => {
    const frame = resolveDetailChartLifecycleFrame({
      ...baseFrame,
      fetch: fetchState({ status: "success", responseCount: 2 }),
      candleCount: 0,
      parseStats: parseStats({ total: 2, parsed: 0, invalidTime: 2 }),
    });

    expect(frame.status).toBe("error");
    expect(frame.message).toBe("日付を読み取れないデータがあります (2件)");
  });

  it("keeps cached candles visible while a refresh is still loading", () => {
    const frame = resolveDetailChartLifecycleFrame({
      ...baseFrame,
      fetch: fetchState({ status: "loading", responseCount: 120 }),
      candleCount: 120,
      parseStats: parseStats({ total: 120, parsed: 120 }),
      loading: true,
      pendingSwap: true,
    });

    expect(frame.status).toBe("ready");
    expect(frame.message).toBeNull();
  });
});
