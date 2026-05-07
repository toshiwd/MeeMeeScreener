import type {
  ChartDataFreshnessArea,
  MeeMeeDataFreshnessContract,
} from "../../dataFreshnessContract";
import { getChartArea, normalizeMeeMeeDataFreshnessContract } from "../../dataFreshnessContract";
import type { FetchState, ParseStats, Timeframe } from "./detailTypes";

export type DetailChartLifecycleStatus =
  | "idle"
  | "loading"
  | "ready"
  | "empty"
  | "missing"
  | "error";

export type DetailChartLifecycleFrame = {
  timeframe: Timeframe;
  status: DetailChartLifecycleStatus;
  responseCount: number;
  candleCount: number;
  message: string | null;
  rightEdgeDate: string | null;
  classification: ChartDataFreshnessArea["classification"];
  freshnessState: ChartDataFreshnessArea["freshness_state"];
};

export type DetailChartLifecycleState = Record<Timeframe, DetailChartLifecycleFrame>;

type FrameInput = {
  timeframe: Timeframe;
  fetch: FetchState;
  errors: string[];
  candleCount: number;
  parseStats: ParseStats;
  loading: boolean;
  pendingSwap: boolean;
  contractArea: ChartDataFreshnessArea;
};

type BuildInput = {
  daily: Omit<FrameInput, "timeframe" | "contractArea">;
  weekly: Omit<FrameInput, "timeframe" | "contractArea">;
  monthly: Omit<FrameInput, "timeframe" | "contractArea">;
  contract?: MeeMeeDataFreshnessContract | null;
};

const parseErrorMessage = (parseStats: ParseStats) =>
  parseStats.parsed === 0 && parseStats.total > 0
    ? `Date parse failed ${parseStats.invalidTime}`
    : null;

export const resolveDetailChartLifecycleFrame = ({
  timeframe,
  fetch,
  errors,
  candleCount,
  parseStats,
  loading,
  pendingSwap,
  contractArea,
}: FrameInput): DetailChartLifecycleFrame => {
  const firstError = errors.find((item) => typeof item === "string" && item.trim()) ?? null;
  const parseError = parseErrorMessage(parseStats);
  let status: DetailChartLifecycleStatus = "idle";
  let message: string | null = null;

  if (loading || pendingSwap || fetch.status === "loading") {
    status = "loading";
    message = "Loading...";
  } else if (firstError) {
    status = "error";
    message = firstError;
  } else if (fetch.status === "error") {
    status = "error";
    message = fetch.errorMessage ?? "Chart data request failed";
  } else if (parseError) {
    status = "error";
    message = parseError;
  } else if (fetch.status === "success" && fetch.responseCount === 0) {
    status = "empty";
    message = "No data";
  } else if (contractArea.status === "error" && candleCount === 0) {
    status = "error";
    message = "Chart data contract error";
  } else if (contractArea.status === "missing" && candleCount === 0 && fetch.status !== "idle") {
    status = "missing";
    message = "Chart data unavailable";
  } else if (contractArea.status === "empty" && candleCount === 0 && fetch.status !== "idle") {
    status = "empty";
    message = "No data";
  } else if (fetch.status === "success") {
    status = "ready";
  }

  return {
    timeframe,
    status,
    responseCount: fetch.responseCount,
    candleCount,
    message,
    rightEdgeDate: contractArea.right_edge_date ?? null,
    classification: contractArea.classification,
    freshnessState: contractArea.freshness_state,
  };
};

export const buildDetailChartLifecycle = ({
  daily,
  weekly,
  monthly,
  contract,
}: BuildInput): DetailChartLifecycleState => {
  const normalized = normalizeMeeMeeDataFreshnessContract(contract);
  return {
    daily: resolveDetailChartLifecycleFrame({
      ...daily,
      timeframe: "daily",
      contractArea: getChartArea(normalized, "daily"),
    }),
    weekly: resolveDetailChartLifecycleFrame({
      ...weekly,
      timeframe: "weekly",
      contractArea: getChartArea(normalized, "weekly"),
    }),
    monthly: resolveDetailChartLifecycleFrame({
      ...monthly,
      timeframe: "monthly",
      contractArea: getChartArea(normalized, "monthly"),
    }),
  };
};

export const isDetailChartLifecycleSettled = (frame: DetailChartLifecycleFrame) =>
  frame.status === "ready" ||
  frame.status === "empty" ||
  frame.status === "missing" ||
  frame.status === "error";
