export type DataFreshnessClassification =
  | "confirmed"
  | "provisional"
  | "mixed"
  | "missing"
  | "research-only";

export type DataFreshnessState = "fresh" | "stale" | "missing" | "error";

export type DataFreshnessStatus = "ready" | "loading" | "empty" | "missing" | "error";

export type DataFreshnessTimeframe = "daily" | "weekly" | "monthly";

export type DataFreshnessArea = {
  source: string | null;
  source_path_or_adapter: string | null;
  classification: DataFreshnessClassification;
  status: DataFreshnessStatus;
};

export type RankingDataFreshnessArea = DataFreshnessArea & {
  snapshot_as_of: string | null;
  snapshot_id: string | null;
  freshness_state: DataFreshnessState;
};

export type ChartDataFreshnessArea = DataFreshnessArea & {
  right_edge_date: string | null;
  freshness_state: DataFreshnessState;
};

export type MeeMeeDataFreshnessContract = {
  contract_version: "meemee_data_freshness_v1" | "unknown";
  generated_at: string | null;
  ranking: RankingDataFreshnessArea;
  detail: DataFreshnessArea;
  charts: Record<DataFreshnessTimeframe, ChartDataFreshnessArea>;
  research: {
    normal_ui_exposure_allowed: boolean;
    classification: "research-only" | "missing";
  };
};

export type DataFreshnessBadgeTone = "ok" | "warn" | "muted" | "error";

export type DataFreshnessBadgeItem = {
  key: string;
  label: string;
  tone: DataFreshnessBadgeTone;
};

const TIMEFRAMES: DataFreshnessTimeframe[] = ["daily", "weekly", "monthly"];

const CLASSIFICATIONS = new Set<DataFreshnessClassification>([
  "confirmed",
  "provisional",
  "mixed",
  "missing",
  "research-only",
]);

const FRESHNESS_STATES = new Set<DataFreshnessState>(["fresh", "stale", "missing", "error"]);

const STATUSES = new Set<DataFreshnessStatus>(["ready", "loading", "empty", "missing", "error"]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value != null && typeof value === "object" && !Array.isArray(value);

const textOrNull = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text ? text : null;
};

const normalizeClassification = (value: unknown): DataFreshnessClassification => {
  const text = textOrNull(value);
  return text != null && CLASSIFICATIONS.has(text as DataFreshnessClassification)
    ? (text as DataFreshnessClassification)
    : "missing";
};

const normalizeFreshnessState = (value: unknown): DataFreshnessState => {
  const text = textOrNull(value);
  return text != null && FRESHNESS_STATES.has(text as DataFreshnessState)
    ? (text as DataFreshnessState)
    : "missing";
};

const normalizeStatus = (value: unknown): DataFreshnessStatus => {
  const text = textOrNull(value);
  return text != null && STATUSES.has(text as DataFreshnessStatus)
    ? (text as DataFreshnessStatus)
    : "missing";
};

const emptyArea = (): DataFreshnessArea => ({
  source: null,
  source_path_or_adapter: null,
  classification: "missing",
  status: "missing",
});

const emptyChartArea = (): ChartDataFreshnessArea => ({
  ...emptyArea(),
  right_edge_date: null,
  freshness_state: "missing",
});

export const emptyMeeMeeDataFreshnessContract = (): MeeMeeDataFreshnessContract => ({
  contract_version: "unknown",
  generated_at: null,
  ranking: {
    ...emptyArea(),
    snapshot_as_of: null,
    snapshot_id: null,
    freshness_state: "missing",
  },
  detail: emptyArea(),
  charts: {
    daily: emptyChartArea(),
    weekly: emptyChartArea(),
    monthly: emptyChartArea(),
  },
  research: {
    normal_ui_exposure_allowed: false,
    classification: "research-only",
  },
});

const normalizeArea = (value: unknown): DataFreshnessArea => {
  const source = isRecord(value) ? value : {};
  return {
    source: textOrNull(source.source),
    source_path_or_adapter: textOrNull(source.source_path_or_adapter),
    classification: normalizeClassification(source.classification),
    status: normalizeStatus(source.status),
  };
};

const normalizeRankingArea = (value: unknown): RankingDataFreshnessArea => {
  const source = isRecord(value) ? value : {};
  return {
    ...normalizeArea(source),
    snapshot_as_of: textOrNull(source.snapshot_as_of),
    snapshot_id: textOrNull(source.snapshot_id),
    freshness_state: normalizeFreshnessState(source.freshness_state),
  };
};

const normalizeChartArea = (value: unknown): ChartDataFreshnessArea => {
  const source = isRecord(value) ? value : {};
  return {
    ...normalizeArea(source),
    right_edge_date: textOrNull(source.right_edge_date),
    freshness_state: normalizeFreshnessState(source.freshness_state),
  };
};

export const normalizeMeeMeeDataFreshnessContract = (
  value: unknown
): MeeMeeDataFreshnessContract => {
  const source = isRecord(value) ? value : {};
  const charts = isRecord(source.charts) ? source.charts : {};
  const research = isRecord(source.research) ? source.research : {};
  return {
    contract_version:
      source.contract_version === "meemee_data_freshness_v1"
        ? "meemee_data_freshness_v1"
        : "unknown",
    generated_at: textOrNull(source.generated_at),
    ranking: normalizeRankingArea(source.ranking),
    detail: normalizeArea(source.detail),
    charts: {
      daily: normalizeChartArea(charts.daily),
      weekly: normalizeChartArea(charts.weekly),
      monthly: normalizeChartArea(charts.monthly),
    },
    research: {
      normal_ui_exposure_allowed: research.normal_ui_exposure_allowed === true,
      classification:
        research.classification === "research-only" ? "research-only" : "missing",
    },
  };
};

export const classificationTone = (
  classification: DataFreshnessClassification
): DataFreshnessBadgeTone => {
  if (classification === "confirmed") return "ok";
  if (classification === "provisional" || classification === "mixed") return "warn";
  if (classification === "research-only") return "error";
  return "muted";
};

const stateTone = (state: DataFreshnessState | DataFreshnessStatus): DataFreshnessBadgeTone => {
  if (state === "fresh" || state === "ready") return "ok";
  if (state === "stale" || state === "loading") return "warn";
  if (state === "error") return "error";
  return "muted";
};

export const classificationLabel = (classification: DataFreshnessClassification) => {
  if (classification === "confirmed") return "確定";
  if (classification === "provisional") return "暫定";
  if (classification === "mixed") return "一部暫定";
  if (classification === "research-only") return "研究用";
  return "未確認";
};

const freshnessLabel = (state: DataFreshnessState) => {
  if (state === "fresh") return "最新";
  if (state === "stale") return "更新確認";
  if (state === "error") return "確認エラー";
  return "未確認";
};

const statusLabel = (status: DataFreshnessStatus) => {
  if (status === "ready") return "表示可";
  if (status === "loading") return "読込中";
  if (status === "empty") return "データなし";
  if (status === "error") return "確認エラー";
  return "未確認";
};

export const buildDataFreshnessBadgeItems = (
  area: DataFreshnessArea | RankingDataFreshnessArea | ChartDataFreshnessArea
): DataFreshnessBadgeItem[] => {
  const items: DataFreshnessBadgeItem[] = [
    {
      key: "classification",
      label: classificationLabel(area.classification),
      tone: classificationTone(area.classification),
    },
    {
      key: "status",
      label: statusLabel(area.status),
      tone: stateTone(area.status),
    },
  ];
  if ("freshness_state" in area) {
    items.push({
      key: "freshness",
      label: freshnessLabel(area.freshness_state),
      tone: stateTone(area.freshness_state),
    });
  }
  if ("right_edge_date" in area && area.right_edge_date) {
    items.push({
      key: "right-edge",
      label: `右端 ${area.right_edge_date}`,
      tone: "muted",
    });
  }
  if ("snapshot_as_of" in area && area.snapshot_as_of) {
    items.push({
      key: "snapshot",
      label: `基準日 ${area.snapshot_as_of}`,
      tone: "muted",
    });
  }
  return items;
};

export const shouldShowResearchOnlyWarning = (
  area: DataFreshnessArea | RankingDataFreshnessArea | ChartDataFreshnessArea
) => area.classification === "research-only";

export const getChartArea = (
  contract: MeeMeeDataFreshnessContract,
  timeframe: DataFreshnessTimeframe
) => contract.charts[TIMEFRAMES.includes(timeframe) ? timeframe : "daily"];
