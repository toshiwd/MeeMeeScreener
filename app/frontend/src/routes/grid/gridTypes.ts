import type { SortDir, SortKey } from "../../store";

export type Timeframe = "monthly" | "weekly" | "daily";

export type SortOption = { key: SortKey; label: string; fixedDirection?: SortDir };

export type SortSection = { title: string; options: SortOption[] };

export type BuyStateFilter = "all" | "initial" | "base";

export type HealthStatus = {
  txt_count: number;
  code_count?: number;
  last_updated: string | null;
  code_txt_missing: boolean;
  pan_out_txt_dir?: string | null;
};

export type JobStatusPayload = {
  id?: string;
  type?: string;
  status?: string;
  progress?: number | null;
  message?: string;
  error?: string | null;
};

export type TxtUpdateJobState = {
  id: string;
  status: string;
  progress?: number | null;
  message: string | null;
};

export type ToastAction = {
  label: string;
  onClick: () => void;
};

export type JobHistoryItem = {
  id?: string;
  type?: string;
  status?: string;
  message?: string | null;
};

export type WalkforwardSummary = {
  windows_total?: number;
  executed_windows?: number;
  failed_windows?: number;
  oos_trade_events?: number;
  oos_weighted_win_rate?: number | null;
  oos_total_realized_unit_pnl?: number;
  oos_worst_max_drawdown_unit?: number | null;
  oos_mean_profit_factor?: number | null;
  oos_positive_window_ratio?: number | null;
};

export type WalkforwardWindow = {
  index?: number;
  label?: string;
  status?: string;
  test?: {
    metrics?: {
      trade_events?: number;
      win_rate?: number | null;
      total_realized_unit_pnl?: number;
      max_drawdown_unit?: number | null;
      profit_factor?: number | null;
    };
  };
};

export type WalkforwardReport = {
  summary?: WalkforwardSummary;
  attribution?: {
    code?: WalkforwardAttributionBucket;
    sector33_code?: WalkforwardAttributionBucket;
    setup_id?: WalkforwardAttributionBucket;
    setup?: WalkforwardAttributionBucket;
    side?: WalkforwardAttributionBucket;
    hedge?: WalkforwardAttributionBucket;
  };
  windows?: WalkforwardWindow[];
};

export type WalkforwardAttributionRow = {
  key?: string;
  trades?: number;
  win_rate?: number | null;
  ret_net_sum?: number;
  avg_ret_net?: number | null;
  profit_factor?: number | null;
};

export type WalkforwardAttributionBucket = {
  rows?: WalkforwardAttributionRow[];
  top?: WalkforwardAttributionRow[];
  bottom?: WalkforwardAttributionRow[];
};

export type WalkforwardLatest = {
  run_id?: string;
  finished_at?: string;
  status?: string;
  config?: Record<string, unknown>;
  report?: WalkforwardReport;
};

export type WalkforwardResearchSetupRow = {
  setup_id?: string;
  trades?: number;
  ret_net_sum?: number;
  win_rate?: number | null;
  profit_factor?: number | null;
};

export type WalkforwardResearchRejectedRow = {
  reason?: string;
  count?: number;
};

export type WalkforwardResearchHedgeContribution = {
  core_ret_net_sum?: number;
  hedge_ret_net_sum?: number;
  total_ret_net_sum?: number;
  hedge_share?: number | null;
};

export type WalkforwardResearchReport = {
  summary?: WalkforwardSummary;
  adopted_setups?: WalkforwardResearchSetupRow[];
  rejected_reasons?: WalkforwardResearchRejectedRow[];
  hedge_contribution?: WalkforwardResearchHedgeContribution;
};

export type WalkforwardResearchLatest = {
  snapshot_date?: number;
  created_at?: string;
  source_run_id?: string;
  source_finished_at?: string;
  report?: WalkforwardResearchReport;
};

export type WalkforwardParams = {
  trainMonths: number;
  testMonths: number;
  stepMonths: number;
  minWindows: number;
  maxCodes: number;
  allowedSides: "both" | "long" | "short";
  minLongScore: number;
  minShortScore: number;
  maxNewEntriesPerDay: number;
  maxNewEntriesPerMonth: string;
  minMlPUpLong: string;
  useRegimeFilter: boolean;
  regimeBreadthLookbackDays: number;
  regimeLongMinBreadthAbove60: string;
  regimeShortMaxBreadthAbove60: string;
  allowedLongSetups: string;
  allowedShortSetups: string;
};

export type WalkforwardPreset = {
  name: string;
  params: WalkforwardParams;
  createdAt: string;
  updatedAt: string;
};
