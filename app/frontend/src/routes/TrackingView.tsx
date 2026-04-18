import { useEffect, useMemo, useState } from "react";
import type { CSSProperties, ReactNode } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { api } from "../api";
import TopNav from "../components/TopNav";
import { formatIsoDateLabel } from "../utils/dateLabels";
import {
  formatTradeStrengthCaption,
  formatTradeStrengthPoints,
  tradeStrengthToneClass,
} from "../utils/tradeStrength";

type TrackingStatus = "active" | "completed" | "archive";
type TrackingSide = "buy" | "sell";
type TrackingMode = "ranking" | "signal" | "analysis";
type RankingDirection = "up" | "down";
type TrackingOutcomeFilter = "all" | "good" | "bad" | "broken";
type TrackingListSort = "recent" | "oldest" | "best" | "worst";

type ParsedTrackingQuery = {
  view: TrackingMode;
  status: TrackingStatus;
  side: TrackingSide;
  dir: RankingDirection;
  q: string;
  logicVersion: string;
  rankingLogicVersion: string;
  rankBucket: string;
  from: string;
  to: string;
  outcome: TrackingOutcomeFilter;
  sort: TrackingListSort;
};

type TrackingListResponse<T> = {
  items?: T[];
  count?: number;
  has_more?: boolean;
  next_offset?: number | null;
  offset?: number;
  limit?: number;
  outcome?: TrackingOutcomeFilter;
  sort?: TrackingListSort;
};

type SignalLogicVersionItem = {
  logic_version: string;
  basis_version: string;
  label: string | null;
  is_active: boolean;
};

type RankingLogicVersionItem = {
  ranking_logic_version: string;
  basis_version: string;
  label: string | null;
  is_active: boolean;
};

type SignalSummaryResponse = {
  active_count: number;
  completed_count: number;
  archive_count: number;
  active_average_directional_return: number | null;
  completed_win_rate: number | null;
  duplicate_signal_rate: number | null;
  event_summary?: {
    active_count: number;
    completed_count: number;
    archive_count: number;
    active_average_directional_return: number | null;
    completed_win_rate: number | null;
    break_rate: number | null;
  } | null;
};

type RankingSummaryResponse = {
  active_count: number;
  completed_count: number;
  archive_count: number;
  active_average_directional_return: number | null;
  completed_win_rate: number | null;
  break_rate: number | null;
  entry_qualified_rate: number | null;
  signal_present_rate: number | null;
};

type SignalValidationBreakReasonRow = {
  break_reason: string;
  count: number;
  evaluated_count?: number;
  average_directional_return_30?: number | null;
  directional_win_rate?: number | null;
};

type PeakDayBucketRow = {
  bucket?: string | null;
  label?: string | null;
  day_bucket?: string | null;
  count: number;
  days_to_max_favorable_30?: number | null;
  days_to_max_adverse_30?: number | null;
  median_days_to_max_favorable_30?: number | null;
  median_days_to_max_adverse_30?: number | null;
  average_directional_return_30?: number | null;
};

type ProfitTimingPatternRow = {
  bucket: string;
  count: number;
  share: number | null;
  directional_hit_rate_10?: number | null;
  directional_hit_rate_20?: number | null;
  directional_hit_rate_30?: number | null;
  average_directional_return_10?: number | null;
  average_directional_return_20?: number | null;
  average_directional_return_30?: number | null;
};

type ScoreThresholdRow = {
  score_key: string;
  threshold: number;
  count: number;
  coverage_rate: number | null;
  directional_hit_rate_30: number | null;
  average_directional_return_30: number | null;
  same_date_universe_average_directional_return_30?: number | null;
  lift_vs_same_date_universe_30?: number | null;
};

type SellSubsetComparisonRow = {
  subset_key: string;
  label: string;
  count: number;
  campaign_count?: number | null;
  directional_hit_rate?: number | null;
  average_directional_return?: number | null;
  same_date_universe_average_directional_return?: number | null;
  lift_vs_same_date_universe?: number | null;
  break_rate?: number | null;
  median_days_to_max_favorable_30?: number | null;
  median_days_to_max_adverse_30?: number | null;
};

type ShockAnalysisCohortRow = {
  cohort_key: string;
  label: string;
  count: number;
  share: number | null;
  directional_hit_rate_30: number | null;
  average_directional_return_30: number | null;
  same_date_universe_average_directional_return_30: number | null;
  lift_vs_same_date_universe_30: number | null;
  average_trailing_return_20: number | null;
  median_trailing_return_20: number | null;
};

type ShockAnalysisGroupRow = {
  setup_type?: string;
  regime_tag?: string;
  break_reason?: string;
  count: number;
  directional_hit_rate_30: number | null;
  average_directional_return_30: number | null;
  average_lift_vs_universe_30?: number | null;
};

type ShockAnalysisExampleRow = {
  dt: number;
  code: string;
  name?: string | null;
  setup_type?: string | null;
  regime_tag?: string | null;
  trailing_return_20?: number | null;
  return_30?: number | null;
  lift_vs_universe_30?: number | null;
  break_status?: string | null;
  break_reason?: string | null;
};

type ShockAnalysisSideSummary = {
  side: TrackingSide;
  window: {
    from_ymd: number;
    to_ymd: number;
    lookback_years: number;
    trailing_horizon: number;
    drop_threshold: number;
    bottom_decile_threshold: number | null;
  };
  qualified_decisions: number;
  qualified_with_trailing_return: number;
  cohort_rows: ShockAnalysisCohortRow[];
  by_setup_type: ShockAnalysisGroupRow[];
  by_regime: ShockAnalysisGroupRow[];
  by_break_reason: ShockAnalysisGroupRow[];
  shock_examples: ShockAnalysisExampleRow[];
};

type SignalValidationSideSummary = {
  total_decisions: number;
  qualified_decisions: number;
  qualified_directional_hit_rate_5: number | null;
  qualified_directional_hit_rate_10?: number | null;
  qualified_directional_hit_rate_20: number | null;
  qualified_directional_hit_rate_30: number | null;
  qualified_directional_hit_rate_60: number | null;
  average_directional_return_5: number | null;
  average_directional_return_10?: number | null;
  average_directional_return_20: number | null;
  average_directional_return_30: number | null;
  average_directional_return_60: number | null;
  score_threshold_rows?: ScoreThresholdRow[];
  shock_analysis?: ShockAnalysisSideSummary | null;
  days_to_max_favorable_30?: number | null;
  days_to_max_adverse_30?: number | null;
  median_days_to_max_favorable_30?: number | null;
  median_days_to_max_adverse_30?: number | null;
  same_date_universe_average_directional_return_10?: number | null;
  same_date_universe_average_directional_return_30?: number | null;
  lift_vs_same_date_universe_10?: number | null;
  lift_vs_same_date_universe_30?: number | null;
  peak_day_buckets?: PeakDayBucketRow[];
  profit_timing_patterns?: ProfitTimingPatternRow[];
  by_setup_type?: {
    setup_type: string;
    qualified_decisions: number;
    directional_hit_rate_10?: number | null;
    directional_hit_rate_20: number | null;
    directional_hit_rate_30: number | null;
    average_directional_return_10?: number | null;
    average_directional_return_20: number | null;
    average_directional_return_30: number | null;
  }[];
  by_break_reason?: SignalValidationBreakReasonRow[];
  by_regime?: {
    regime: string;
    qualified_decisions: number;
    directional_hit_rate_10?: number | null;
    directional_hit_rate_30: number | null;
    average_directional_return_10?: number | null;
    average_directional_return_30: number | null;
    same_date_universe_average_directional_return_10?: number | null;
    same_date_universe_average_directional_return_30?: number | null;
    lift_vs_same_date_universe_10?: number | null;
    lift_vs_same_date_universe_30?: number | null;
  }[];
  monthly?: {
    month: string;
    qualified_decisions: number;
    directional_hit_rate_10?: number | null;
    directional_hit_rate_30: number | null;
    average_directional_return_10?: number | null;
    average_directional_return_30: number | null;
    same_date_universe_average_directional_return_10?: number | null;
    same_date_universe_average_directional_return_30?: number | null;
    lift_vs_same_date_universe_10?: number | null;
    lift_vs_same_date_universe_30?: number | null;
  }[];
  rolling_6m?: {
    month: string;
    window_size: number;
    average_directional_return_30: number | null;
  }[];
  failure_examples?: {
    event_id: string;
    code: string;
    name: string | null;
    signal_date: string | null;
    break_reason: string | null;
    return_30d: number | null;
    setup_type?: string | null;
    max_adverse_30?: number | null;
    max_favorable_30?: number | null;
  }[];
};

type SignalValidationResponse = {
  generated_at?: string;
  side: TrackingSide;
  logic_version: string;
  primary_horizon?: number;
  decision_level: SignalValidationSideSummary;
  campaign_level: {
    total_campaigns: number;
    active_count: number;
    completed_count: number;
    archive_count: number;
    evaluated_count: number;
    evaluated_directional_win_rate: number | null;
    average_final_directional_return: number | null;
    average_max_favorable_return: number | null;
    average_max_adverse_return: number | null;
    active_average_directional_return: number | null;
    duplicate_signal_rate: number | null;
    by_signal_count?: {
      bucket: string;
      campaign_count: number;
      evaluated_count: number;
      directional_win_rate: number | null;
      average_final_directional_return: number | null;
    }[];
    by_setup_type?: {
      setup_type: string;
      campaign_count: number;
      evaluated_count: number;
      directional_win_rate: number | null;
      average_final_directional_return: number | null;
    }[];
    by_break_reason?: SignalValidationBreakReasonRow[];
  };
  ranking_level?: {
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
    days_to_max_favorable_30?: number | null;
    days_to_max_adverse_30?: number | null;
    median_days_to_max_favorable_30?: number | null;
    median_days_to_max_adverse_30?: number | null;
    peak_day_buckets?: PeakDayBucketRow[];
    same_date_universe_average_directional_return_30?: number | null;
    lift_vs_same_date_universe_30?: number | null;
  };
  summary: {
    total_campaigns: number;
    active_count: number;
    completed_count: number;
    archive_count: number;
    evaluated_count: number;
    evaluated_directional_win_rate: number | null;
    average_final_directional_return: number | null;
    average_max_favorable_return: number | null;
    average_max_adverse_return: number | null;
    active_average_directional_return: number | null;
    duplicate_signal_rate: number | null;
  };
  sell_subset_comparison?: {
    version: number;
    side: TrackingSide;
    primary_horizon: number;
    universe: SellSubsetComparisonRow;
    subsets: SellSubsetComparisonRow[];
  } | null;
};

type MetricComparison<T = number | null> = {
  base: T;
  target: T;
  delta: number | null;
};

type SignalComparisonResponse = {
  generated_at?: string;
  side: TrackingSide;
  primary_horizon: number;
  base_logic_version: string;
  target_logic_version: string;
  decision: {
    qualified_decisions: MetricComparison<number | null>;
    directional_hit_rate: MetricComparison<number | null>;
    average_directional_return: MetricComparison<number | null>;
    lift_vs_same_date_universe: MetricComparison<number | null>;
    median_days_to_max_favorable_30: MetricComparison<number | null>;
    median_days_to_max_adverse_30: MetricComparison<number | null>;
  };
  campaign: {
    total_campaigns: MetricComparison<number | null>;
    evaluated_directional_win_rate: MetricComparison<number | null>;
    average_final_directional_return: MetricComparison<number | null>;
    duplicate_signal_rate: MetricComparison<number | null>;
  };
};

type RankingAnalysisResponse = {
  generated_at?: string;
  ranking_logic_version: string;
  by_dir: {
    dir: RankingDirection;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
    days_to_max_favorable_30?: number | null;
    days_to_max_adverse_30?: number | null;
    median_days_to_max_favorable_30?: number | null;
    median_days_to_max_adverse_30?: number | null;
  }[];
  by_rank_bucket: {
    bucket: string;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
  }[];
  by_entry_qualified: {
    entry_qualified: boolean;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
  }[];
  by_signal_state: {
    signal_state: string;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
  }[];
  monthly?: {
    month: string;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
    same_date_universe_average_directional_return_30?: number | null;
    lift_vs_same_date_universe_30?: number | null;
  }[];
  rolling_6m?: {
    month: string;
    window_size: number;
    average_directional_return_30: number | null;
  }[];
  by_regime?: {
    regime: string;
    count: number;
    average_directional_return_30: number | null;
    directional_win_rate_30: number | null;
    same_date_universe_average_directional_return_30?: number | null;
    lift_vs_same_date_universe_30?: number | null;
  }[];
  same_date_universe_average_directional_return_30?: number | null;
  lift_vs_same_date_universe_30?: number | null;
  peak_day_buckets?: PeakDayBucketRow[];
  break_reason_counts: {
    break_reason: string;
    count: number;
  }[];
};

type LeakageAuditResponse = {
  generated_at?: string;
  logic_version: string;
  basis_version: string;
  basis_provenance: {
    total_rows: number;
    missing_source_as_of_count: number;
    missing_model_version_count: number;
    missing_basis_source_count: number;
    missing_source_hash_count: number;
    missing_payload_schema_version_count: number;
    future_source_as_of_count: number;
    future_pred_dt_count: number;
    prohibited_payload_count: number;
    excluded_from_validation_count: number;
  };
  latest_signal_parity?: {
    available: boolean;
    dt?: number | null;
    reason?: string | null;
    per_side?: {
      side: TrackingSide;
      dt: number;
      compared_codes: number;
      qualified_match_rate: number | null;
      setup_match_rate: number | null;
      pred_dt?: number | null;
      model_version?: string | null;
    }[];
    mismatch_samples?: {
      dt: number;
      side: TrackingSide;
      code: string;
      expected_entry_qualified: boolean;
      actual_entry_qualified: boolean;
      expected_setup_type: string | null;
      actual_setup_type: string | null;
    }[];
  };
  label_policy_audit?: {
    available: boolean;
    path?: string;
    reason?: string;
    tables?: {
      table: string;
      horizon: number;
      total_rows: number;
      missing_policy_version: number;
      missing_leakage_group_id: number;
      missing_purge_end_date: number;
      missing_embargo_until_date: number;
      policy_versions: string[];
    }[];
  };
  external_replay_audit?: {
    available: boolean;
    reason?: string;
    latest_replay?: {
      replay_id: string;
      status: string;
      start_as_of_date: string;
      end_as_of_date: string;
      finished_at?: string | null;
    } | null;
    sample_parity?: {
      publish_id: string;
      as_of_date: string;
      candidate_count_sampled: number;
      qualified_overlap_rate: number | null;
      setup_parity_available: boolean;
    } | null;
  };
};

type TrackingRuntimeStatus = {
  ok: boolean;
  resolved_data_dir: string;
  resolved_stocks_db_path: string;
  signal_occurrence_count: number;
  signal_decision_count: number;
  signal_latest_date: number | null;
  signal_latest_date_iso: string | null;
  signal_history_generated: boolean;
  ranking_appearance_count: number;
  ranking_latest_date: number | null;
  ranking_latest_date_iso: string | null;
  ranking_history_generated: boolean;
};

type SignalEvent = {
  event_id: string;
  campaign_id: string | null;
  code: string;
  name: string | null;
  side: TrackingSide;
  signalDate: string | null;
  setup_type: string | null;
  anchor_price_close: number | null;
  anchor_price_next_open: number | null;
  current_directional_return: number | null;
  current_exec_directional_return: number | null;
  return_30d: number | null;
  max_favorable_30: number | null;
  max_adverse_30: number | null;
  status: TrackingStatus;
  break_status: "alive" | "broken" | "completed_clean" | null;
  break_reason: string | null;
  reason_summary: string[];
  priority_score?: number | null;
};

type SignalEventDetail = {
  event: SignalEvent & {
    logic_version: string;
    reason_snapshot?: Record<string, unknown> | null;
    score_snapshot?: Record<string, unknown> | null;
  };
  occurrences?: {
    occurrence_id: string;
    signalDate: string | null;
    is_additional?: boolean;
  }[];
  price_series?: {
    date_iso: string | null;
    close: number | null;
    return_close_basis: number | null;
    return_exec_basis: number | null;
  }[];
};

type RankingAppearance = {
  appearance_id: string;
  date_iso: string | null;
  dir: RankingDirection;
  rank: number;
  code: string;
  name: string | null;
  signal_state_at_appearance: "buy" | "sell" | "wait" | "both";
  display_score: number | null;
  setup_type_at_appearance: string | null;
  anchor_price_close: number | null;
  anchor_price_next_open: number | null;
  current_directional_return: number | null;
  return_30d: number | null;
  max_favorable_30: number | null;
  max_adverse_30: number | null;
  status: TrackingStatus;
  break_status: "alive" | "broken" | "completed_clean" | null;
  break_reason: string | null;
};

type RankingAppearanceDetail = {
  appearance: RankingAppearance & {
    ranking_logic_version: string;
    signal_logic_version: string;
    payload?: Record<string, unknown> | null;
  };
  price_series?: {
    date_iso: string | null;
    close: number | null;
    return_close_basis: number | null;
    return_exec_basis: number | null;
  }[];
};

const STATUS_TABS: { key: TrackingStatus; label: string }[] = [
  { key: "active", label: "Active" },
  { key: "completed", label: "Completed" },
  { key: "archive", label: "Archive" },
];

const _LEGACY_MODE_TABS: { key: TrackingMode; label: string }[] = [
  { key: "ranking", label: "ランキング掲載履歴" },
  { key: "signal", label: "売買判定履歴" },
];

const MODE_TABS: { key: TrackingMode; label: string }[] = [
  { key: "ranking", label: "ランキング掲載履歴" },
  { key: "signal", label: "売買判定履歴" },
  { key: "analysis", label: "分析" },
];

const SIDE_TABS: { key: TrackingSide; label: string }[] = [
  { key: "buy", label: "買い" },
  { key: "sell", label: "売り" },
];

const DIR_TABS: { key: RankingDirection; label: string }[] = [
  { key: "up", label: "上昇側" },
  { key: "down", label: "下落側" },
];

const TRACKING_OUTCOME_OPTIONS: { value: TrackingOutcomeFilter; label: string }[] = [
  { value: "all", label: "全件" },
  { value: "good", label: "良かった" },
  { value: "bad", label: "不調" },
  { value: "broken", label: "シナリオ崩れ" },
];

const TRACKING_SORT_OPTIONS: { value: TrackingListSort; label: string }[] = [
  { value: "recent", label: "新しい順" },
  { value: "oldest", label: "古い順" },
  { value: "best", label: "良い順" },
  { value: "worst", label: "悪い順" },
];

const TRACKING_PAGE_SIZE = 100;

const RANK_BUCKETS = [
  { value: "", label: "全順位" },
  { value: "1-5", label: "1-5位" },
  { value: "6-10", label: "6-10位" },
  { value: "11-20", label: "11-20位" },
  { value: "21-50", label: "21-50位" },
];

const formatPercent = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const percent = value * 100;
  if (Math.abs(percent) < 0.05) return "0.0%";
  return `${percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
};

const formatPrice = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", { maximumFractionDigits: 2 });
};

const formatSignalStateLabel = (value: "buy" | "sell" | "wait" | "both") => {
  if (value === "sell") return "売り";
  if (value === "both") return "両方";
  if (value === "wait") return "待機";
  return "買い";
};

const metricTone = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "is-flat";
  if (value > 0) return "is-up";
  if (value < 0) return "is-down";
  return "is-flat";
};

const formatBreakLabel = (status: string | null | undefined, reason: string | null | undefined) => {
  if (status === "broken") return reason ? `崩れ ${reason}` : "崩れ";
  if (status === "completed_clean") return "完走";
  return "継続中";
};

const summarizeReasons = (reasons: string[] | null | undefined) => {
  const values = Array.isArray(reasons) ? reasons.filter(Boolean) : [];
  if (!values.length) return "--";
  return values.slice(0, 2).join(" / ");
};

const formatSignedPercent = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const percent = value * 100;
  if (Math.abs(percent) < 0.05) return "0.0%";
  return `${percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
};

const formatPlainNumber = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", { maximumFractionDigits: 0 });
};

const formatSignedNumber = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${value > 0 ? "+" : ""}${value.toLocaleString("ja-JP", { maximumFractionDigits: 0 })}`;
};

const formatDayCount = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const rounded = Math.round(value * 10) / 10;
  return Number.isInteger(rounded) ? `${rounded.toFixed(0)}日目` : `${rounded.toFixed(1)}日目`;
};

const formatDateInputYmd = (value: Date) => {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const date = `${value.getDate()}`.padStart(2, "0");
  return `${year}${month}${date}`;
};

const parseDateInputYmd = (value: string) => {
  const normalized = value.trim();
  if (!/^\d{8}$/.test(normalized)) return null;
  const year = Number.parseInt(normalized.slice(0, 4), 10);
  const month = Number.parseInt(normalized.slice(4, 6), 10) - 1;
  const date = Number.parseInt(normalized.slice(6, 8), 10);
  const parsed = new Date(year, month, date);
  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getFullYear() !== year ||
    parsed.getMonth() !== month ||
    parsed.getDate() !== date
  ) {
    return null;
  }
  return parsed;
};

const formatYmdDisplay = (value: number | string | null | undefined) => {
  if (value === null || value === undefined) return "--";
  const parsed = parseDateInputYmd(String(value).trim());
  if (!parsed) return String(value);
  return formatDateInputYmd(parsed);
};

const buildYearsPresetRange = (endYmd: string, years: number) => {
  const endDate = parseDateInputYmd(endYmd) ?? new Date();
  const startDate = new Date(endDate);
  startDate.setFullYear(startDate.getFullYear() - years);
  return {
    from: formatDateInputYmd(startDate),
    to: formatDateInputYmd(endDate),
  };
};

const appendUniqueItems = <T,>(current: T[], incoming: T[], keyOf: (item: T) => string) => {
  if (!current.length) return incoming;
  const seen = new Set(current.map((item) => keyOf(item)));
  const merged = [...current];
  for (const item of incoming) {
    const key = keyOf(item);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(item);
  }
  return merged;
};

const countFromPageFallback = <T,>(items: T[], offset: number) => offset + items.length;

const _diffRate = (left: number | null | undefined, right: number | null | undefined) => {
  if (typeof left !== "number" || !Number.isFinite(left)) return null;
  if (typeof right !== "number" || !Number.isFinite(right)) return null;
  return left - right;
};

const getPeakBucketLabel = (bucket: PeakDayBucketRow) => bucket.bucket ?? bucket.label ?? bucket.day_bucket ?? "--";

const pickTopEntries = <T extends { count: number }>(items: T[] | null | undefined, limit = 3) =>
  (Array.isArray(items) ? items : [])
    .slice()
    .sort((left, right) => (right.count === left.count ? 0 : right.count - left.count))
    .slice(0, limit);

const describeTrackingLoadError = (error: unknown) => {
  const message = String((error as { message?: string } | null)?.message ?? "");
  const code = String((error as { code?: string } | null)?.code ?? "");
  const normalized = `${code} ${message}`.toLowerCase();
  if (normalized.includes("timeout") || normalized.includes("econnaborted")) {
    return "判定履歴の取得がタイムアウトしました。DB と履歴生成状況を確認してください。";
  }
  return "判定履歴の取得に失敗しました。";
};

const buildDetailLink = (
  code: string,
  signalDate: string | null,
  side: TrackingSide,
  logicVersion: string,
  rankingDirection?: RankingDirection
) => {
  const params = new URLSearchParams();
  if (signalDate) params.set("signal_date", signalDate);
  params.set("side", side);
  params.set("logic_version", logicVersion);
  if (rankingDirection) params.set("ranking_dir", rankingDirection);
  return `/detail/${encodeURIComponent(code)}?${params.toString()}`;
};

function SummaryCard({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value: string;
  tone?: "neutral" | "up" | "down";
}) {
  return (
    <article className={`tracking-summary-card ${tone !== "neutral" ? `is-${tone}` : ""}`}>
      <div className="tracking-summary-card-label">{label}</div>
      <div className="tracking-summary-card-value">{value}</div>
    </article>
  );
}

function AnalysisSection({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string | null;
  children: ReactNode;
}) {
  return (
    <section className="tracking-analysis-section">
      <div className="tracking-analysis-section-header">
        <div className="tracking-analysis-section-title">{title}</div>
        {subtitle ? <div className="tracking-analysis-section-subtitle">{subtitle}</div> : null}
      </div>
      <div className="tracking-analysis-section-body">{children}</div>
    </section>
  );
}

function TrackingDrawer({
  mode,
  signalDetail,
  rankingDetail,
  onClose,
}: {
  mode: TrackingMode;
  signalDetail: SignalEventDetail | null;
  rankingDetail: RankingAppearanceDetail | null;
  onClose: () => void;
}) {
  const activeSignal = mode === "signal" ? signalDetail : null;
  const activeRanking = mode === "ranking" ? rankingDetail : null;
  const isOpen = Boolean(activeSignal || activeRanking);

  return (
    <>
      {isOpen ? <button type="button" className="tracking-drawer-backdrop" onClick={onClose} aria-label="drawer を閉じる" /> : null}
      <aside className={`tracking-drawer ${isOpen ? "is-open" : ""}`}>
        <div className="tracking-drawer-header">
          <div>
            <div className="tracking-drawer-title">
              {mode === "signal"
                ? `${activeSignal?.event.code ?? "--"} ${activeSignal?.event.name ?? ""}`.trim()
                : `${activeRanking?.appearance.code ?? "--"} ${activeRanking?.appearance.name ?? ""}`.trim()}
            </div>
            <div className="tracking-drawer-sub">
              {mode === "signal"
                ? `${formatIsoDateLabel(activeSignal?.event.signalDate)} / ${activeSignal?.event.side === "sell" ? "売り" : "買い"}`
                : `${formatIsoDateLabel(activeRanking?.appearance.date_iso)} / ${activeRanking?.appearance.dir === "down" ? "下落側" : "上昇側"} / ${activeRanking?.appearance.rank ?? "--"}位`}
            </div>
          </div>
          <button type="button" className="tracking-drawer-close" onClick={onClose}>
            閉じる
          </button>
        </div>
        <div className="tracking-drawer-body">
          {activeSignal ? (
            <>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">イベント詳細</div>
                <div className="tracking-detail-metrics">
                  <div>
                    <span>現在損益</span>
                    <strong>{formatPercent(activeSignal.event.current_directional_return)}</strong>
                  </div>
                  <div>
                    <span>30日成績</span>
                    <strong>{formatPercent(activeSignal.event.return_30d)}</strong>
                  </div>
                  <div>
                    <span>最大有利</span>
                    <strong>{formatPercent(activeSignal.event.max_favorable_30)}</strong>
                  </div>
                  <div>
                    <span>最大不利</span>
                    <strong>{formatPercent(activeSignal.event.max_adverse_30)}</strong>
                  </div>
                  <div>
                    <span>シナリオ崩れ</span>
                    <strong>{formatBreakLabel(activeSignal.event.break_status, activeSignal.event.break_reason)}</strong>
                  </div>
                </div>
              </section>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">初回判定理由</div>
                <div className="tracking-detail-reason">
                  {summarizeReasons(
                    Array.isArray(activeSignal.event.reason_snapshot?.tradeDecisionReasons)
                      ? (activeSignal.event.reason_snapshot?.tradeDecisionReasons as string[])
                      : activeSignal.event.reason_summary
                  )}
                </div>
              </section>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">追加判定履歴</div>
                <div className="tracking-occurrence-list">
                  {(activeSignal.occurrences ?? []).length > 0 ? (
                    activeSignal.occurrences?.map((occurrence) => (
                    <div className="tracking-occurrence-item" key={occurrence.occurrence_id}>
                        <span>{formatIsoDateLabel(occurrence.signalDate)}</span>
                        <strong>{occurrence.is_additional ? "追加判定" : "初回判定"}</strong>
                      </div>
                    ))
                  ) : (
                    <div className="tracking-drawer-empty">追加判定なし</div>
                  )}
                </div>
              </section>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">30営業日推移</div>
                <div className="tracking-series-table">
                  <div className="tracking-series-head">
                    <span>日付</span>
                    <span>終値</span>
                    <span>Close基準</span>
                  </div>
                  {(activeSignal.price_series ?? []).map((row) => (
                    <div className="tracking-series-row" key={`${row.date_iso ?? "na"}-signal`}>
                      <span>{formatIsoDateLabel(row.date_iso)}</span>
                      <span>{formatPrice(row.close)}</span>
                      <span>{formatPercent(row.return_close_basis)}</span>
                    </div>
                  ))}
                </div>
              </section>
              <Link
                className="tracking-detail-link"
                to={buildDetailLink(
                  activeSignal.event.code,
                  activeSignal.event.signalDate,
                  activeSignal.event.side,
                  activeSignal.event.logic_version
                )}
              >
                銘柄詳細を開く
              </Link>
            </>
          ) : null}
          {activeRanking ? (
            <>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">掲載時 snapshot</div>
                <div className="tracking-detail-metrics">
                  <div>
                    <span>掲載順位</span>
                    <strong>{activeRanking.appearance.rank}位</strong>
                  </div>
                  <div>
                    <span>掲載時売買判定</span>
                    <strong>
                      {formatTradeStrengthCaption(
                        formatSignalStateLabel(activeRanking.appearance.signal_state_at_appearance),
                        activeRanking.appearance.display_score
                      )}
                    </strong>
                  </div>
                  <div>
                    <span>掲載時スコア</span>
                    <strong>{formatTradeStrengthPoints(activeRanking.appearance.display_score)}</strong>
                  </div>
                  <div>
                    <span>現在損益</span>
                    <strong>{formatPercent(activeRanking.appearance.current_directional_return)}</strong>
                  </div>
                  <div>
                    <span>30日成績</span>
                    <strong>{formatPercent(activeRanking.appearance.return_30d)}</strong>
                  </div>
                  <div>
                    <span>シナリオ崩れ</span>
                    <strong>{formatBreakLabel(activeRanking.appearance.break_status, activeRanking.appearance.break_reason)}</strong>
                  </div>
                </div>
              </section>
              <section className="tracking-drawer-block">
                <div className="tracking-drawer-block-title">30営業日推移</div>
                <div className="tracking-series-table">
                  <div className="tracking-series-head">
                    <span>日付</span>
                    <span>終値</span>
                    <span>方向有利率</span>
                  </div>
                  {(activeRanking.price_series ?? []).map((row) => (
                    <div className="tracking-series-row" key={`${row.date_iso ?? "na"}-ranking`}>
                      <span>{formatIsoDateLabel(row.date_iso)}</span>
                      <span>{formatPrice(row.close)}</span>
                      <span>{formatPercent(row.return_close_basis)}</span>
                    </div>
                  ))}
                </div>
              </section>
              <Link
                className="tracking-detail-link"
                to={buildDetailLink(
                  activeRanking.appearance.code,
                  activeRanking.appearance.date_iso,
                  activeRanking.appearance.dir === "down" ? "sell" : "buy",
                  activeRanking.appearance.signal_logic_version,
                  activeRanking.appearance.dir
                )}
              >
                銘柄詳細を開く
              </Link>
            </>
          ) : null}
          {!activeSignal && !activeRanking ? (
            <div className="tracking-drawer-empty">行を選ぶと詳細を表示します。</div>
          ) : null}
        </div>
      </aside>
    </>
  );
}

export default function TrackingView() {
  const location = useLocation();
  const navigate = useNavigate();
  const parsedQuery = useMemo<ParsedTrackingQuery>(() => {
    const params = new URLSearchParams(location.search);
    const view = params.get("view");
    const status = params.get("status");
    const side = params.get("side");
    const dir = params.get("dir");
    return {
      view: view === "signal" ? "signal" : view === "analysis" ? "analysis" : "ranking",
      status: status === "completed" || status === "archive" ? status : "active",
      side: side === "sell" ? "sell" : "buy",
      dir: dir === "down" ? "down" : "up",
      q: params.get("q")?.trim() ?? "",
      logicVersion: params.get("logic_version")?.trim() || "latest",
      rankingLogicVersion: params.get("ranking_logic_version")?.trim() || "latest",
      rankBucket: params.get("rank_bucket")?.trim() ?? "",
      from: params.get("from")?.trim() ?? "",
      to: params.get("to")?.trim() ?? "",
      outcome:
        params.get("outcome") === "good" || params.get("outcome") === "bad" || params.get("outcome") === "broken"
          ? (params.get("outcome") as TrackingOutcomeFilter)
          : "all",
      sort:
        params.get("sort") === "oldest" || params.get("sort") === "best" || params.get("sort") === "worst"
          ? (params.get("sort") as TrackingListSort)
          : "recent",
    };
  }, [location.search]);

  const [view, setView] = useState<TrackingMode>(parsedQuery.view);
  const [status, setStatus] = useState<TrackingStatus>(parsedQuery.status);
  const [side, setSide] = useState<TrackingSide>(parsedQuery.side);
  const [direction, setDirection] = useState<RankingDirection>(parsedQuery.dir);
  const [logicVersion, setLogicVersion] = useState(parsedQuery.logicVersion);
  const [rankingLogicVersion, setRankingLogicVersion] = useState(parsedQuery.rankingLogicVersion);
  const [rankBucket, setRankBucket] = useState(parsedQuery.rankBucket);
  const [search, setSearch] = useState(parsedQuery.q);
  const [fromYmd, setFromYmd] = useState(parsedQuery.from);
  const [toYmd, setToYmd] = useState(parsedQuery.to);
  const [outcomeFilter, setOutcomeFilter] = useState<TrackingOutcomeFilter>(parsedQuery.outcome);
  const [listSort, setListSort] = useState<TrackingListSort>(parsedQuery.sort);
  const [pageOffset, setPageOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [totalCount, setTotalCount] = useState<number | null>(null);
  const [signalItems, setSignalItems] = useState<SignalEvent[]>([]);
  const [rankingItems, setRankingItems] = useState<RankingAppearance[]>([]);
  const [signalSummary, setSignalSummary] = useState<SignalSummaryResponse | null>(null);
  const [rankingSummary, setRankingSummary] = useState<RankingSummaryResponse | null>(null);
  const [analysisBuyValidation, setAnalysisBuyValidation] = useState<SignalValidationResponse | null>(null);
  const [analysisSellValidation, setAnalysisSellValidation] = useState<SignalValidationResponse | null>(null);
  const [analysisRankingAnalysis, setAnalysisRankingAnalysis] = useState<RankingAnalysisResponse | null>(null);
  const [analysisLeakageAudit, setAnalysisLeakageAudit] = useState<LeakageAuditResponse | null>(null);
  const [analysisSellComparison, setAnalysisSellComparison] = useState<SignalComparisonResponse | null>(null);
  const [signalLogicVersions, setSignalLogicVersions] = useState<SignalLogicVersionItem[]>([]);
  const [rankingLogicVersions, setRankingLogicVersions] = useState<RankingLogicVersionItem[]>([]);
  const [runtimeStatus, setRuntimeStatus] = useState<TrackingRuntimeStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedSignalId, setSelectedSignalId] = useState<string | null>(null);
  const [selectedRankingId, setSelectedRankingId] = useState<string | null>(null);
  const [signalDetail, setSignalDetail] = useState<SignalEventDetail | null>(null);
  const [rankingDetail, setRankingDetail] = useState<RankingAppearanceDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const [loadingMore, setLoadingMore] = useState(false);

  const resetListViewState = () => {
    setPageOffset(0);
    setHasMore(false);
    setTotalCount(null);
    setSelectedSignalId(null);
    setSelectedRankingId(null);
    setSignalDetail(null);
    setRankingDetail(null);
  };

  useEffect(() => {
    setView(parsedQuery.view);
    setStatus(parsedQuery.status);
    setSide(parsedQuery.side);
    setDirection(parsedQuery.dir);
    setLogicVersion(parsedQuery.logicVersion);
    setRankingLogicVersion(parsedQuery.rankingLogicVersion);
    setRankBucket(parsedQuery.rankBucket);
    setSearch(parsedQuery.q);
    setFromYmd(parsedQuery.from);
    setToYmd(parsedQuery.to);
    setOutcomeFilter(parsedQuery.outcome);
    setListSort(parsedQuery.sort);
    setPageOffset(0);
  }, [parsedQuery]);

  useEffect(() => {
    const params = new URLSearchParams();
    params.set("view", view);
    params.set("status", status);
    if (view === "signal" || view === "analysis") {
      params.set("side", side);
      params.set("logic_version", logicVersion);
    }
    if (view === "ranking" || view === "analysis") {
      params.set("dir", direction);
      params.set("ranking_logic_version", rankingLogicVersion);
      if (rankBucket) params.set("rank_bucket", rankBucket);
    }
    if (view !== "analysis") {
      params.set("sort", listSort);
      if (outcomeFilter !== "all") params.set("outcome", outcomeFilter);
    }
    if (search.trim()) params.set("q", search.trim());
    if (fromYmd.trim()) params.set("from", fromYmd.trim());
    if (toYmd.trim()) params.set("to", toYmd.trim());
    const nextSearch = params.toString();
    if (nextSearch !== location.search.replace(/^\?/, "")) {
      navigate({ pathname: location.pathname, search: `?${nextSearch}` }, { replace: true });
    }
  }, [
    direction,
    fromYmd,
    location.pathname,
    location.search,
    logicVersion,
    navigate,
    outcomeFilter,
    rankBucket,
    rankingLogicVersion,
    search,
    side,
    listSort,
    status,
    toYmd,
    view,
  ]);

  useEffect(() => {
    let canceled = false;
    const loadVersions = async () => {
      try {
        const [signalResponse, rankingResponse] = await Promise.all([
          api.get("/signal-tracking/logic-versions", { timeout: 60000 }),
          api.get("/signal-tracking/ranking-logic-versions", { timeout: 60000 }),
        ]);
        if (canceled) return;
        setSignalLogicVersions(Array.isArray(signalResponse.data?.items) ? signalResponse.data.items : []);
        setRankingLogicVersions(Array.isArray(rankingResponse.data?.items) ? rankingResponse.data.items : []);
      } catch (loadError) {
        if (!canceled) {
          console.error("[tracking] version load failed", loadError);
        }
      }
    };
    void loadVersions();
    return () => {
      canceled = true;
    };
  }, [refreshToken]);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      const isFirstPage = pageOffset === 0 || view === "analysis";
      if (isFirstPage) {
        setLoading(true);
      } else {
        setLoadingMore(true);
      }
      setError(null);
      try {
        const statusRequest = isFirstPage
          ? api.get("/signal-tracking/status", {
              timeout: 60000,
            })
          : null;
        if (view === "signal") {
          const listRequest = api.get("/signal-tracking/events", {
            params: {
              status,
              side,
              logic_version: logicVersion,
              q: search.trim() || undefined,
              from: fromYmd.trim() || undefined,
              to: toYmd.trim() || undefined,
              limit: TRACKING_PAGE_SIZE,
              offset: pageOffset,
              sort: listSort,
              outcome: outcomeFilter,
            },
            timeout: 60000,
          });
          if (isFirstPage && statusRequest) {
            const [statusResponse, summaryResponse, listResponse] = await Promise.all([
              statusRequest,
              api.get("/signal-tracking/summary", {
                params: { side, logic_version: logicVersion },
                timeout: 60000,
              }),
              listRequest,
            ]);
            if (canceled) return;
            const listPayload = (listResponse.data ?? null) as TrackingListResponse<SignalEvent> | null;
            const items = Array.isArray(listPayload?.items) ? (listPayload.items as SignalEvent[]) : [];
            setRuntimeStatus((statusResponse.data ?? null) as TrackingRuntimeStatus | null);
            setSignalSummary((summaryResponse.data ?? null) as SignalSummaryResponse | null);
            setSignalItems(items);
            setHasMore(Boolean(listPayload?.has_more));
            setTotalCount(typeof listPayload?.count === "number" ? listPayload.count : items.length);
          } else {
            const listResponse = await listRequest;
            if (canceled) return;
            const listPayload = (listResponse.data ?? null) as TrackingListResponse<SignalEvent> | null;
            const items = Array.isArray(listPayload?.items) ? (listPayload.items as SignalEvent[]) : [];
            setSignalItems((current) => appendUniqueItems(current, items, (item) => item.event_id));
            setHasMore(Boolean(listPayload?.has_more));
            setTotalCount(typeof listPayload?.count === "number" ? listPayload.count : countFromPageFallback(items, pageOffset));
          }
          if (canceled) return;
          setAnalysisBuyValidation(null);
          setAnalysisSellValidation(null);
          setAnalysisRankingAnalysis(null);
          setAnalysisLeakageAudit(null);
          setAnalysisSellComparison(null);
        } else if (view === "ranking") {
          const listRequest = api.get("/ranking-history/appearances", {
            params: {
              status,
              dir: direction,
              ranking_logic_version: rankingLogicVersion,
              q: search.trim() || undefined,
              rank_bucket: rankBucket || undefined,
              from: fromYmd.trim() || undefined,
              to: toYmd.trim() || undefined,
              limit: TRACKING_PAGE_SIZE,
              offset: pageOffset,
              sort: listSort,
              outcome: outcomeFilter,
            },
            timeout: 60000,
          });
          if (isFirstPage && statusRequest) {
            const [statusResponse, summaryResponse, listResponse] = await Promise.all([
              statusRequest,
              api.get("/ranking-history/summary", {
                params: { dir: direction, ranking_logic_version: rankingLogicVersion },
                timeout: 60000,
              }),
              listRequest,
            ]);
            if (canceled) return;
            const listPayload = (listResponse.data ?? null) as TrackingListResponse<RankingAppearance> | null;
            const items = Array.isArray(listPayload?.items) ? (listPayload.items as RankingAppearance[]) : [];
            setRuntimeStatus((statusResponse.data ?? null) as TrackingRuntimeStatus | null);
            setRankingSummary((summaryResponse.data ?? null) as RankingSummaryResponse | null);
            setRankingItems(items);
            setHasMore(Boolean(listPayload?.has_more));
            setTotalCount(typeof listPayload?.count === "number" ? listPayload.count : items.length);
          } else {
            const listResponse = await listRequest;
            if (canceled) return;
            const listPayload = (listResponse.data ?? null) as TrackingListResponse<RankingAppearance> | null;
            const items = Array.isArray(listPayload?.items) ? (listPayload.items as RankingAppearance[]) : [];
            setRankingItems((current) => appendUniqueItems(current, items, (item) => item.appearance_id));
            setHasMore(Boolean(listPayload?.has_more));
            setTotalCount(typeof listPayload?.count === "number" ? listPayload.count : countFromPageFallback(items, pageOffset));
          }
          if (canceled) return;
          setAnalysisBuyValidation(null);
          setAnalysisSellValidation(null);
          setAnalysisRankingAnalysis(null);
          setAnalysisLeakageAudit(null);
          setAnalysisSellComparison(null);
        } else {
          const [statusResponse, buyResponse, sellResponse, rankingAnalysisResponse, leakageAuditResponse, sellCompareResponse, signalResponse, rankingResponse] = await Promise.all([
            statusRequest!,
            api.get("/signal-tracking/validation", {
              params: { side: "buy", logic_version: logicVersion, from: fromYmd.trim() || undefined, to: toYmd.trim() || undefined },
              timeout: 60000,
            }),
            api.get("/signal-tracking/validation", {
              params: { side: "sell", logic_version: logicVersion, from: fromYmd.trim() || undefined, to: toYmd.trim() || undefined },
              timeout: 60000,
            }),
            api.get("/ranking-history/analysis", {
              params: { ranking_logic_version: rankingLogicVersion },
              timeout: 60000,
            }),
            api.get("/signal-tracking/leakage-audit", {
              params: { logic_version: logicVersion },
              timeout: 60000,
            }),
            api.get("/signal-tracking/compare", {
              params: {
                side: "sell",
                base_logic_version: "logic:trade:v1",
                target_logic_version: "logic:trade:v2-sell-tightened",
                primary_horizon: 10,
                from: fromYmd.trim() || undefined,
                to: toYmd.trim() || undefined,
              },
              timeout: 60000,
            }),
            api.get("/signal-tracking/events", {
              params: {
                status,
                side,
                logic_version: logicVersion,
                q: search.trim() || undefined,
                from: fromYmd.trim() || undefined,
                to: toYmd.trim() || undefined,
                limit: 80,
              },
              timeout: 60000,
            }),
            api.get("/ranking-history/appearances", {
              params: {
                status,
                dir: direction,
                ranking_logic_version: rankingLogicVersion,
                q: search.trim() || undefined,
                rank_bucket: rankBucket || undefined,
                from: fromYmd.trim() || undefined,
                to: toYmd.trim() || undefined,
                limit: 80,
              },
              timeout: 60000,
            }),
          ]);
          if (canceled) return;
          setRuntimeStatus((statusResponse.data ?? null) as TrackingRuntimeStatus | null);
          setAnalysisBuyValidation((buyResponse.data ?? null) as SignalValidationResponse | null);
          setAnalysisSellValidation((sellResponse.data ?? null) as SignalValidationResponse | null);
          setAnalysisRankingAnalysis((rankingAnalysisResponse.data ?? null) as RankingAnalysisResponse | null);
          setAnalysisLeakageAudit((leakageAuditResponse.data ?? null) as LeakageAuditResponse | null);
          setAnalysisSellComparison((sellCompareResponse.data ?? null) as SignalComparisonResponse | null);
          setSignalItems(Array.isArray(signalResponse.data?.items) ? (signalResponse.data.items as SignalEvent[]) : []);
          setRankingItems(Array.isArray(rankingResponse.data?.items) ? (rankingResponse.data.items as RankingAppearance[]) : []);
          setSignalSummary(null);
          setRankingSummary(null);
          setHasMore(false);
          setTotalCount(null);
        }
      } catch (loadError) {
        if (!canceled) {
          console.error("[tracking] load failed", loadError);
          setError(describeTrackingLoadError(loadError));
        }
      } finally {
        if (!canceled) {
          if (isFirstPage) {
            setLoading(false);
          } else {
            setLoadingMore(false);
          }
        }
      }
    };
    void load();
    return () => {
      canceled = true;
    };
  }, [direction, fromYmd, logicVersion, outcomeFilter, pageOffset, rankBucket, rankingLogicVersion, refreshToken, search, side, listSort, status, toYmd, view]);

  useEffect(() => {
    const selectedId = view === "signal" ? selectedSignalId : selectedRankingId;
    if (!selectedId) {
      setSignalDetail(null);
      setRankingDetail(null);
      return;
    }
    let canceled = false;
    const loadDetail = async () => {
      setDetailLoading(true);
      try {
        if (view === "signal") {
          const response = await api.get(`/signal-tracking/events/${encodeURIComponent(selectedId)}`, {
            timeout: 60000,
          });
          if (!canceled) setSignalDetail((response.data ?? null) as SignalEventDetail | null);
        } else {
          const response = await api.get(`/ranking-history/appearances/${encodeURIComponent(selectedId)}`, {
            timeout: 60000,
          });
          if (!canceled) setRankingDetail((response.data ?? null) as RankingAppearanceDetail | null);
        }
      } catch (detailError) {
        if (!canceled) {
          console.error("[tracking] detail load failed", detailError);
        }
      } finally {
        if (!canceled) setDetailLoading(false);
      }
    };
    void loadDetail();
    return () => {
      canceled = true;
    };
  }, [selectedRankingId, selectedSignalId, view]);

  const summaryCards = useMemo(() => {
    if (view === "signal") {
      const eventSummary = signalSummary?.event_summary ?? null;
      return [
        { label: "Active件数", value: String(eventSummary?.active_count ?? "--") },
        { label: "Completed件数", value: String(eventSummary?.completed_count ?? "--") },
        { label: "Archive件数", value: String(eventSummary?.archive_count ?? "--") },
        {
          label: "Active平均方向有利率",
          value: formatPercent(eventSummary?.active_average_directional_return),
          tone: metricTone(eventSummary?.active_average_directional_return) === "is-up" ? "up" : "down",
        },
        { label: "Completed満了勝率", value: formatPercent(eventSummary?.completed_win_rate) },
        { label: "重複判定率", value: formatPercent(signalSummary?.duplicate_signal_rate) },
      ] as const;
    }
    return [
      { label: "Active件数", value: String(rankingSummary?.active_count ?? "--") },
      { label: "Completed件数", value: String(rankingSummary?.completed_count ?? "--") },
      { label: "Archive件数", value: String(rankingSummary?.archive_count ?? "--") },
      {
        label: "Active平均方向有利率",
        value: formatPercent(rankingSummary?.active_average_directional_return),
        tone: metricTone(rankingSummary?.active_average_directional_return) === "is-up" ? "up" : "down",
      },
      { label: "Completed満了勝率", value: formatPercent(rankingSummary?.completed_win_rate) },
      { label: "シナリオ崩れ率", value: formatPercent(rankingSummary?.break_rate) },
    ] as const;
  }, [rankingSummary, signalSummary, view]);

  const analysisScoreThresholdBlocks = useMemo(() => {
    if (view !== "analysis") return { buy: [], sell: [] };
    const buildBlocks = (rows: ScoreThresholdRow[]) => {
      const scoreKeys = ["tradePriorityScore", "entryScore", "probSide"] as const;
      return scoreKeys.map((scoreKey) => {
        const sortedRows = rows
          .filter((row) => row.score_key === scoreKey)
          .slice()
          .sort((left, right) => {
            const leftReturn = left.average_directional_return_30 ?? Number.NEGATIVE_INFINITY;
            const rightReturn = right.average_directional_return_30 ?? Number.NEGATIVE_INFINITY;
            if (rightReturn !== leftReturn) return rightReturn - leftReturn;
            const leftLift = left.lift_vs_same_date_universe_30 ?? Number.NEGATIVE_INFINITY;
            const rightLift = right.lift_vs_same_date_universe_30 ?? Number.NEGATIVE_INFINITY;
            if (rightLift !== leftLift) return rightLift - leftLift;
            return right.count - left.count;
          });
        return { scoreKey, rows: sortedRows.slice(0, 3) };
      });
    };
    return {
      buy: buildBlocks(analysisBuyValidation?.decision_level?.score_threshold_rows ?? []),
      sell: buildBlocks(analysisSellValidation?.decision_level?.score_threshold_rows ?? []),
    };
  }, [analysisBuyValidation, analysisSellValidation, view]);

  const analysisSummaryCards = useMemo(() => {
    if (view !== "analysis") return [];
    const buyDecision = analysisBuyValidation?.decision_level ?? null;
    const sellDecision = analysisSellValidation?.decision_level ?? null;
    const rankingUp = analysisRankingAnalysis?.by_dir.find((item) => item.dir === "up") ?? null;
    const bestScoreThresholdRow = (rows: ScoreThresholdRow[]) =>
      rows
        .slice()
        .sort((left, right) => {
          const leftReturn = left.average_directional_return_30 ?? Number.NEGATIVE_INFINITY;
          const rightReturn = right.average_directional_return_30 ?? Number.NEGATIVE_INFINITY;
          if (rightReturn !== leftReturn) return rightReturn - leftReturn;
          const leftLift = left.lift_vs_same_date_universe_30 ?? Number.NEGATIVE_INFINITY;
          const rightLift = right.lift_vs_same_date_universe_30 ?? Number.NEGATIVE_INFINITY;
          if (rightLift !== leftLift) return rightLift - leftLift;
          return right.count - left.count;
        })[0] ?? null;
    const buyThresholdBest = bestScoreThresholdRow(
      analysisScoreThresholdBlocks.buy.flatMap((block) => block.rows)
    );
    const sellThresholdBest = bestScoreThresholdRow(
      analysisScoreThresholdBlocks.sell.flatMap((block) => block.rows)
    );
    const leakageFlagCount =
      (analysisLeakageAudit?.basis_provenance?.future_source_as_of_count ?? 0) +
      (analysisLeakageAudit?.basis_provenance?.future_pred_dt_count ?? 0) +
      (analysisLeakageAudit?.basis_provenance?.prohibited_payload_count ?? 0);
    return [
      {
        label: "買い 30d方向勝率",
        value: formatPercent(buyDecision?.qualified_directional_hit_rate_20),
        tone: metricTone(buyDecision?.average_directional_return_20) === "is-up" ? "up" : "down",
      },
      {
        label: "売り 30d方向勝率",
        value: formatPercent(sellDecision?.qualified_directional_hit_rate_10),
        tone: metricTone(sellDecision?.average_directional_return_10) === "is-up" ? "up" : "down",
      },
      {
        label: "買い vs 上昇掲載",
        value: formatSignedPercent(buyDecision?.lift_vs_same_date_universe_30),
        tone: metricTone(buyDecision?.lift_vs_same_date_universe_30) === "is-up" ? "up" : "down",
      },
      {
        label: "売り vs 下降掲載",
        value: formatSignedPercent(sellDecision?.lift_vs_same_date_universe_10),
        tone: metricTone(sellDecision?.lift_vs_same_date_universe_10) === "is-up" ? "up" : "down",
      },
      {
        label: "ranking up 30d",
        value: formatSignedPercent(rankingUp?.average_directional_return_30),
        tone: metricTone(rankingUp?.average_directional_return_30) === "is-up" ? "up" : "down",
      },
      {
        label: "buy profit peak median",
        value: formatDayCount(
          buyDecision?.median_days_to_max_favorable_30 ?? buyDecision?.days_to_max_favorable_30
        ),
      },
      {
        label: "buy adverse peak median",
        value: formatDayCount(buyDecision?.median_days_to_max_adverse_30 ?? buyDecision?.days_to_max_adverse_30),
      },
      {
        label: "sell profit peak median",
        value: formatDayCount(
          sellDecision?.median_days_to_max_favorable_30 ?? sellDecision?.days_to_max_favorable_30
        ),
      },
      {
        label: "sell adverse peak median",
        value: formatDayCount(sellDecision?.median_days_to_max_adverse_30 ?? sellDecision?.days_to_max_adverse_30),
      },
      {
        label: "future leakage flags",
        value: formatPlainNumber(leakageFlagCount),
      },
      {
        label: "buy best threshold",
        value: buyThresholdBest ? `${buyThresholdBest.score_key} ${buyThresholdBest.threshold.toFixed(2)}` : "--",
        tone: metricTone(buyThresholdBest?.average_directional_return_30) === "is-up" ? "up" : "down",
      },
      {
        label: "buy threshold ret",
        value: formatSignedPercent(buyThresholdBest?.average_directional_return_30),
        tone: metricTone(buyThresholdBest?.average_directional_return_30) === "is-up" ? "up" : "down",
      },
      {
        label: "sell best threshold",
        value: sellThresholdBest ? `${sellThresholdBest.score_key} ${sellThresholdBest.threshold.toFixed(2)}` : "--",
        tone: metricTone(sellThresholdBest?.average_directional_return_30) === "is-up" ? "up" : "down",
      },
      {
        label: "sell threshold ret",
        value: formatSignedPercent(sellThresholdBest?.average_directional_return_30),
        tone: metricTone(sellThresholdBest?.average_directional_return_30) === "is-up" ? "up" : "down",
      },
    ] as const;
  }, [analysisBuyValidation, analysisLeakageAudit, analysisRankingAnalysis, analysisScoreThresholdBlocks, analysisSellValidation, view]);

  const analysisRollingBlocks = useMemo(() => {
    if (view !== "analysis") return [];
    return [
      {
        label: "buy",
        rows: [
          {
            key: "5",
            text: `5d ${formatPercent(analysisBuyValidation?.decision_level?.qualified_directional_hit_rate_5)} / ${formatSignedPercent(analysisBuyValidation?.decision_level?.average_directional_return_5)}`,
          },
          {
            key: "10",
            text: `10d ${formatPercent(analysisBuyValidation?.decision_level?.qualified_directional_hit_rate_10)} / ${formatSignedPercent(analysisBuyValidation?.decision_level?.average_directional_return_10)}`,
          },
          {
            key: "20",
            text: `20d ${formatPercent(analysisBuyValidation?.decision_level?.qualified_directional_hit_rate_20)} / ${formatSignedPercent(analysisBuyValidation?.decision_level?.average_directional_return_20)}`,
          },
          {
            key: "30",
            text: `30d ${formatPercent(analysisBuyValidation?.decision_level?.qualified_directional_hit_rate_30)} / ${formatSignedPercent(analysisBuyValidation?.decision_level?.average_directional_return_30)}`,
          },
          {
            key: "60",
            text: `60d ${formatPercent(analysisBuyValidation?.decision_level?.qualified_directional_hit_rate_60)} / ${formatSignedPercent(analysisBuyValidation?.decision_level?.average_directional_return_60)}`,
          },
        ],
      },
      {
        label: "sell",
        rows: [
          {
            key: "5",
            text: `5d ${formatPercent(analysisSellValidation?.decision_level?.qualified_directional_hit_rate_5)} / ${formatSignedPercent(analysisSellValidation?.decision_level?.average_directional_return_5)}`,
          },
          {
            key: "10",
            text: `10d ${formatPercent(analysisSellValidation?.decision_level?.qualified_directional_hit_rate_10)} / ${formatSignedPercent(analysisSellValidation?.decision_level?.average_directional_return_10)}`,
          },
          {
            key: "20",
            text: `20d ${formatPercent(analysisSellValidation?.decision_level?.qualified_directional_hit_rate_20)} / ${formatSignedPercent(analysisSellValidation?.decision_level?.average_directional_return_20)}`,
          },
          {
            key: "30",
            text: `30d ${formatPercent(analysisSellValidation?.decision_level?.qualified_directional_hit_rate_30)} / ${formatSignedPercent(analysisSellValidation?.decision_level?.average_directional_return_30)}`,
          },
          {
            key: "60",
            text: `60d ${formatPercent(analysisSellValidation?.decision_level?.qualified_directional_hit_rate_60)} / ${formatSignedPercent(analysisSellValidation?.decision_level?.average_directional_return_60)}`,
          },
        ],
      },
    ];
  }, [analysisBuyValidation, analysisSellValidation, view]);

  const analysisPeakComparisonBlocks = useMemo(() => {
    if (view !== "analysis") return [];
    const up = analysisRankingAnalysis?.by_dir.find((item) => item.dir === "up") ?? null;
    const down = analysisRankingAnalysis?.by_dir.find((item) => item.dir === "down") ?? null;
    return [
      {
        label: "ranking up",
        rows: [
          {
            key: "profit peak median",
            text: formatDayCount(up?.median_days_to_max_favorable_30 ?? up?.days_to_max_favorable_30),
          },
          {
            key: "adverse peak median",
            text: formatDayCount(up?.median_days_to_max_adverse_30 ?? up?.days_to_max_adverse_30),
          },
          {
            key: "30d win",
            text: formatPercent(up?.directional_win_rate_30),
          },
          {
            key: "30d return",
            text: formatSignedPercent(up?.average_directional_return_30),
          },
        ],
      },
      {
        label: "ranking down",
        rows: [
          {
            key: "profit peak median",
            text: formatDayCount(down?.median_days_to_max_favorable_30 ?? down?.days_to_max_favorable_30),
          },
          {
            key: "adverse peak median",
            text: formatDayCount(down?.median_days_to_max_adverse_30 ?? down?.days_to_max_adverse_30),
          },
          {
            key: "30d win",
            text: formatPercent(down?.directional_win_rate_30),
          },
          {
            key: "30d return",
            text: formatSignedPercent(down?.average_directional_return_30),
          },
        ],
      },
    ];
  }, [analysisRankingAnalysis, view]);

  const analysisSellCompareRows = useMemo(() => {
    if (view !== "analysis" || !analysisSellComparison) return [];
    return [
      {
        key: "qualified decisions",
        text: formatPlainNumber(analysisSellComparison.decision.qualified_decisions.target),
        delta: formatSignedNumber(analysisSellComparison.decision.qualified_decisions.delta),
      },
      {
        key: `${analysisSellComparison.primary_horizon}d direction勝率`,
        text: formatPercent(analysisSellComparison.decision.directional_hit_rate.target),
        delta: formatSignedPercent(analysisSellComparison.decision.directional_hit_rate.delta),
      },
      {
        key: `${analysisSellComparison.primary_horizon}d return`,
        text: formatSignedPercent(analysisSellComparison.decision.average_directional_return.target),
        delta: formatSignedPercent(analysisSellComparison.decision.average_directional_return.delta),
      },
      {
        key: `${analysisSellComparison.primary_horizon}d lift`,
        text: formatSignedPercent(analysisSellComparison.decision.lift_vs_same_date_universe.target),
        delta: formatSignedPercent(analysisSellComparison.decision.lift_vs_same_date_universe.delta),
      },
      {
        key: "campaign win",
        text: formatPercent(analysisSellComparison.campaign.evaluated_directional_win_rate.target),
        delta: formatSignedPercent(analysisSellComparison.campaign.evaluated_directional_win_rate.delta),
      },
    ];
  }, [analysisSellComparison, view]);

  const analysisPeakBuckets = useMemo(() => {
    if (view !== "analysis") return [];
    const source =
      analysisRankingAnalysis?.peak_day_buckets ??
      analysisBuyValidation?.decision_level?.peak_day_buckets ??
      analysisSellValidation?.decision_level?.peak_day_buckets ??
      [];
    return pickTopEntries(source, 6);
  }, [analysisBuyValidation, analysisRankingAnalysis, analysisSellValidation, view]);

  const analysisProfitTimingPatterns = useMemo(() => {
    if (view !== "analysis") return { buy: [], sell: [] };
    return {
      buy: analysisBuyValidation?.decision_level?.profit_timing_patterns ?? [],
      sell: analysisSellValidation?.decision_level?.profit_timing_patterns ?? [],
    };
  }, [analysisBuyValidation, analysisSellValidation, view]);

  const analysisSellSubsetRows = useMemo(() => {
    if (view !== "analysis") return [];
    return analysisSellValidation?.sell_subset_comparison?.subsets ?? [];
  }, [analysisSellValidation, view]);

  const analysisShockBlocks = useMemo(() => {
    if (view !== "analysis") return [];
    const buildBlock = (label: string, summary: ShockAnalysisSideSummary | null | undefined) => {
      const cohortByKey = new Map((summary?.cohort_rows ?? []).map((row) => [row.cohort_key, row]));
      const cohortOrder = ["both", "drop_10pct_only", "bottom_decile_only", "normal", "insufficient_history"] as const;
      const cohortRows = cohortOrder.map((cohortKey) => cohortByKey.get(cohortKey) ?? null).filter(Boolean) as ShockAnalysisCohortRow[];
      return {
        label,
        summary,
        cohortRows,
        topSetupTypes: pickTopEntries(summary?.by_setup_type ?? [], 3),
        topRegimes: pickTopEntries(summary?.by_regime ?? [], 3),
        topBreakReasons: pickTopEntries(summary?.by_break_reason ?? [], 3),
        shockExamples: (summary?.shock_examples ?? []).slice(0, 4),
      };
    };
    return [
      buildBlock("buy", analysisBuyValidation?.decision_level?.shock_analysis ?? null),
      buildBlock("sell", analysisSellValidation?.decision_level?.shock_analysis ?? null),
    ];
  }, [analysisBuyValidation, analysisSellValidation, view]);

  const analysisRegimeRows = useMemo(() => {
    if (view !== "analysis") return [];
    return pickTopEntries(
      (analysisBuyValidation?.decision_level?.by_regime ?? []).map((row) => ({
        count: row.qualified_decisions,
        regime_label: row.regime ?? "--",
        regime_score: row.lift_vs_same_date_universe_30 ?? row.average_directional_return_30 ?? null,
        directional_hit_rate_30: row.directional_hit_rate_30 ?? null,
      })),
      4
    );
  }, [analysisBuyValidation, view]);

  const analysisFailureReasons = useMemo(() => {
    if (view !== "analysis") return [];
    return pickTopEntries(analysisBuyValidation?.decision_level?.by_break_reason ?? [], 5);
  }, [analysisBuyValidation, view]);

  const analysisFailureExamples = useMemo(() => {
    if (view !== "analysis") return [];
    return analysisBuyValidation?.decision_level?.failure_examples ?? [];
  }, [analysisBuyValidation, view]);

  const signalEmptyMessage = useMemo(() => {
    if (runtimeStatus && !runtimeStatus.signal_history_generated) {
      return "このDBでは売買判定履歴がまだ生成されていません。signal backfill / rebuild を先に実行してください。";
    }
    return "条件に一致する売買判定イベントがありません。期間かコード条件を見直してください。";
  }, [runtimeStatus]);

  const rankingEmptyMessage = useMemo(() => {
    if (runtimeStatus && !runtimeStatus.ranking_history_generated) {
      return "このDBではランキング掲載履歴がまだ生成されていません。ranking appearance rebuild を先に実行してください。";
    }
    return "条件に一致するランキング掲載イベントがありません。期間かコード条件を見直してください。";
  }, [runtimeStatus]);

  const runtimeNote = useMemo(() => {
    if (!runtimeStatus) return null;
    const historyCount = view === "signal" ? runtimeStatus.signal_occurrence_count : runtimeStatus.ranking_appearance_count;
    const latestDate = view === "signal" ? runtimeStatus.signal_latest_date_iso : runtimeStatus.ranking_latest_date_iso;
    const historyLabel = view === "signal" ? "売買判定履歴" : "ランキング掲載履歴";
    return `${historyLabel} ${historyCount.toLocaleString("ja-JP")}件 / latest ${latestDate ? formatIsoDateLabel(latestDate) : "--"} / DB ${runtimeStatus.resolved_stocks_db_path}`;
  }, [runtimeStatus, view]);

  const tableStyle = useMemo(
    () =>
      ({
        "--tracking-date-col": "116px",
        "--tracking-code-col": "96px",
        "--tracking-name-col": "180px",
        "--tracking-price-col": "140px",
        "--tracking-return-col": "168px",
        "--tracking-progress-col": "128px",
        "--tracking-status-col": "120px",
        "--tracking-count-col": "120px",
        "--tracking-extra-col": "200px",
        "--tracking-max-col": "118px",
      }) as CSSProperties,
    []
  );

  return (
    <div className="tracking-view">
      <TopNav />
      <main className="tracking-main">
        <header className="tracking-header-main">
          <div className="tracking-page-kicker">Ranking / Tracking</div>
          <h1 className="tracking-page-title">売買判定とランキング掲載の予後を30営業日で見る</h1>
          <p className="tracking-page-lead">
            判定が出た日とランキングに載った日を event 単位で残し、その後の推移とシナリオ崩れを同じ表で追います。
          </p>
        </header>

        <div className="tracking-tabs">
          {MODE_TABS.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`tracking-tab ${view === tab.key ? "is-active" : ""}`}
              onClick={() => {
                resetListViewState();
                setView(tab.key);
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="tracking-tabs">
          {STATUS_TABS.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`tracking-tab ${status === tab.key ? "is-active" : ""}`}
              onClick={() => {
                resetListViewState();
                setStatus(tab.key);
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <section className="tracking-summary-grid">
          {(view === "analysis" ? analysisSummaryCards : summaryCards).map((card) => (
            <SummaryCard key={card.label} label={card.label} value={card.value} tone={(card as { tone?: "neutral" | "up" | "down" }).tone ?? "neutral"} />
          ))}
        </section>

        <section className="tracking-filter-row">
          <div className="tracking-side-toggle">
            {(view === "ranking" ? DIR_TABS : SIDE_TABS).map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={`tracking-side-button ${
                  view === "ranking" ? (direction === tab.key ? "is-active" : "") : (side === tab.key ? "is-active" : "")
                }`}
                onClick={() => {
                  resetListViewState();
                  if (view === "ranking") {
                    setDirection(tab.key as RankingDirection);
                  } else {
                    setSide(tab.key as TrackingSide);
                  }
                }}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {view === "analysis" ? (
            <>
              <label className="tracking-version-field">
                <span>signal logic version</span>
                <select
                  value={logicVersion}
                  onChange={(event) => {
                    resetListViewState();
                    setLogicVersion(event.target.value);
                  }}
                >
                  <option value="latest">latest</option>
                  {signalLogicVersions.map((item) => {
                    const version = item.logic_version;
                    return (
                      <option key={version} value={version}>
                        {version}
                      </option>
                    );
                  })}
                </select>
              </label>
              <label className="tracking-version-field">
                <span>ranking logic version</span>
                <select
                  value={rankingLogicVersion}
                  onChange={(event) => {
                    resetListViewState();
                    setRankingLogicVersion(event.target.value);
                  }}
                >
                  <option value="latest">latest</option>
                  {rankingLogicVersions.map((item) => {
                    const version = item.ranking_logic_version;
                    return (
                      <option key={version} value={version}>
                        {version}
                      </option>
                    );
                  })}
                </select>
              </label>
            </>
          ) : (
            <label className="tracking-version-field">
              <span>{view === "signal" ? "logic version" : "ranking version"}</span>
              <select
                value={view === "signal" ? logicVersion : rankingLogicVersion}
                onChange={(event) => {
                  resetListViewState();
                  if (view === "signal") {
                    setLogicVersion(event.target.value);
                  } else {
                    setRankingLogicVersion(event.target.value);
                  }
                }}
              >
                <option value="latest">latest</option>
                {(view === "signal" ? signalLogicVersions : rankingLogicVersions).map((item) => {
                  const version =
                    "logic_version" in item ? item.logic_version : item.ranking_logic_version;
                  return (
                    <option key={version} value={version}>
                      {version}
                    </option>
                  );
                })}
              </select>
            </label>
          )}

          {view === "ranking" ? (
            <label className="tracking-version-field">
              <span>rank bucket</span>
              <select
                value={rankBucket}
                onChange={(event) => {
                  resetListViewState();
                  setRankBucket(event.target.value);
                }}
              >
                {RANK_BUCKETS.map((item) => (
                  <option key={item.value || "all"} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
          ) : null}

          {view !== "analysis" ? (
            <>
              <label className="tracking-version-field">
                <span>結果</span>
                <select
                  value={outcomeFilter}
                  onChange={(event) => {
                    resetListViewState();
                    setOutcomeFilter(event.target.value as TrackingOutcomeFilter);
                  }}
                >
                  {TRACKING_OUTCOME_OPTIONS.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="tracking-version-field">
                <span>並び順</span>
                <select
                  value={listSort}
                  onChange={(event) => {
                    resetListViewState();
                    setListSort(event.target.value as TrackingListSort);
                  }}
                >
                  {TRACKING_SORT_OPTIONS.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
            </>
          ) : null}

          <label className="tracking-search-field">
            <span>コード / 銘柄名</span>
            <input
              value={search}
              onChange={(event) => {
                resetListViewState();
                setSearch(event.target.value);
              }}
              placeholder="2413 / エムスリー"
            />
          </label>

          <label className="tracking-version-field">
            <span>from</span>
            <input
              value={fromYmd}
              onChange={(event) => {
                resetListViewState();
                setFromYmd(event.target.value);
              }}
              placeholder="20250301"
            />
          </label>

          <label className="tracking-version-field">
            <span>to</span>
            <input
              value={toYmd}
              onChange={(event) => {
                resetListViewState();
                setToYmd(event.target.value);
              }}
              placeholder="20260402"
            />
          </label>

          {view !== "analysis" ? (
            <div className="tracking-side-toggle">
              {[1, 2].map((years) => (
                <button
                  key={years}
                  type="button"
                  className="tracking-side-button"
                  onClick={() => {
                    resetListViewState();
                    const range = buildYearsPresetRange(toYmd, years);
                    setFromYmd(range.from);
                    setToYmd(range.to);
                  }}
                >
                  {years}Y
                </button>
              ))}
            </div>
          ) : null}

          <button
            type="button"
            className="tracking-refresh-button"
            onClick={() => {
              resetListViewState();
              setRefreshToken((current) => current + 1);
            }}
          >
            再取得
          </button>
        </section>

        {runtimeNote ? <div className="tracking-runtime-note">{runtimeNote}</div> : null}
        {error ? <div className="tracking-inline-error">{error}</div> : null}
        {loading ? <div className="tracking-empty">読み込み中...</div> : null}
        {view !== "analysis" && !loading ? (
          <div className="tracking-runtime-note">
            {`表示 ${view === "signal" ? signalItems.length : rankingItems.length} / ${totalCount?.toLocaleString("ja-JP") ?? "--"} 件`}
          </div>
        ) : null}

        {view === "analysis" ? (
          <section className="tracking-analysis-shell">
            <AnalysisSection title="summary" subtitle="buy / sell と ranking 上下の基本指標">
              <div className="tracking-summary-grid">
                {analysisSummaryCards.map((card) => (
                  <SummaryCard
                    key={card.label}
                    label={card.label}
                    value={card.value}
                    tone={(card as { tone?: "neutral" | "up" | "down" }).tone ?? "neutral"}
                  />
                ))}
              </div>
            </AnalysisSection>

            <AnalysisSection title="rolling" subtitle="5d, 20d, 30d, 60d の勝率 / 平均リターン">
              <div className="tracking-analysis-card-grid">
                {analysisRollingBlocks.map((block) => (
                  <article className="tracking-analysis-card" key={block.label}>
                    <div className="tracking-analysis-card-title">{block.label}</div>
                    <div className="tracking-analysis-card-body">
                      {block.rows.map((row) => (
                        <div className="tracking-analysis-mini-row" key={`${block.label}-${row.key}`}>
                          <span>{row.key}</span>
                          <strong>{row.text}</strong>
                        </div>
                      ))}
                    </div>
                  </article>
                ))}
              </div>
            </AnalysisSection>

            <AnalysisSection title="sell compare" subtitle="v1 と tightened v2 の 10日主評価比較">
              <div className="tracking-analysis-card-grid">
                <article className="tracking-analysis-card">
                  <div className="tracking-analysis-card-title">sell v2-tightened</div>
                  <div className="tracking-analysis-card-body">
                    {analysisSellCompareRows.length > 0 ? (
                      analysisSellCompareRows.map((row) => (
                        <div className="tracking-analysis-mini-row" key={row.key}>
                          <span>{row.key}</span>
                          <strong>{row.text}</strong>
                          <small>{row.delta}</small>
                        </div>
                      ))
                    ) : (
                      <div className="tracking-analysis-empty-inline">sell compare はまだ利用できません。</div>
                    )}
                  </div>
                </article>
              </div>
            </AnalysisSection>

            <AnalysisSection title="sell subsets" subtitle="既存 sell event を条件別に絞ったときの 10日主評価比較">
              <div className="tracking-analysis-card-grid">
                {analysisSellSubsetRows.length > 0 ? (
                  analysisSellSubsetRows.map((row) => (
                    <article className="tracking-analysis-card" key={row.subset_key}>
                      <div className="tracking-analysis-card-title">{row.label}</div>
                      <div className="tracking-analysis-card-body">
                        <div className="tracking-analysis-mini-row">
                          <span>count</span>
                          <strong>{formatPlainNumber(row.count)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>campaigns</span>
                          <strong>{formatPlainNumber(row.campaign_count)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>10d direction勝率</span>
                          <strong>{formatPercent(row.directional_hit_rate)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>10d return</span>
                          <strong>{formatSignedPercent(row.average_directional_return)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>10d lift</span>
                          <strong>{formatSignedPercent(row.lift_vs_same_date_universe)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>break rate</span>
                          <strong>{formatPercent(row.break_rate)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>profit peak median</span>
                          <strong>{formatDayCount(row.median_days_to_max_favorable_30)}</strong>
                        </div>
                      </div>
                    </article>
                  ))
                ) : (
                  <div className="tracking-analysis-empty-inline">sell subsets はまだ利用できません。</div>
                )}
              </div>
            </AnalysisSection>

            <AnalysisSection title="timing pattern" subtitle="利益ピークが 10日型 / 20日型 / 30日型のどれに寄るか">
              <div className="tracking-analysis-card-grid">
                {(["buy", "sell"] as const).map((kind) => (
                  <article className="tracking-analysis-card" key={kind}>
                    <div className="tracking-analysis-card-title">{kind}</div>
                    <div className="tracking-analysis-card-body">
                      {analysisProfitTimingPatterns[kind].length > 0 ? (
                        analysisProfitTimingPatterns[kind].map((row) => (
                          <div className="tracking-analysis-mini-row" key={`${kind}-${row.bucket}`}>
                            <span>{row.bucket}</span>
                            <strong>{formatPlainNumber(row.count)}件 / {formatPercent(row.share)}</strong>
                            <small>
                              10d {formatSignedPercent(row.average_directional_return_10)} / 20d {formatSignedPercent(row.average_directional_return_20)} / 30d {formatSignedPercent(row.average_directional_return_30)}
                            </small>
                          </div>
                        ))
                      ) : (
                        <div className="tracking-analysis-empty-inline">timing pattern はまだ利用できません。</div>
                      )}
                    </div>
                  </article>
                ))}
              </div>
            </AnalysisSection>

            <AnalysisSection title="peak day" subtitle="day 0 = signal / appearance date">
              <div className="tracking-analysis-card-grid">
                {analysisPeakComparisonBlocks.map((block) => (
                  <article className="tracking-analysis-card" key={block.label}>
                    <div className="tracking-analysis-card-title">{block.label}</div>
                    <div className="tracking-analysis-card-body">
                      {block.rows.map((row) => (
                        <div className="tracking-analysis-mini-row" key={`${block.label}-${row.key}`}>
                          <span>{row.key}</span>
                          <strong>{row.text}</strong>
                        </div>
                      ))}
                    </div>
                  </article>
                ))}
              </div>
              <div className="tracking-analysis-card-grid">
                {analysisPeakBuckets.length ? (
                  analysisPeakBuckets.map((bucket) => (
                    <article className="tracking-analysis-card" key={getPeakBucketLabel(bucket)}>
                      <div className="tracking-analysis-card-title">{getPeakBucketLabel(bucket)}</div>
                      <div className="tracking-analysis-card-body">
                        <div className="tracking-analysis-mini-row">
                          <span>count</span>
                          <strong>{formatPlainNumber(bucket.count)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>profit peak median</span>
                          <strong>
                            {formatDayCount(
                              bucket.median_days_to_max_favorable_30 ?? bucket.days_to_max_favorable_30
                            )}
                          </strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>adverse peak median</span>
                          <strong>
                            {formatDayCount(
                              bucket.median_days_to_max_adverse_30 ?? bucket.days_to_max_adverse_30
                            )}
                          </strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>30d return</span>
                          <strong>{formatSignedPercent(bucket.average_directional_return_30)}</strong>
                        </div>
                      </div>
                    </article>
                  ))
                ) : (
                  <div className="tracking-analysis-empty-inline">peak_day_buckets is not available yet.</div>
                )}
              </div>
            </AnalysisSection>

            <AnalysisSection title="regime" subtitle="analysis-bridge の公開 regime snapshot">
              <div className="tracking-analysis-card-grid">
                <article className="tracking-analysis-card">
                  <div className="tracking-analysis-card-title">snapshot</div>
                  <div className="tracking-analysis-card-body">
                    <div className="tracking-analysis-mini-row">
                      <span>publish</span>
                      <strong>{analysisLeakageAudit?.basis_version ?? "--"}</strong>
                    </div>
                    <div className="tracking-analysis-mini-row">
                      <span>freshness</span>
                      <strong>
                        {formatPlainNumber(
                          (analysisLeakageAudit?.basis_provenance?.future_source_as_of_count ?? 0) +
                            (analysisLeakageAudit?.basis_provenance?.future_pred_dt_count ?? 0)
                        )}
                      </strong>
                    </div>
                    <div className="tracking-analysis-mini-row">
                      <span>as_of</span>
                      <strong>{formatPlainNumber(analysisLeakageAudit?.basis_provenance?.prohibited_payload_count)}</strong>
                    </div>
                  </div>
                </article>
                {analysisRegimeRows.length > 0 ? (
                  analysisRegimeRows.map((row, index) => (
                    <article className="tracking-analysis-card" key={`${row.regime_label ?? "na"}-${index}`}>
                      <div className="tracking-analysis-card-title">{row.regime_label}</div>
                      <div className="tracking-analysis-card-body">
                        <div className="tracking-analysis-mini-row">
                          <span>as_of</span>
                          <strong>{formatSignedPercent(row.regime_score)}</strong>
                        </div>
                        <div className="tracking-analysis-mini-row">
                          <span>score</span>
                          <strong>{formatPercent(row.directional_hit_rate_30)}</strong>
                        </div>
                      </div>
                    </article>
                  ))
                ) : (
                  <article className="tracking-analysis-card">
                    <div className="tracking-analysis-card-body">regime snapshot はありません。</div>
                  </article>
                )}
              </div>
            </AnalysisSection>

            <AnalysisSection title="failure" subtitle="break reason と失敗例">
              <div className="tracking-analysis-card-grid">
                <article className="tracking-analysis-card">
                  <div className="tracking-analysis-card-title">top break reasons</div>
                  <div className="tracking-analysis-card-body">
                    {analysisFailureReasons.length > 0 ? (
                      analysisFailureReasons.map((item) => (
                        <div className="tracking-analysis-mini-row" key={item.break_reason}>
                          <span>{item.break_reason}</span>
                          <strong>{formatPlainNumber(item.count)}</strong>
                        </div>
                      ))
                    ) : (
                      <div className="tracking-analysis-empty-inline">break reason はありません。</div>
                    )}
                  </div>
                </article>
                <article className="tracking-analysis-card">
                  <div className="tracking-analysis-card-title">signal failure examples</div>
                  <div className="tracking-analysis-card-body">
                    {analysisFailureExamples.length > 0 ? (
                      analysisFailureExamples.map((item) => (
                        <div className="tracking-analysis-example" key={item.event_id}>
                          <Link className="tracking-analysis-link" to={buildDetailLink(item.code, item.signal_date, "buy", logicVersion)}>
                            {item.code}
                          </Link>
                          <span>{item.signal_date ?? "--"}</span>
                          <strong>{formatPercent(item.return_30d)}</strong>
                        </div>
                      ))
                    ) : (
                      <div className="tracking-analysis-empty-inline">failure example はありません。</div>
                    )}
                  </div>
                </article>
              </div>
            </AnalysisSection>
            <AnalysisSection title="shock" subtitle="過去10年の 20d 急落局面を buy / sell で分離してみる">
              <div className="tracking-analysis-card-grid">
                {analysisShockBlocks.map((block) => {
                  const summary = block.summary;
                  const severeCohort = block.cohortRows.find((row) => row.cohort_key === "both") ?? null;
                  return (
                    <article className="tracking-analysis-card" key={block.label}>
                      <div className="tracking-analysis-card-title">{block.label}</div>
                      <div className="tracking-analysis-card-body">
                        {summary ? (
                          <>
                            <div className="tracking-analysis-mini-row">
                              <span>window</span>
                              <strong>
                                {formatYmdDisplay(summary.window.from_ymd)} - {formatYmdDisplay(summary.window.to_ymd)}
                              </strong>
                              <small>
                                {formatPercent(summary.window.drop_threshold)} / bottom {formatPercent(summary.window.bottom_decile_threshold)}
                              </small>
                            </div>
                            <div className="tracking-analysis-mini-row">
                              <span>qualified</span>
                              <strong>{formatPlainNumber(summary.qualified_decisions)}</strong>
                              <small>{formatPlainNumber(summary.qualified_with_trailing_return)} with 20d history</small>
                            </div>
                            {block.cohortRows.map((row) => (
                              <div className="tracking-analysis-mini-row" key={`${block.label}-${row.cohort_key}`}>
                                <span>{row.label}</span>
                                <strong>
                                  {formatPlainNumber(row.count)} / {formatPercent(row.share)}
                                </strong>
                                <small>
                                  30d {formatSignedPercent(row.average_directional_return_30)} / lift {formatSignedPercent(row.lift_vs_same_date_universe_30)} / 20d {formatSignedPercent(row.average_trailing_return_20)}
                                </small>
                              </div>
                            ))}
                            <div className="tracking-analysis-mini-row">
                              <span>severe return</span>
                              <strong>{formatSignedPercent(severeCohort?.average_directional_return_30)}</strong>
                              <small>{formatPercent(severeCohort?.share)} of qualified</small>
                            </div>
                            <div className="tracking-analysis-mini-row">
                              <span>top setup</span>
                              <strong>{block.topSetupTypes[0]?.setup_type ?? "--"}</strong>
                              <small>
                                {formatPlainNumber(block.topSetupTypes[0]?.count)} / {formatSignedPercent(block.topSetupTypes[0]?.average_directional_return_30)}
                              </small>
                            </div>
                            <div className="tracking-analysis-mini-row">
                              <span>top regime</span>
                              <strong>{block.topRegimes[0]?.regime_tag ?? "--"}</strong>
                              <small>
                                {formatPlainNumber(block.topRegimes[0]?.count)} / {formatSignedPercent(block.topRegimes[0]?.average_directional_return_30)}
                              </small>
                            </div>
                            <div className="tracking-analysis-mini-row">
                              <span>top break</span>
                              <strong>{block.topBreakReasons[0]?.break_reason ?? "--"}</strong>
                              <small>
                                {formatPlainNumber(block.topBreakReasons[0]?.count)} / {formatSignedPercent(block.topBreakReasons[0]?.average_directional_return_30)}
                              </small>
                            </div>
                            {block.shockExamples.length > 0 ? (
                              block.shockExamples.map((example) => (
                                <div className="tracking-analysis-mini-row" key={`${block.label}-${example.dt}-${example.code}`}>
                                  <span>{formatYmdDisplay(example.dt)} {example.code}</span>
                                  <strong>
                                    {formatSignedPercent(example.trailing_return_20)} / {formatSignedPercent(example.return_30)}
                                  </strong>
                                  <small>{example.regime_tag ?? "--"} / {example.break_reason ?? "--"}</small>
                                </div>
                              ))
                            ) : (
                              <div className="tracking-analysis-empty-inline">shock example はまだありません。</div>
                            )}
                          </>
                        ) : (
                          <div className="tracking-analysis-empty-inline">shock analysis is not available yet.</div>
                        )}
                      </div>
                    </article>
                  );
                })}
              </div>
            </AnalysisSection>
          </section>
        ) : null}

        <section className={`tracking-table-shell ${view === "analysis" ? "is-hidden" : ""}`}>
          <div className="tracking-table-scroll">
            {view === "signal" ? (
              <div className="tracking-table" style={tableStyle}>
                <div className="tracking-row tracking-head">
                  {["判定日", "コード", "銘柄名", "売買", "起点価格", "現在損益", "30日成績", "最大有利", "最大不利", "シナリオ崩れ", "判定理由要約"].map(
                    (label, index) => (
                      <div
                        key={label}
                        className={`tracking-cell ${index === 0 ? "tracking-sticky-left" : ""} ${
                          index === 1 ? "tracking-sticky-left second" : ""
                        } ${index === 2 ? "tracking-sticky-left third" : ""}`}
                      >
                        {label}
                      </div>
                    )
                  )}
                </div>
                {signalItems.map((item, index) => (
                  <button
                    key={item.event_id}
                    type="button"
                    className={`tracking-row tracking-body-row ${index % 2 === 0 ? "is-even" : "is-odd"} ${
                      selectedSignalId === item.event_id ? "is-selected" : ""
                    }`}
                    onClick={() => {
                      setSelectedRankingId(null);
                      setRankingDetail(null);
                      setSelectedSignalId(item.event_id);
                    }}
                  >
                    <div className="tracking-cell tracking-sticky-left">{formatIsoDateLabel(item.signalDate)}</div>
                    <div className="tracking-cell tracking-sticky-left second">
                      <Link
                        className="tracking-code-link"
                        to={buildDetailLink(item.code, item.signalDate, item.side, logicVersion)}
                        onClick={(event) => event.stopPropagation()}
                      >
                        {item.code}
                      </Link>
                    </div>
                    <div className="tracking-cell tracking-sticky-left third">{item.name ?? "--"}</div>
                    <div className="tracking-cell">
                      <span
                        className={`rank-score-badge tracking-strength-badge ${tradeStrengthToneClass(item.priority_score)}`.trim()}
                      >
                        {formatTradeStrengthCaption(formatSignalStateLabel(item.side), item.priority_score)}
                      </span>
                    </div>
                    <div className="tracking-cell tracking-price-cell">
                      <strong>{formatPrice(item.anchor_price_close)}</strong>
                      <small>open {formatPrice(item.anchor_price_next_open)}</small>
                    </div>
                    <div className="tracking-cell tracking-return-cell">
                      <span className={`tracking-return-badge ${metricTone(item.current_directional_return)}`}>
                        {formatPercent(item.current_directional_return)}
                      </span>
                      <small>open基準 {formatPercent(item.current_exec_directional_return)}</small>
                    </div>
                    <div className="tracking-cell">{formatPercent(item.return_30d)}</div>
                    <div className="tracking-cell">{formatPercent(item.max_favorable_30)}</div>
                    <div className="tracking-cell">{formatPercent(item.max_adverse_30)}</div>
                    <div className="tracking-cell">
                      <span className={`tracking-status-pill is-${item.break_status === "broken" ? "archive" : item.status}`}>
                        {formatBreakLabel(item.break_status, item.break_reason)}
                      </span>
                    </div>
                    <div className="tracking-cell">{summarizeReasons(item.reason_summary)}</div>
                  </button>
                ))}
                {!signalItems.length && !loading ? <div className="tracking-empty">{signalEmptyMessage}</div> : null}
              </div>
            ) : (
              <div className="tracking-table" style={tableStyle}>
                <div className="tracking-row tracking-head is-ranking">
                  {["掲載日", "コード", "銘柄名", "順位", "掲載時売買判定", "掲載時スコア", "起点価格", "現在損益", "30日成績", "最大有利", "最大不利", "シナリオ崩れ"].map(
                    (label, index) => (
                      <div
                        key={label}
                        className={`tracking-cell ${index === 0 ? "tracking-sticky-left" : ""} ${
                          index === 1 ? "tracking-sticky-left second" : ""
                        } ${index === 2 ? "tracking-sticky-left third" : ""}`}
                      >
                        {label}
                      </div>
                    )
                  )}
                </div>
                {rankingItems.map((item, index) => (
                  <button
                    key={item.appearance_id}
                    type="button"
                    className={`tracking-row tracking-body-row is-ranking ${index % 2 === 0 ? "is-even" : "is-odd"} ${
                      selectedRankingId === item.appearance_id ? "is-selected" : ""
                    }`}
                    onClick={() => {
                      setSelectedSignalId(null);
                      setSignalDetail(null);
                      setSelectedRankingId(item.appearance_id);
                    }}
                  >
                    <div className="tracking-cell tracking-sticky-left">{formatIsoDateLabel(item.date_iso)}</div>
                    <div className="tracking-cell tracking-sticky-left second">
                      <Link
                        className="tracking-code-link"
                        to={buildDetailLink(
                          item.code,
                          item.date_iso,
                          item.dir === "down" ? "sell" : "buy",
                          "latest",
                          item.dir
                        )}
                        onClick={(event) => event.stopPropagation()}
                      >
                        {item.code}
                      </Link>
                    </div>
                    <div className="tracking-cell tracking-sticky-left third">{item.name ?? "--"}</div>
                    <div className="tracking-cell">{item.rank}位</div>
                    <div className="tracking-cell">
                      <span
                        className={`rank-score-badge tracking-strength-badge ${tradeStrengthToneClass(item.display_score)}`.trim()}
                      >
                        {formatTradeStrengthCaption(formatSignalStateLabel(item.signal_state_at_appearance), item.display_score)}
                      </span>
                    </div>
                    <div className="tracking-cell">{formatTradeStrengthPoints(item.display_score)}</div>
                    <div className="tracking-cell tracking-price-cell">
                      <strong>{formatPrice(item.anchor_price_close)}</strong>
                      <small>open {formatPrice(item.anchor_price_next_open)}</small>
                    </div>
                    <div className="tracking-cell tracking-return-cell">
                      <span className={`tracking-return-badge ${metricTone(item.current_directional_return)}`}>
                        {formatPercent(item.current_directional_return)}
                      </span>
                    </div>
                    <div className="tracking-cell">{formatPercent(item.return_30d)}</div>
                    <div className="tracking-cell">{formatPercent(item.max_favorable_30)}</div>
                    <div className="tracking-cell">{formatPercent(item.max_adverse_30)}</div>
                    <div className="tracking-cell">
                      <span className={`tracking-status-pill is-${item.break_status === "broken" ? "archive" : item.status}`}>
                        {formatBreakLabel(item.break_status, item.break_reason)}
                      </span>
                    </div>
                  </button>
                ))}
                {!rankingItems.length && !loading ? <div className="tracking-empty">{rankingEmptyMessage}</div> : null}
              </div>
            )}
          </div>
        </section>

        {view !== "analysis" && hasMore ? (
          <div style={{ display: "flex", justifyContent: "center", padding: "12px 0 0" }}>
            <button
              type="button"
              className="tracking-refresh-button"
              onClick={() => setPageOffset((current) => current + TRACKING_PAGE_SIZE)}
              disabled={loadingMore}
            >
              {loadingMore ? "続きを読み込み中..." : "さらに読み込む"}
            </button>
          </div>
        ) : null}

        {detailLoading ? <div className="tracking-empty">詳細を読み込み中...</div> : null}
      </main>

      <TrackingDrawer
        mode={view}
        signalDetail={signalDetail}
        rankingDetail={rankingDetail}
        onClose={() => {
          setSelectedSignalId(null);
          setSelectedRankingId(null);
          setSignalDetail(null);
          setRankingDetail(null);
        }}
      />
    </div>
  );
}
