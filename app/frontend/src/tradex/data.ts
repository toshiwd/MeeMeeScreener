import { tradexFetchJson, tradexFetchJsonWithRetry } from "./http";
import type {
  TradexAnomalyReport,
  TradexBaseline,
  TradexBootstrapData,
  TradexCandidate,
  TradexDiffVsCurrent,
  TradexDecisionSummary,
  TradexMetricDeltas,
  TradexRankingImpact,
  TradexSummaryStrip,
  TradexValidationResult
} from "./contracts";

type AnyRecord = Record<string, unknown>;

type RuntimeSelectionSnapshot = {
  selected_logic_id?: string | null;
  selected_logic_version?: string | null;
  logic_key?: string | null;
  source_of_truth?: string | null;
  registry_sync_state?: string | null;
  last_sync_time?: string | null;
  maintenance_state?: AnyRecord | null;
  candidate_backfill_last_run?: AnyRecord | null;
  snapshot_sweep_last_run?: AnyRecord | null;
  non_promotable_legacy_count?: number | null;
  maintenance_degraded?: boolean;
  operator_mutation_observability?: AnyRecord | null;
};

type PublishStateSnapshot = {
  source_of_truth?: string | null;
  registry_sync_state?: string | null;
  degraded?: boolean;
  last_sync_time?: string | null;
  bootstrap_rule?: string | null;
  default_logic_pointer?: string | null;
  champion?: AnyRecord | null;
  challengers?: AnyRecord[];
  champion_logic_key?: string | null;
  challenger_logic_keys?: string[];
  previous_stable_champion_logic_key?: string | null;
  external_registry_version?: string | null;
  local_mirror_version?: string | null;
  mirror_schema_version?: string | null;
  mirror_normalized?: boolean;
  candidate_backfill_last_run?: AnyRecord | null;
  snapshot_sweep_last_run?: AnyRecord | null;
  non_promotable_legacy_count?: number | null;
  maintenance_degraded?: boolean;
  maintenance_state?: AnyRecord | null;
  operator_mutation_observability?: AnyRecord | null;
};

type AnalysisBridgeStatus = {
  publish?: {
    publish_id?: string | null;
    as_of_date?: string | null;
    published_at?: string | null;
    freshness_state?: string | null;
  } | null;
  manifest?: {
    publish_id?: string | null;
    as_of_date?: string | null;
    published_at?: string | null;
    freshness_state?: string | null;
  } | null;
  public_table_counts?: Record<string, number>;
  degraded?: boolean;
  reason?: string | null;
};

type ActionQueueItem = {
  kind: string;
  priority: number;
  title: string;
  label: string;
  side?: string | null;
  strategy_tag?: string | null;
  holding_band?: string | null;
  metric_label?: string | null;
  metric_value?: number | null;
  note?: string | null;
};

type ActionQueueResponse = {
  degraded?: boolean;
  degrade_reason?: string | null;
  publish_id?: string | null;
  as_of_date?: string | null;
  freshness_state?: string | null;
  actions: ActionQueueItem[];
};

type ReplayProgressRun = {
  replay_id: string;
  status: string;
  start_as_of_date: string;
  end_as_of_date: string;
  total_days: number;
  completed_days: number;
  progress_pct: number;
  current_phase?: string | null;
  current_publish_id?: string | null;
  last_completed_as_of_date?: string | null;
};

type ReplayProgressResponse = {
  running: boolean;
  current_run: ReplayProgressRun | null;
  recent_runs: ReplayProgressRun[];
};

type CandidateBundle = {
  candidate_id: string;
  logic_key: string;
  logic_id?: string | null;
  logic_version?: string | null;
  logic_family?: string | null;
  status?: string | null;
  validation_state?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  source_publish_id?: string | null;
  validation_summary?: AnyRecord | null;
  published_logic_manifest?: AnyRecord | null;
  published_logic_artifact?: AnyRecord | null;
  published_ranking_snapshot?: AnyRecord | null;
};

type CandidateCatalogResponse = {
  ok?: boolean;
  items?: CandidateBundle[];
  count?: number;
};

const text = (value: unknown, fallback = "") => {
  const result = typeof value === "string" ? value.trim() : String(value ?? "").trim();
  return result || fallback;
};

const num = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const obj = (value: unknown): AnyRecord | null => (value && typeof value === "object" && !Array.isArray(value) ? (value as AnyRecord) : null);

const toCandidate = (bundle: CandidateBundle): TradexCandidate => {
  const summary = obj(bundle.validation_summary);
  const metrics = obj(summary?.metrics);
  const readinessPass = Boolean(metrics?.readiness_pass);
  const sampleCount = num(metrics?.sample_count);
  const expectancyDelta = num(metrics?.expectancy_delta);
  const winRate = num(metrics?.win_rate);
  const maxLoss = num(metrics?.max_drawdown_pct ?? metrics?.adverse_move_mean ?? metrics?.adverse_move);
  const validationResult: TradexValidationResult = {
    status: text(bundle.validation_state ?? bundle.status, "未接続"),
    sample_count: sampleCount,
    expectancy_delta: expectancyDelta,
    win_rate: winRate,
    max_loss: maxLoss,
    notes: Array.isArray(summary?.notes) ? summary.notes.map((item) => text(item)).filter(Boolean).slice(0, 4) : []
  };
  const comparisonSnapshot = buildComparisonSnapshot(bundle);
  const anomalyReport = buildAnomalyReport(bundle, validationResult, comparisonSnapshot);
  return {
    candidate_id: text(bundle.candidate_id, text(bundle.logic_key, "unknown")),
    logic_key: text(bundle.logic_key, text(bundle.candidate_id, "unknown")),
    name: text(bundle.logic_family, text(bundle.logic_key, "候補")),
    kind: text(bundle.logic_family, "候補"),
    status: text(bundle.status, "unknown"),
    validation_state: text(bundle.validation_state, "unknown"),
    created_at: bundle.created_at ?? null,
    updated_at: bundle.updated_at ?? null,
    logic_id: bundle.logic_id ?? null,
    logic_version: bundle.logic_version ?? null,
    logic_family: bundle.logic_family ?? null,
    source_publish_id: bundle.source_publish_id ?? null,
    readiness_pass: readinessPass,
    sample_count: sampleCount,
    expectancy_delta: expectancyDelta,
    has_snapshot: Boolean(bundle.published_ranking_snapshot),
    validation_summary: bundle.validation_summary ?? null,
    published_logic_manifest: bundle.published_logic_manifest ?? null,
    published_logic_artifact: bundle.published_logic_artifact ?? null,
    published_ranking_snapshot: bundle.published_ranking_snapshot ?? null,
    comparison_snapshot: {
      ...comparisonSnapshot,
      decision_summary: {
        ...comparisonSnapshot.decision_summary,
        confidence: readinessPass ? 0.7 : 0.35
      }
    },
    validation_result,
    anomaly_report: anomalyReport
  };
};

const buildMetricDeltas = (bundle: CandidateBundle): TradexMetricDeltas => {
  const summary = obj(bundle.validation_summary);
  const metrics = obj(summary?.metrics);
  return {
    total_score_delta: num(metrics?.total_score_delta ?? metrics?.score_delta ?? metrics?.expectancy_delta),
    max_drawdown_delta: num(metrics?.max_drawdown_delta ?? metrics?.adverse_move_delta ?? metrics?.max_drawdown_pct_delta),
    sample_count_delta: num(metrics?.sample_count_delta ?? null),
    win_rate_delta: num(metrics?.win_rate_delta ?? null),
    expected_value_delta: num(metrics?.expected_value_delta ?? metrics?.expectancy_delta)
  };
};

const buildComparisonSnapshot = (bundle: CandidateBundle): TradexDiffVsCurrent => {
  const summary = obj(bundle.validation_summary);
  const metrics = obj(summary?.metrics);
  const metricDeltas = buildMetricDeltas(bundle);
  const readinessPass = Boolean(metrics?.readiness_pass);
  const improvedExpectancy = Boolean(metrics?.improved_expectancy);
  const sampleCount = num(metrics?.sample_count);
  const expectancyDelta = num(metrics?.expectancy_delta);
  const rankShift = num(metrics?.rank_shift ?? metrics?.ranking_impact ?? null);
  const scoreDelta = num(metrics?.total_score_delta ?? metrics?.score_delta ?? null);
  const direction: TradexRankingImpact["direction"] = improvedExpectancy ? "改善" : readinessPass ? "維持" : "悪化";
  const decisionSummary: TradexDecisionSummary = {
    headline: readinessPass ? "差分確認待ち" : "再検証が必要",
    detail: readinessPass
      ? "backend enforcement は未接続のため、採用は保留として扱う。"
      : "現行版との差分と検証結果を先に見直す。",
    suggested_action: readinessPass ? "比較済み" : "要再検証",
    confidence: sampleCount && sampleCount > 0 ? Math.min(0.95, Math.max(0.25, sampleCount / 100)) : null
  };
  return {
    baseline_publish_id: bundle.source_publish_id ?? null,
    metric_deltas: {
      ...metricDeltas,
      expected_value_delta: metricDeltas.expected_value_delta ?? expectancyDelta
    },
    ranking_impact: {
      current_rank: null,
      candidate_rank: null,
      rank_shift: Number.isFinite(rankShift ?? NaN) ? (rankShift as number) : null,
      score_delta: Number.isFinite(scoreDelta ?? NaN) ? (scoreDelta as number) : null,
      direction,
      note: readinessPass
        ? `期待値差 ${expectancyDelta == null ? "--" : expectancyDelta.toFixed(4)} / 件数 ${sampleCount ?? "--"}`
        : "比較未完了"
    },
    decision_summary: decisionSummary
  };
};

const buildAnomalyReport = (
  bundle: CandidateBundle,
  validationResult: TradexValidationResult,
  comparisonSnapshot: TradexDiffVsCurrent
): TradexAnomalyReport | null => {
  if (validationResult.status.toLowerCase() === "healthy" || validationResult.status.toLowerCase() === "ready") {
    return null;
  }
  return {
    error_type: validationResult.status || "candidate_validation_pending",
    target: bundle.logic_key,
    probable_causes: [
      validationResult.sample_count == null ? "sample_count_missing" : "",
      comparisonSnapshot.metric_deltas.expected_value_delta == null ? "expected_value_delta_missing" : "",
      comparisonSnapshot.decision_summary.suggested_action === "要再検証" ? "comparison_not_ready" : ""
    ].filter(Boolean),
    impact_scope: "候補比較 / 反映判定 / 候補詳細",
    suggested_fix: "validation_summary.metrics を再生成し、比較用差分 DTO を埋める。",
    ai_prompt:
      `TRADEX の候補比較処理で異常が発生。対象は ${bundle.logic_key}。` +
      `症状は ${validationResult.status}。期待する正常動作は候補比較と現行版差分が一貫して表示されること。` +
      "原因候補は validation_summary.metrics の欠損か集計未完了。影響範囲は候補比較と反映判定。再現条件は候補詳細を開いたときに差分値が null になること。"
  };
};

const buildBaseline = (
  analysisStatus: AnalysisBridgeStatus,
  runtimeSelection: RuntimeSelectionSnapshot,
  publishState: PublishStateSnapshot
): TradexBaseline => {
  const publish = analysisStatus.publish ?? analysisStatus.manifest ?? null;
  return {
    logic_id: text(runtimeSelection.selected_logic_id ?? publishState.champion_logic_key ?? publishState.default_logic_pointer, null as unknown as string | null) || null,
    version: text(runtimeSelection.selected_logic_version ?? publishState.external_registry_version, null as unknown as string | null) || null,
    published_at: text(publish?.published_at ?? publishState.last_sync_time, null as unknown as string | null) || null,
    publish_id: text(publish?.publish_id, null as unknown as string | null) || null
  };
};

const buildSummary = (
  analysisStatus: AnalysisBridgeStatus,
  actionQueue: ActionQueueResponse,
  replayProgress: ReplayProgressResponse,
  publishState: PublishStateSnapshot,
  candidates: TradexCandidate[]
): TradexSummaryStrip => {
  const publish = analysisStatus.publish ?? analysisStatus.manifest ?? null;
  const current = replayProgress.current_run;
  return {
    as_of_date: text(publish?.as_of_date, null as unknown as string | null) || null,
    freshness_state: text(publish?.freshness_state ?? publishState.registry_sync_state, null as unknown as string | null) || null,
    replay_status: current ? `${current.status}${current.current_phase ? ` / ${current.current_phase}` : ""}` : "待機",
    replay_phase: current?.current_phase ?? null,
    attention_count: Array.isArray(actionQueue.actions) ? actionQueue.actions.length : 0,
    candidate_count: candidates.length,
    champion_logic_key: text(publishState.champion_logic_key ?? publishState.default_logic_pointer, null as unknown as string | null) || null,
    publish_id: text(publish?.publish_id, null as unknown as string | null) || null
  };
};

export async function loadTradexBootstrap(): Promise<TradexBootstrapData> {
  const [analysisStatus, runtimeSelection, publishState, publishQueue, replayProgress, actionQueue, candidateCatalog] = await Promise.all([
    tradexFetchJson<AnalysisBridgeStatus>("/analysis-bridge/status"),
    tradexFetchJsonWithRetry<RuntimeSelectionSnapshot>("/system/runtime-selection"),
    tradexFetchJsonWithRetry<PublishStateSnapshot>("/system/publish/state"),
    tradexFetchJsonWithRetry<Record<string, unknown>>("/system/publish/queue"),
    tradexFetchJson<ReplayProgressResponse>("/analysis-bridge/internal/replay-progress"),
    tradexFetchJson<ActionQueueResponse>("/analysis-bridge/internal/state-eval-action-queue"),
    tradexFetchJsonWithRetry<CandidateCatalogResponse>("/system/publish/candidates")
  ]);
  const candidates = (candidateCatalog.items ?? []).map(toCandidate);
  const baseline = buildBaseline(analysisStatus, runtimeSelection, publishState);
  const summary = buildSummary(analysisStatus, actionQueue, replayProgress, publishState, candidates);
  return {
    baseline,
    summary,
    candidates,
    raw: {
      analysis_status: analysisStatus as unknown as Record<string, unknown>,
      runtime_selection: runtimeSelection as unknown as Record<string, unknown>,
      publish_state: publishState as unknown as Record<string, unknown>,
      publish_queue: publishQueue,
      replay_progress: replayProgress as unknown as Record<string, unknown>,
      action_queue: actionQueue as unknown as Record<string, unknown>
    }
  };
}

export function findTradexCandidate(candidates: TradexCandidate[], candidateId: string | null | undefined) {
  const normalized = text(candidateId, "");
  if (!normalized) return null;
  return (
    candidates.find((candidate) => candidate.candidate_id === normalized) ??
    candidates.find((candidate) => candidate.logic_key === normalized) ??
    null
  );
}

export function buildComparisonDraft(baseline: TradexBaseline, candidate: TradexCandidate): TradexDiffVsCurrent {
  const snapshot = candidate.comparison_snapshot;
  return {
    baseline_publish_id: snapshot.baseline_publish_id ?? baseline.publish_id,
    metric_deltas: snapshot.metric_deltas,
    ranking_impact: snapshot.ranking_impact,
    decision_summary: snapshot.decision_summary
  };
}

