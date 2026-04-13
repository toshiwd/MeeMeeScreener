export type TradexForecastSurfaceProjectionRow = {
  as_of_date: string | null;
  code: string;
  side: "long" | "short" | string;
  action_state: string;
  direction_prob: number | null;
  expected_ret_20: number | null;
  expected_upside: number;
  expected_downside: number;
  invalidation_price: number | null;
  setup_tags: string[];
  reason_codes: string[];
  opportunity_score: number | null;
  freshness_state: string;
};

export type TradexForecastSurfaceProjectionSummary = {
  as_of_date: string | null;
  model_version: string | null;
  universe_code_count: number | null;
  expected_row_count: number | null;
  actual_row_count: number | null;
  missing_row_count: number | null;
  coverage_ratio: number | null;
  feature_frame_version: string | null;
  market_opportunity_score_enabled: boolean;
  personal_fit_score_enabled: boolean;
  side_counts: Record<string, number>;
  action_counts: Record<string, number>;
  source_context_presence: Record<string, unknown>;
  alerts: string[];
  created_at: string | null;
};

export type TradexForecastSurfaceProjection = {
  summary: TradexForecastSurfaceProjectionSummary;
  long_rank: TradexForecastSurfaceProjectionRow[];
  short_rank: TradexForecastSurfaceProjectionRow[];
  high_risk_avoid: TradexForecastSurfaceProjectionRow[];
  watchlist_promotions: TradexForecastSurfaceProjectionRow[];
};

export type TradexForecastSurfaceProjectionEnvelope = {
  publish?: Record<string, unknown> | null;
  manifest?: Record<string, unknown> | null;
  projection: TradexForecastSurfaceProjection | null;
  degraded?: boolean;
  reason?: string | null;
  publish_id?: string | null;
  as_of_date?: string | null;
  freshness_state?: string | null;
};

export type TradexLiveStrategyJudgement = {
  status: string | null;
  reason: string | null;
  experiment_id: string | null;
  hypothesis_id: string | null;
  generated_at: string | null;
  target: {
    code: string | null;
    as_of_date: string | null;
    side: string | null;
    judgement_type: string | null;
  };
  primary_adapter_id: string | null;
  machine_action_state: string | null;
  human_readable_judgement: string | null;
  buy_score: number | null;
  environment_score: number | null;
  trend_score: number | null;
  trigger_score: number | null;
  risk_score: number | null;
  reason_codes: string[];
  authoritative_decision: string | null;
  authoritative_decision_path: string | null;
  strategy_judgement_path: string | null;
  experiment_manifest_path: string | null;
  is_buy_signal: boolean;
};

export type TradexBaseline = {
  logic_id: string | null;
  version: string | null;
  published_at: string | null;
  publish_id: string | null;
};

export type TradexMetricDeltas = {
  total_score_delta: number | null;
  max_drawdown_delta: number | null;
  sample_count_delta: number | null;
  win_rate_delta: number | null;
  expected_value_delta: number | null;
};

export type TradexRankingImpact = {
  current_rank: number | null;
  candidate_rank: number | null;
  rank_shift: number | null;
  score_delta: number | null;
  direction: "上昇" | "中立" | "下落";
  note: string;
};

export type TradexDecisionSummary = {
  headline: string;
  detail: string;
  suggested_action: "採用" | "保留" | "再検証";
  confidence: number | null;
};

export type TradexDiffVsCurrent = {
  comparison_snapshot_id: string;
  baseline_publish_id: string | null;
  metric_deltas: TradexMetricDeltas;
  ranking_impact: TradexRankingImpact;
  decision_summary: TradexDecisionSummary;
};

export type TradexValidationResult = {
  status: string;
  sample_count: number | null;
  expectancy_delta: number | null;
  win_rate: number | null;
  max_loss: number | null;
  notes: string[];
};

export type TradexAnomalyReport = {
  error_type: string;
  target: string;
  probable_causes: string[];
  impact_scope: string;
  suggested_fix: string;
  ai_prompt: string;
};

export type TradexCandidate = {
  candidate_id: string;
  logic_key: string;
  name: string;
  kind: string;
  status: string;
  validation_state: string;
  created_at: string | null;
  updated_at: string | null;
  logic_id: string | null;
  logic_version: string | null;
  logic_family: string | null;
  source_publish_id: string | null;
  readiness_pass: boolean;
  sample_count: number | null;
  expectancy_delta: number | null;
  has_snapshot: boolean;
  validation_summary: Record<string, unknown> | null;
  published_logic_manifest: Record<string, unknown> | null;
  published_logic_artifact: Record<string, unknown> | null;
  published_ranking_snapshot: Record<string, unknown> | null;
  comparison_snapshot: TradexDiffVsCurrent;
  comparison_snapshot_id: string;
  validation_result: TradexValidationResult;
  anomaly_report: TradexAnomalyReport | null;
};

export type TradexSummaryStrip = {
  as_of_date: string | null;
  freshness_state: string | null;
  replay_status: string | null;
  replay_phase: string | null;
  attention_count: number;
  candidate_count: number;
  champion_logic_key: string | null;
  publish_id: string | null;
  authoritative_state: string | null;
  authoritative_decision: string | null;
  authoritative_ready: boolean;
};

export type TradexBootstrapData = {
  baseline: TradexBaseline;
  summary: TradexSummaryStrip;
  candidates: TradexCandidate[];
  live_strategy_judgement: TradexLiveStrategyJudgement | null;
  forecast_surface_projection: TradexForecastSurfaceProjectionEnvelope | null;
  raw: {
    analysis_status: Record<string, unknown> | null;
    runtime_selection: Record<string, unknown> | null;
    publish_state: Record<string, unknown> | null;
    publish_queue: Record<string, unknown> | null;
    replay_progress: Record<string, unknown> | null;
    action_queue: Record<string, unknown> | null;
    live_strategy_judgement: Record<string, unknown> | null;
    forecast_surface_projection: Record<string, unknown> | null;
  };
};

export type TradexAdoptRequest = {
  candidate_id: string;
  baseline_publish_id: string;
  comparison_snapshot_id: string;
  reason?: string | null;
  actor?: string | null;
};

export type TradexAdoptResponse = {
  ok: boolean;
  candidate_id: string;
  logic_key: string;
  baseline_publish_id: string | null;
  comparison_snapshot_id: string;
  result: Record<string, unknown>;
};
