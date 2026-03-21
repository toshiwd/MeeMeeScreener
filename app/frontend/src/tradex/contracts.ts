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
  direction: "改善" | "維持" | "悪化";
  note: string;
};

export type TradexDecisionSummary = {
  headline: string;
  detail: string;
  suggested_action: "比較済み" | "保留" | "要再検証";
  confidence: number | null;
};

export type TradexDiffVsCurrent = {
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
};

export type TradexBootstrapData = {
  baseline: TradexBaseline;
  summary: TradexSummaryStrip;
  candidates: TradexCandidate[];
  raw: {
    analysis_status: Record<string, unknown> | null;
    runtime_selection: Record<string, unknown> | null;
    publish_state: Record<string, unknown> | null;
    publish_queue: Record<string, unknown> | null;
    replay_progress: Record<string, unknown> | null;
    action_queue: Record<string, unknown> | null;
  };
};

