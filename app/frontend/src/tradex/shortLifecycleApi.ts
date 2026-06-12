import { tradexFetchJson } from "./http";
import { TRADEX_RESEARCH_ENDPOINTS, tradexResearchRoute } from "./researchRoutes";

export type ShortLifecycleCandidate = {
  code: string;
  name: string | null;
  signal_ymd: string | null;
  lifecycle_rank: number | null;
  lifecycle_state: string;
  lifecycle_rank_score: number | null;
  original_rank: number | null;
  original_score: number | null;
  final_review_status: string | null;
  setup_state: string | null;
  continuation_status: string | null;
  expected_downside_pct: number | null;
  risk_reward_to_sl8: number | null;
  base_target_actionability: string | null;
  regime_permission_status: string | null;
  advancers_ratio: number | null;
  visual_micro_label: string | null;
  lifecycle_reasons: string[];
  profit_target_rule: string;
  stop_loss_rule: string;
};

export type ShortLifecycleBoard = {
  available: boolean;
  reason?: string;
  artifact_root?: string;
  artifact_path?: string;
  run_id?: string;
  created_at?: string;
  authoritative_decision?: string;
  counts?: {
    total_candidates?: number;
    lifecycle_state_counts?: Record<string, number>;
    [key: string]: unknown;
  };
  classification_contract?: Record<string, unknown>;
  source_artifact_paths?: Record<string, unknown>;
  runtime_db_write?: boolean;
  meemee_modified?: boolean;
  production_ranking_modified?: boolean;
  candidates: ShortLifecycleCandidate[];
};

export async function loadShortLifecycleBoard(limit = 30): Promise<ShortLifecycleBoard> {
  const params = new URLSearchParams({ limit: String(limit) });
  return tradexFetchJson<ShortLifecycleBoard>(
    `${tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.shortLifecycleBoard)}?${params.toString()}`
  );
}
