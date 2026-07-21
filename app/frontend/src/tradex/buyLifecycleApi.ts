import { tradexFetchJson } from "./http";
import { TRADEX_RESEARCH_ENDPOINTS, tradexResearchRoute } from "./researchRoutes";

export type BuyLifecycleCandidate = {
  code: string; as_of_date: string | null; lifecycle_rank: number | null; entry_state: string;
  held_position_review_state: string; entry_actionability_score: number | null; upside_probability_20d: number | null;
  downside_risk_probability_20d: number | null; lifecycle_reasons: string[];
};
export type BuyLifecycleBoard = {
  available: boolean; reason?: string; artifact_path?: string; authoritative_decision?: string;
  counts?: { total_candidates?: number; entry_state_counts?: Record<string, number> }; candidates: BuyLifecycleCandidate[];
};
export async function loadBuyLifecycleBoard(limit = 100): Promise<BuyLifecycleBoard> {
  return tradexFetchJson<BuyLifecycleBoard>(`${tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.buyLifecycleBoard)}?limit=${limit}`);
}

export type ShapeEntryRow = {
  selection_rank: number; code: string; confirmed_signal_date?: string | null; shape_signal: string;
  new_entry_verdict: string; entry_condition: string; entry_status: string; confirmed_close?: number | null;
  realized_vol20?: number | null; market_breadth?: number | null; gap_ma60?: number | null; research_status: string;
};
export type ShapeEntryBoard = {
  available: boolean; reason?: string; artifact_path?: string; confirmed_signal_date?: string | null;
  default_verdict?: string; candidate_count?: number; research_status?: string; automatic_trading?: boolean;
  production_ranking_changed?: boolean; runtime_db_write?: boolean;
  selection_contract?: { realized_vol20_ceiling?: number; take_profit?: number; stop_loss?: number; maximum_holding_sessions?: number };
  quality_metrics_2026?: { sample_count_2026_completed?: number; win_rate?: number; average_win?: number;
    average_loss?: number; payoff_ratio?: number; expectancy?: number; profit_factor?: number };
  candidates: ShapeEntryRow[];
};
export async function loadShapeEntryBoard(limit = 30): Promise<ShapeEntryBoard> {
  return tradexFetchJson<ShapeEntryBoard>(`${tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.shapeEntryBoard)}?limit=${limit}`);
}

export type AdaptiveRuleState = {
  rule: string; state: "Active" | "Secondary" | "Watch" | "Dormant"; score: number;
  pf20?: number | null; expectancy20?: number | null; pf60?: number | null;
  same_regime_pf?: number | null; regime_permission_allowed?: boolean;
};
export type AdaptiveCandidate = {
  code: string; side: string; signal_date: string; confirmed_close?: number | null; entry_condition?: string;
  router_rule: string; router_state: string; router_score?: number | null; router_priority_rank?: number | null;
  router_verdict: string;
};
export type AdaptiveRuleBoard = {
  available: boolean; reason?: string; artifact_path?: string; current_as_of?: string; current_regime?: string;
  selected_policy?: { policy?: string; top_rules?: number };
  quality_2026?: { metrics?: { event_count?: number; daily_profit_factor?: number; daily_expectancy?: number }; weekly_coverage?: { average_events_per_calendar_week?: number } };
  adoption_gate?: { pass?: boolean }; current_rule_states: AdaptiveRuleState[];
  current_active_rule_priority: AdaptiveRuleState[]; current_candidates: AdaptiveCandidate[];
  automatic_trading?: boolean; production_ranking_changed?: boolean; runtime_db_write?: boolean;
};
export async function loadAdaptiveRuleBoard(limit = 30): Promise<AdaptiveRuleBoard> {
  return tradexFetchJson<AdaptiveRuleBoard>(`${tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.adaptiveRuleBoard)}?limit=${limit}`);
}
