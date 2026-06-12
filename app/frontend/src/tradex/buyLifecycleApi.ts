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
