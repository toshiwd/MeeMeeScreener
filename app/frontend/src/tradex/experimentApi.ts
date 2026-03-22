import { tradexFetchJson } from "./http";

export type TradexPeriodSegment = {
  start_date: string;
  end_date: string;
  label?: string | null;
};

export type TradexPlanSpec = {
  plan_id: string;
  plan_version?: string | null;
  label?: string | null;
  minimum_confidence?: number | null;
  minimum_ready_rate?: number | null;
  signal_bias?: string | null;
  top_k?: number | null;
  notes?: string | null;
};

export type TradexFamily = {
  schema_version: string;
  family_id: string;
  family_name: string;
  created_at: string;
  frozen: boolean;
  frozen_at: string | null;
  universe: string[];
  period: { segments: TradexPeriodSegment[] };
  confirmed_only: boolean;
  input_dataset_version: string;
  code_revision: string;
  timezone: string;
  price_source: string;
  data_cutoff_at: string;
  random_seed: number;
  baseline_plan: TradexPlanSpec;
  candidate_plans: TradexPlanSpec[];
  candidate_limit: number;
  run_ids: string[];
  baseline_run_id: string | null;
  candidate_run_ids: string[];
  notes: string;
  status_summary: Record<string, unknown>;
};

export type TradexRun = {
  schema_version: string;
  family_id: string;
  run_id: string;
  run_kind: "baseline" | "candidate";
  plan_id: string;
  plan_version: string;
  baseline_version: string;
  status: string;
  started_at: string;
  completed_at: string | null;
  error: string | null;
  universe: string[];
  period: { segments: TradexPeriodSegment[] };
  confirmed_only: boolean;
  input_dataset_version: string;
  timezone: string;
  price_source: string;
  data_cutoff_at: string;
  random_seed: number;
  notes: string;
  metrics: Record<string, unknown>;
  summary: Record<string, unknown>;
  analysis: Record<string, unknown>;
  adopt?: Record<string, unknown> | null;
};

export type TradexCompare = {
  schema_version: string;
  family_id: string;
  generated_at: string;
  baseline_run_id: string;
  candidate_results: TradexCompareCandidateResult[];
};

export type TradexCompareCandidateResult = {
  run_id: string;
  plan_id: string;
  plan_version: string;
  status: string;
  metric_directions?: Record<string, string>;
  baseline_absolute?: Record<string, unknown>;
  candidate_absolute?: Record<string, unknown>;
  absolute_metric_comparisons?: Array<Record<string, unknown>>;
  primary_metric_deltas?: Record<string, unknown>;
  target_symbol_count_delta?: number;
  signal_date_deltas?: Record<string, unknown>;
  winning_examples?: Array<Record<string, unknown>>;
  losing_examples?: Array<Record<string, unknown>>;
  top_conditions?: Array<Record<string, unknown>>;
  review_focus?: Array<Record<string, unknown>>;
  symbol_summary?: Record<string, unknown>;
};

export type TradexDetail = {
  schema_version: string;
  family_id: string;
  run_id: string;
  run_kind: string;
  plan_id: string;
  plan_version: string;
  code: string;
  generated_at: string;
  summary: Record<string, unknown>;
  examples: Record<string, unknown>;
  samples: Array<Record<string, unknown>>;
};

export type TradexAdoptResult = {
  ok: boolean;
  family_id: string;
  run_id: string;
  status: string;
  gate: Record<string, unknown>;
  adopt: Record<string, unknown>;
  compare: Record<string, unknown>;
};

export type TradexCreateFamilyRequest = {
  family_id?: string | null;
  family_name?: string | null;
  universe: string[];
  period: { segments: TradexPeriodSegment[] };
  baseline_plan: TradexPlanSpec;
  candidate_plans: TradexPlanSpec[];
  confirmed_only?: boolean;
  input_dataset_version?: string | null;
  code_revision?: string | null;
  timezone?: string | null;
  price_source?: string | null;
  data_cutoff_at?: string | null;
  random_seed?: number | null;
  notes?: string | null;
};

export type TradexCreateRunRequest = {
  run_kind: "baseline" | "candidate";
  plan_id?: string | null;
  notes?: string | null;
};

export type TradexAdoptRequest = {
  family_id?: string | null;
  run_id?: string | null;
  candidate_id?: string | null;
  baseline_publish_id?: string | null;
  comparison_snapshot_id?: string | null;
  reason?: string | null;
  actor?: string | null;
};

const api = <T,>(url: string, init?: RequestInit) => tradexFetchJson<T>(url, init);

export function loadTradexFamilies() {
  return api<{ ok: boolean; items: TradexFamily[] }>("/tradex/families");
}

export function createTradexFamily(payload: TradexCreateFamilyRequest) {
  return api<{ ok: boolean; family: TradexFamily }>("/tradex/families", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function loadTradexFamily(familyId: string) {
  return api<{ ok: boolean; family: TradexFamily }>(`/tradex/families/${encodeURIComponent(familyId)}`);
}

export function createTradexRun(familyId: string, payload: TradexCreateRunRequest) {
  return api<{ ok: boolean; run: TradexRun }>(`/tradex/families/${encodeURIComponent(familyId)}/runs`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function loadTradexFamilyCompare(familyId: string) {
  return api<{ ok: boolean; compare: TradexCompare }>(`/tradex/families/${encodeURIComponent(familyId)}/compare`);
}

export function loadTradexRun(runId: string) {
  return api<{ ok: boolean; run: TradexRun }>(`/tradex/runs/${encodeURIComponent(runId)}`);
}

export function loadTradexRunCompare(runId: string) {
  return api<{ ok: boolean; compare: Record<string, unknown> }>(`/tradex/runs/${encodeURIComponent(runId)}/compare`);
}

export function loadTradexRunDetail(runId: string, code: string) {
  return api<{ ok: boolean; detail: TradexDetail }>(`/tradex/runs/${encodeURIComponent(runId)}/detail?code=${encodeURIComponent(code)}`);
}

export function adoptTradexRun(payload: TradexAdoptRequest) {
  return api<TradexAdoptResult>("/tradex/adopt", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
