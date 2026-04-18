import { tradexFetchJson } from "./http";

export type TradexReplayRunRequest = {
  run_id?: string | null;
  suite_id?: string | null;
  policy_id?: string | null;
  policy_version?: string | null;
  window_start_date?: string | null;
  window_start_dates?: string[];
  window_months?: number;
  universe?: string[];
  market_benchmark_symbol?: string | null;
  capital?: Record<string, unknown>;
  scoring?: Record<string, unknown>;
  policy?: Record<string, unknown>;
  unit_scale?: number | null;
  addon_units?: number[];
  execution_convention?: string | null;
  weekly_activity_required?: boolean;
  short_cash_reusable?: boolean;
  selection_rule_change_log?: Record<string, unknown>[];
};

export type TradexReplayRun = {
  ok: boolean;
  run_id: string;
  run_config: Record<string, unknown>;
  daily_selection_snapshot: { items: Array<Record<string, unknown>> };
  feature_snapshot: { items: Array<Record<string, unknown>> };
  positions_timeline: { items: Array<Record<string, unknown>> };
  trade_ledger: { items: Array<Record<string, unknown>> };
  daily_equity_curve: { items: Array<Record<string, unknown>> };
  benchmark_market: Record<string, unknown>;
  benchmark_universe: Record<string, unknown>;
  relative_performance: Record<string, unknown>;
  window_summary: Record<string, unknown>;
  selection_rule_change_log: { items: Array<Record<string, unknown>> };
};

export const loadTradexReplayRun = (runId: string) =>
  tradexFetchJson<TradexReplayRun>(`/tradex/replay/runs/${encodeURIComponent(runId)}`);

export const createTradexReplayRun = (payload: TradexReplayRunRequest) =>
  tradexFetchJson<Record<string, unknown>>("/tradex/replay/runs", {
    method: "POST",
    body: JSON.stringify(payload),
  });

