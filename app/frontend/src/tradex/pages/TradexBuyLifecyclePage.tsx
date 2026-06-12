import { useEffect, useMemo, useState } from "react";
import { loadBuyLifecycleBoard, type BuyLifecycleBoard } from "../buyLifecycleApi";

const STATES = ["Starter", "Accumulate", "Wait", "Avoid"];
const LABELS: Record<string, string> = { Starter: "打診", Accumulate: "追加検討", Wait: "監視", Avoid: "回避", Hold: "保有継続参考", HoldCaution: "保有注意参考", ExitReview: "撤退確認参考" };
const pct = (value: number | null) => typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
const tone = (state: string) => state === "Starter" || state === "Accumulate" || state === "Hold" ? "is-ok" : state === "Avoid" || state === "ExitReview" ? "is-warn" : "is-muted";

export default function TradexBuyLifecyclePage() {
  const [board, setBoard] = useState<BuyLifecycleBoard | null>(null);
  const [state, setState] = useState("All");
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { loadBuyLifecycleBoard().then(setBoard).catch((reason: unknown) => setError(reason instanceof Error ? reason.message : "request failed")); }, []);
  const rows = useMemo(() => state === "All" ? board?.candidates ?? [] : (board?.candidates ?? []).filter((row) => row.entry_state === state), [board, state]);
  const counts = board?.counts?.entry_state_counts ?? {};
  const total = board?.counts?.total_candidates ?? 0;
  return <div className="tradex-page">
    <section className="tradex-panel">
      <div className="tradex-panel-head"><div><div className="tradex-panel-title">Buy lifecycle board</div><div className="tradex-panel-caption">{board?.artifact_path ?? board?.reason ?? "current_buy_lifecycle_board_v1"}</div></div><span className="tradex-pill is-ok">review-only</span></div>
      {error ? <div className="tradex-inline-error">{error}</div> : null}
      <div className="tradex-panel-actions tradex-state-filter">{["All", ...STATES].map((item) => <button key={item} type="button" className={`tradex-nav-item ${state === item ? "active" : ""}`} onClick={() => setState(item)}>{item === "All" ? "All" : LABELS[item]} <span className="tradex-table-sub">{item === "All" ? total : counts[item] ?? 0}</span></button>)}</div>
    </section>
    <section className="tradex-panel">
      <div className="tradex-panel-caption">entry state is for unheld candidates. Held review is reference-only because no position ledger is attached.</div>
      <div className="tradex-table-wrap"><table className="tradex-table tradex-short-lifecycle-table"><thead><tr><th>rank</th><th>entry state</th><th>code</th><th>as-of</th><th>entry score</th><th>upside 20d</th><th>downside risk</th><th>held review reference</th><th>reasons</th></tr></thead><tbody>
        {rows.map((row) => <tr key={`${row.code}-${row.lifecycle_rank}`}><td>{row.lifecycle_rank ?? "--"}</td><td><span className={`tradex-pill ${tone(row.entry_state)}`}>{LABELS[row.entry_state] ?? row.entry_state}</span></td><td><strong>{row.code}</strong></td><td>{row.as_of_date ?? "--"}</td><td>{row.entry_actionability_score?.toFixed(3) ?? "--"}</td><td>{pct(row.upside_probability_20d)}</td><td>{pct(row.downside_risk_probability_20d)}</td><td><span className={`tradex-pill ${tone(row.held_position_review_state)}`}>{LABELS[row.held_position_review_state] ?? row.held_position_review_state}</span></td><td>{row.lifecycle_reasons.join(" / ") || "--"}</td></tr>)}
        {!rows.length ? <tr><td colSpan={9}>No rows</td></tr> : null}
      </tbody></table></div>
    </section>
  </div>;
}
