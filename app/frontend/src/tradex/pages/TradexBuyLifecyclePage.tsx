import { useEffect, useMemo, useState } from "react";
import { loadAdaptiveRuleBoard, loadBuyLifecycleBoard, loadShapeEntryBoard, type AdaptiveRuleBoard, type BuyLifecycleBoard, type ShapeEntryBoard } from "../buyLifecycleApi";

const STATES = ["Starter", "Accumulate", "Wait", "Avoid"];
const LABELS: Record<string, string> = { Starter: "打診", Accumulate: "追加検討", Wait: "監視", Avoid: "回避", Hold: "保有継続参考", HoldCaution: "保有注意参考", ExitReview: "撤退確認参考" };
const pct = (value: number | null) => typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
const tone = (state: string) => state === "Starter" || state === "Accumulate" || state === "Hold" ? "is-ok" : state === "Avoid" || state === "ExitReview" ? "is-warn" : "is-muted";

export default function TradexBuyLifecyclePage() {
  const [board, setBoard] = useState<BuyLifecycleBoard | null>(null);
  const [shapeBoard, setShapeBoard] = useState<ShapeEntryBoard | null>(null);
  const [adaptiveBoard, setAdaptiveBoard] = useState<AdaptiveRuleBoard | null>(null);
  const [state, setState] = useState("All");
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    Promise.all([loadBuyLifecycleBoard(), loadShapeEntryBoard(), loadAdaptiveRuleBoard()])
      .then(([lifecycle, shape, adaptive]) => { setBoard(lifecycle); setShapeBoard(shape); setAdaptiveBoard(adaptive); })
      .catch((reason: unknown) => setError(reason instanceof Error ? reason.message : "request failed"));
  }, []);
  const rows = useMemo(() => state === "All" ? board?.candidates ?? [] : (board?.candidates ?? []).filter((row) => row.entry_state === state), [board, state]);
  const counts = board?.counts?.entry_state_counts ?? {};
  const total = board?.counts?.total_candidates ?? 0;
  return <div className="tradex-page">
    <section className="tradex-panel">
      <div className="tradex-panel-head"><div><div className="tradex-panel-title">適応型ルール選定</div><div className="tradex-panel-caption">{adaptiveBoard?.current_as_of ?? "--"} 確定 / {adaptiveBoard?.current_regime ?? "--"} / 表示のみ・自動売買なし</div></div><span className={`tradex-pill ${adaptiveBoard?.adoption_gate?.pass ? "is-ok" : "is-warn"}`}>{adaptiveBoard?.selected_policy?.policy ?? "未評価"}</span></div>
      <div className="tradex-summary-strip tradex-summary-strip--compact">
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">現在候補</span><strong className="tradex-summary-card-value">{adaptiveBoard?.current_candidates?.filter((row) => row.router_verdict === "review_entry").length ?? 0}</strong></div>
        <div className="tradex-summary-card is-ok"><span className="tradex-summary-card-label">2026 日次PF</span><strong className="tradex-summary-card-value">{adaptiveBoard?.quality_2026?.metrics?.daily_profit_factor?.toFixed(2) ?? "--"}</strong></div>
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">2026 期待値</span><strong className="tradex-summary-card-value">{pct(adaptiveBoard?.quality_2026?.metrics?.daily_expectancy ?? null)}</strong></div>
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">平均候補/週</span><strong className="tradex-summary-card-value">{adaptiveBoard?.quality_2026?.weekly_coverage?.average_events_per_calendar_week?.toFixed(2) ?? "--"}</strong></div>
      </div>
      <div className="tradex-panel-caption">Activeルール上位3だけを採用。直近20件・60件・同一相場環境の確定成績で日次更新します。</div>
      <div className="tradex-table-wrap"><table className="tradex-table"><thead><tr><th>優先</th><th>ルール</th><th>状態</th><th>環境許可</th><th>適応度</th><th>直近20 PF</th><th>直近20期待値</th><th>同環境PF</th></tr></thead><tbody>
        {(adaptiveBoard?.current_rule_states ?? []).map((row, index) => <tr key={`adaptive-${row.rule}`}><td>{index + 1}</td><td><strong>{row.rule}</strong></td><td><span className={`tradex-pill ${row.state === "Active" ? "is-ok" : row.state === "Dormant" ? "is-muted" : ""}`}>{row.state}</span></td><td>{row.regime_permission_allowed ? "許可" : "停止"}</td><td>{row.score?.toFixed(3) ?? "--"}</td><td>{row.pf20?.toFixed(2) ?? "--"}</td><td>{pct(row.expectancy20 ?? null)}</td><td>{row.same_regime_pf?.toFixed(2) ?? "--"}</td></tr>)}
      </tbody></table></div>
      <div className="tradex-table-wrap"><table className="tradex-table"><thead><tr><th>順位</th><th>方向</th><th>コード</th><th>シナリオ</th><th>状態</th><th>確定終値</th><th>判定</th><th>条件</th></tr></thead><tbody>
        {(adaptiveBoard?.current_candidates ?? []).map((row) => <tr key={`adaptive-candidate-${row.code}-${row.router_rule}`}><td>{row.router_priority_rank ?? "--"}</td><td>{row.side}</td><td><strong>{row.code}</strong></td><td>{row.router_rule}</td><td>{row.router_state}</td><td>{row.confirmed_close?.toFixed(1) ?? "--"}</td><td>{row.router_verdict}</td><td>{row.entry_condition ?? "--"}</td></tr>)}
        {!(adaptiveBoard?.current_candidates ?? []).length ? <tr><td colSpan={8}>現在の候補はありません</td></tr> : null}
      </tbody></table></div>
    </section>
    <section className="tradex-panel">
      <div className="tradex-panel-head"><div><div className="tradex-panel-title">葉20・低ボラ選定</div><div className="tradex-panel-caption">{shapeBoard?.confirmed_signal_date ?? "--"} 確定 / 表示のみ・自動売買なし</div></div><span className="tradex-pill is-ok">forward monitor</span></div>
      <div className="tradex-summary-strip tradex-summary-strip--compact">
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">現在候補</span><strong className="tradex-summary-card-value">{shapeBoard?.candidate_count ?? 0}</strong></div>
        <div className="tradex-summary-card is-ok"><span className="tradex-summary-card-label">2026勝率</span><strong className="tradex-summary-card-value">{pct(shapeBoard?.quality_metrics_2026?.win_rate ?? null)}</strong></div>
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">平均利益</span><strong className="tradex-summary-card-value">{pct(shapeBoard?.quality_metrics_2026?.average_win ?? null)}</strong></div>
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">期待値</span><strong className="tradex-summary-card-value">{pct(shapeBoard?.quality_metrics_2026?.expectancy ?? null)}</strong></div>
        <div className="tradex-summary-card"><span className="tradex-summary-card-label">2026 PF</span><strong className="tradex-summary-card-value">{shapeBoard?.quality_metrics_2026?.profit_factor?.toFixed(2) ?? "--"}</strong></div>
      </div>
      <div className="tradex-panel-caption">葉20 / 実現ボラ3%未満 / 市場幅60%以下 / 翌日始値が確定終値以下 / TP8%・SL5%・最大10日。候補0件を許容します。</div>
      <div className="tradex-table-wrap"><table className="tradex-table"><thead><tr><th>順位</th><th>コード</th><th>形状</th><th>確定終値</th><th>20日ボラ</th><th>市場幅</th><th>MA60乖離</th><th>判定</th><th>条件</th></tr></thead><tbody>
        {(shapeBoard?.candidates ?? []).map((row) => <tr key={`shape-${row.code}`}><td>{row.selection_rank}</td><td><strong>{row.code}</strong></td><td>{row.shape_signal}</td><td>{row.confirmed_close?.toFixed(1) ?? "--"}</td><td>{pct(row.realized_vol20 ?? null)}</td><td>{pct(row.market_breadth ?? null)}</td><td>{pct(row.gap_ma60 ?? null)}</td><td>{row.new_entry_verdict}</td><td>{row.entry_condition}</td></tr>)}
        {!(shapeBoard?.candidates ?? []).length ? <tr><td colSpan={9}>現在は該当候補なし — 待機</td></tr> : null}
      </tbody></table></div>
    </section>
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
