import { useEffect, useMemo, useState } from "react";
import { loadShortLifecycleBoard, type ShortLifecycleBoard, type ShortLifecycleCandidate } from "../shortLifecycleApi";

const STATE_ORDER = ["Probe", "Watch", "AddWatch", "HoldReview", "TakeProfit", "Exit", "Avoid", "RegimeMissing"];

const STATE_LABELS: Record<string, string> = {
  Probe: "打診",
  Watch: "監視",
  AddWatch: "追加監視",
  HoldReview: "保有確認",
  TakeProfit: "利確",
  Exit: "撤退",
  Avoid: "回避",
  RegimeMissing: "地合い欠損",
};

function formatPct(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(1)}%`;
}

function formatNumber(value: number | null | undefined, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(digits);
}

function stateLabel(state: string) {
  return STATE_LABELS[state] ?? state;
}

function stateTone(state: string) {
  if (state === "Probe" || state === "AddWatch" || state === "TakeProfit") return "is-ok";
  if (state === "Avoid" || state === "Exit" || state === "RegimeMissing") return "is-warn";
  return "is-muted";
}

function stateCount(board: ShortLifecycleBoard | null, state: string) {
  const counts = board?.counts?.lifecycle_state_counts;
  return typeof counts?.[state] === "number" ? counts[state] : 0;
}

function CandidateRow({ row }: { row: ShortLifecycleCandidate }) {
  return (
    <tr>
      <td>{row.lifecycle_rank ?? "--"}</td>
      <td>
        <span className={`tradex-pill ${stateTone(row.lifecycle_state)}`}>{stateLabel(row.lifecycle_state)}</span>
      </td>
      <td>
        <strong>{row.code}</strong>
        {row.name ? <div className="tradex-table-sub">{row.name}</div> : null}
      </td>
      <td>{row.signal_ymd ?? "--"}</td>
      <td>
        <div>{row.setup_state ?? "--"}</div>
        <div className="tradex-table-sub">{row.continuation_status ?? "--"}</div>
      </td>
      <td>
        <div>{formatPct(row.expected_downside_pct)}</div>
        <div className="tradex-table-sub">RR {formatNumber(row.risk_reward_to_sl8)}</div>
      </td>
      <td>
        <div>{row.final_review_status ?? "--"}</div>
        <div className="tradex-table-sub">{row.regime_permission_status ?? "--"}</div>
      </td>
      <td>
        <div>{row.base_target_actionability ?? "--"}</div>
        <div className="tradex-table-sub">{row.visual_micro_label ?? "--"}</div>
      </td>
      <td>{row.lifecycle_reasons.slice(0, 3).join(" / ") || "--"}</td>
    </tr>
  );
}

export default function TradexShortLifecyclePage() {
  const [board, setBoard] = useState<ShortLifecycleBoard | null>(null);
  const [selectedState, setSelectedState] = useState<string>("All");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    loadShortLifecycleBoard(30)
      .then((payload) => {
        if (!cancelled) setBoard(payload);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "request failed");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const states = useMemo(() => {
    const counts = board?.counts?.lifecycle_state_counts ?? {};
    const extra = Object.keys(counts).filter((state) => !STATE_ORDER.includes(state));
    return ["All", ...STATE_ORDER, ...extra];
  }, [board]);

  const rows = useMemo(() => {
    const candidates = board?.candidates ?? [];
    if (selectedState === "All") return candidates;
    return candidates.filter((row) => row.lifecycle_state === selectedState);
  }, [board, selectedState]);

  const total = board?.counts?.total_candidates ?? board?.candidates.length ?? 0;

  return (
    <div className="tradex-page tradex-short-lifecycle-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Short lifecycle board</div>
            <div className="tradex-panel-caption">{board?.artifact_path ?? board?.reason ?? "current_short_lifecycle_rank_board_v1"}</div>
          </div>
          <div className="tradex-panel-actions">
            <span className={`tradex-pill ${board?.available ? "is-ok" : "is-warn"}`}>
              {loading ? "loading" : board?.available ? "review-only" : "unavailable"}
            </span>
            <span className="tradex-pill is-muted">{board?.created_at ?? "--"}</span>
          </div>
        </div>

        {error ? <div className="tradex-inline-error">{error}</div> : null}
        {!loading && board && !board.available ? (
          <div className="tradex-inline-error">{board.reason ?? "short lifecycle board is unavailable"}</div>
        ) : null}

        <div className="tradex-summary-strip tradex-summary-strip--compact">
          <div className="tradex-summary-card">
            <span className="tradex-summary-card-label">total</span>
            <strong className="tradex-summary-card-value">{total.toLocaleString("ja-JP")}</strong>
          </div>
          <div className="tradex-summary-card is-ok">
            <span className="tradex-summary-card-label">打診</span>
            <strong className="tradex-summary-card-value">{stateCount(board, "Probe")}</strong>
          </div>
          <div className="tradex-summary-card">
            <span className="tradex-summary-card-label">監視</span>
            <strong className="tradex-summary-card-value">{stateCount(board, "Watch")}</strong>
          </div>
          <div className="tradex-summary-card is-warn">
            <span className="tradex-summary-card-label">回避</span>
            <strong className="tradex-summary-card-value">{stateCount(board, "Avoid")}</strong>
          </div>
        </div>
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Candidates</div>
            <div className="tradex-panel-caption">{board?.authoritative_decision ?? "--"}</div>
          </div>
          <div className="tradex-panel-actions tradex-state-filter">
            {states.map((state) => (
              <button
                key={state}
                type="button"
                className={`tradex-nav-item ${selectedState === state ? "active" : ""}`}
                onClick={() => setSelectedState(state)}
              >
                {state === "All" ? "All" : stateLabel(state)}
                <span className="tradex-table-sub">{state === "All" ? total : stateCount(board, state)}</span>
              </button>
            ))}
          </div>
        </div>

        <div className="tradex-table-wrap">
          <table className="tradex-table tradex-short-lifecycle-table">
            <thead>
              <tr>
                <th>rank</th>
                <th>state</th>
                <th>code</th>
                <th>signal</th>
                <th>setup</th>
                <th>downside</th>
                <th>gate</th>
                <th>micro</th>
                <th>reason</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <CandidateRow key={`${row.code}-${row.signal_ymd ?? index}-${row.lifecycle_rank ?? index}`} row={row} />
              ))}
              {!rows.length ? (
                <tr>
                  <td colSpan={9}>No rows</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
