import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadTradexReplayRun, type TradexReplayRun } from "../replayApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

const fmt = (value: unknown, digits = 3) => (typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "--");
const asArray = (value: unknown) => (Array.isArray(value) ? value : []);
const asRecord = (value: unknown) => (value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {});

function SummaryMetric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "ok" | "warn" }) {
  return (
    <div className={`tradex-summary-metric ${tone === "ok" ? "is-ok" : tone === "warn" ? "is-warn" : ""}`}>
      <span className="tradex-summary-metric-label">{label}</span>
      <strong className="tradex-summary-metric-value">{value}</strong>
    </div>
  );
}

function EquityChart({ run }: { run: TradexReplayRun | null }) {
  const equity = asArray(run?.daily_equity_curve?.items);
  const ledger = asArray(run?.trade_ledger?.items);
  if (!equity.length) return <div className="tradex-empty-state"><strong>no equity curve</strong></div>;
  const values = equity.map((item) => Number((item as Record<string, unknown>).equity ?? 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = 1000;
  const height = 220;
  const scaleX = equity.length > 1 ? width / (equity.length - 1) : width;
  const scaleY = max === min ? 0 : height / (max - min);
  const points = values.map((value, index) => `${index * scaleX},${height - (value - min) * scaleY}`).join(" ");
  const markerByDate = new Map<string, Array<Record<string, unknown>>>();
  for (const row of ledger) {
    const item = row as Record<string, unknown>;
    const date = String(item.date ?? "");
    if (!date) continue;
    const events = markerByDate.get(date) ?? [];
    events.push(item);
    markerByDate.set(date, events);
  }
  return (
    <svg viewBox={`0 0 ${width} ${height + 30}`} className="tradex-equity-chart" role="img" aria-label="equity curve">
      <polyline fill="none" stroke="currentColor" strokeWidth="2.5" points={points} />
      {equity.map((item, index) => {
        const row = item as Record<string, unknown>;
        const date = String(row.date ?? "");
        const marker = markerByDate.get(date);
        if (!marker?.length) return null;
        const x = index * scaleX;
        return marker.slice(0, 3).map((event, eventIndex) => {
          const action = String(event.action_taken ?? "");
          const color = action.includes("enter") || action.includes("add") ? "#3aa675" : action.includes("exit") || action.includes("take") ? "#d48a00" : "#6688ff";
          return <circle key={`${date}-${eventIndex}`} cx={x} cy={height - (Number(row.equity ?? 0) - min) * scaleY} r="4" fill={color} />;
        });
      })}
    </svg>
  );
}

export default function TradexReplayPage() {
  const [runId, setRunId] = useState(readTradexLocal<string>(tradexStorageKeys.replayRunId, ""));
  const [run, setRun] = useState<TradexReplayRun | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedIndex, setSelectedIndex] = useState(0);

  const equityItems = asArray(run?.daily_equity_curve?.items);
  const selectedDay = equityItems[Math.min(selectedIndex, Math.max(0, equityItems.length - 1))] as Record<string, unknown> | undefined;
  const selectedDate = String(selectedDay?.date ?? "");
  const selectedPositions = useMemo(
    () => asArray(run?.positions_timeline?.items).filter((item) => String((item as Record<string, unknown>).date ?? "") === selectedDate),
    [run, selectedDate]
  );
  const ledgerItems = asArray(run?.trade_ledger?.items);

  useEffect(() => {
    if (!runId) return;
    let active = true;
    setLoading(true);
    setError(null);
    void loadTradexReplayRun(runId)
      .then((response) => {
        if (!active) return;
        setRun(response);
        writeTradexLocal(tradexStorageKeys.replayRunId, runId);
        setSelectedIndex(0);
      })
      .catch((err) => {
        if (active) {
          setError(err instanceof Error ? err.message : "failed to load replay");
          setRun(null);
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [runId]);

  const summary = asRecord(run?.window_summary);
  const rel = asRecord(run?.relative_performance);
  const changeLog = asArray(run?.selection_rule_change_log?.items);

  return (
    <div className="tradex-page tradex-replay-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Portfolio Replay</div>
            <div className="tradex-panel-caption">strict 3-month replay viewer, read-only.</div>
          </div>
          <div className="tradex-panel-actions">
            <Link className="tradex-secondary-action" to="/verify">verify</Link>
            <Link className="tradex-secondary-action" to="/compare">compare</Link>
          </div>
        </div>

        <div className="tradex-action-row">
          <label className="tradex-field" style={{ minWidth: 320, flex: 1 }}>
            <span>run id</span>
            <input value={runId} onChange={(event) => setRunId(event.target.value)} placeholder="replay_..." />
          </label>
          <button type="button" className="tradex-primary-action" onClick={() => setRunId(runId.trim())} disabled={loading || !runId.trim()}>
            load replay
          </button>
        </div>

        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}
        {!run ? <div className="tradex-empty-state"><strong>{loading ? "loading" : "enter a run id"}</strong></div> : null}

        {run ? (
          <>
            <div className="tradex-summary-strip" aria-label="replay summary">
              <SummaryMetric label="portfolio return" value={fmt(summary.portfolio_return_3m, 4)} tone={Number(summary.portfolio_return_3m) >= 0 ? "ok" : "warn"} />
              <SummaryMetric label="excess vs universe" value={fmt(summary.excess_vs_universe, 4)} tone={Number(summary.excess_vs_universe) >= 0 ? "ok" : "warn"} />
              <SummaryMetric label="final score" value={fmt(summary.final_score, 4)} />
              <SummaryMetric label="weekly pass rate" value={fmt(summary.weekly_activity_pass_rate, 3)} tone={Number(summary.weekly_activity_pass_rate) >= 0.8 ? "ok" : "warn"} />
              <SummaryMetric label="max drawdown" value={fmt(summary.max_drawdown, 4)} tone={Number(summary.max_drawdown) >= -0.08 ? "ok" : "warn"} />
            </div>

            <div className="tradex-inline-grid">
              <div><span>policy</span><strong>{String(summary.policy_id ?? "--")} / {String(summary.policy_version ?? "--")}</strong></div>
              <div><span>window</span><strong>{String(summary.window_start_date ?? "--")} - {String(summary.window_end_date ?? "--")}</strong></div>
              <div><span>market benchmark</span><strong>{String((run.benchmark_market as Record<string, unknown>).symbol ?? "--")}</strong></div>
              <div><span>selection rule</span><strong>{String(asRecord(summary.selection_rule_signatures).selection_rule_signature ?? "--")}</strong></div>
            </div>

            <EquityChart run={run} />

            <label className="tradex-field">
              <span>timeline scrubber</span>
              <input
                type="range"
                min={0}
                max={Math.max(0, equityItems.length - 1)}
                value={selectedIndex}
                onChange={(event) => setSelectedIndex(Number(event.target.value))}
              />
            </label>

            <div className="tradex-inline-grid">
              <div><span>selected date</span><strong>{selectedDate || "--"}</strong></div>
              <div><span>cash</span><strong>{fmt(selectedDay?.cash, 2)}</strong></div>
              <div><span>equity</span><strong>{fmt(selectedDay?.equity, 2)}</strong></div>
              <div><span>net exposure</span><strong>{fmt(selectedDay?.net_exposure, 2)}</strong></div>
            </div>

            <div className="tradex-inline-grid">
              <div><span>selected notation</span><strong>{selectedPositions.map((row) => String((row as Record<string, unknown>).position_notation ?? "0-0")).join(", ") || "0-0"}</strong></div>
              <div><span>holding days</span><strong>{selectedPositions.map((row) => String((row as Record<string, unknown>).holding_days_open ?? 0)).join(", ") || "0"}</strong></div>
              <div><span>realized pnl</span><strong>{selectedPositions.map((row) => fmt((row as Record<string, unknown>).realized_pnl_cum, 2)).join(", ") || "0"}</strong></div>
              <div><span>unrealized pnl</span><strong>{selectedPositions.map((row) => fmt((row as Record<string, unknown>).unrealized_pnl, 2)).join(", ") || "0"}</strong></div>
            </div>

            <div className="tradex-table-wrap">
              <table className="tradex-table">
                <thead>
                  <tr>
                    <th>date</th>
                    <th>symbol</th>
                    <th>notation</th>
                    <th>holding</th>
                    <th>action</th>
                    <th>reason</th>
                    <th>cash</th>
                    <th>equity</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedPositions.map((row) => {
                    const item = row as Record<string, unknown>;
                    return (
                      <tr key={`${String(item.date ?? "")}-${String(item.symbol ?? "")}`}>
                        <td>{String(item.date ?? "--")}</td>
                        <td>{String(item.symbol ?? "--")}</td>
                        <td>{String(item.position_notation ?? "--")}</td>
                        <td>{String(item.holding_days_open ?? 0)}</td>
                        <td>{String(item.action_taken ?? "--")}</td>
                        <td>{String(item.trigger_reason_code ?? "--")}</td>
                        <td>{fmt(item.cash_after, 2)}</td>
                        <td>{fmt(item.equity_after, 2)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <details className="tradex-json-panel">
              <summary>trade ledger</summary>
              <div className="tradex-table-wrap">
                <table className="tradex-table">
                  <thead>
                    <tr>
                      <th>date</th>
                      <th>symbol</th>
                      <th>action</th>
                      <th>state</th>
                      <th>units</th>
                      <th>reason</th>
                      <th>pnl</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ledgerItems.map((row) => {
                      const item = row as Record<string, unknown>;
                      return (
                        <tr key={`${String(item.date ?? "")}-${String(item.symbol ?? "")}-${String(item.action_taken ?? "")}`}>
                          <td>{String(item.date ?? "--")}</td>
                          <td>{String(item.symbol ?? "--")}</td>
                          <td>{String(item.action_taken ?? "--")}</td>
                          <td>{String(item.state_to ?? "--")}</td>
                          <td>{String(item.units_changed ?? 0)}</td>
                          <td>{String(item.trigger_reason_text ?? "--")}</td>
                          <td>{fmt(item.realized_pnl_delta, 2)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </details>

            <details className="tradex-json-panel">
              <summary>selection change log</summary>
              <pre>{JSON.stringify(changeLog, null, 2)}</pre>
            </details>

            <details className="tradex-json-panel">
              <summary>replay json</summary>
              <pre>{JSON.stringify(run, null, 2)}</pre>
            </details>
            <details className="tradex-json-panel">
              <summary>relative performance</summary>
              <pre>{JSON.stringify(rel, null, 2)}</pre>
            </details>
          </>
        ) : null}
      </section>
    </div>
  );
}

