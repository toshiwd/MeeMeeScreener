import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  adoptTradexRun,
  loadTradexFamilies,
  loadTradexFamilyCompare,
  type TradexCompare,
  type TradexFamily,
} from "../experimentApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

export default function TradexAdoptPage() {
  const [families, setFamilies] = useState<TradexFamily[]>([]);
  const [compare, setCompare] = useState<TradexCompare | null>(null);
  const [selectedFamilyId, setSelectedFamilyId] = useState(readTradexLocal<string>(tradexStorageKeys.familyId, ""));
  const [message, setMessage] = useState<string | null>(null);
  const [gate, setGate] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submittingRunId, setSubmittingRunId] = useState<string | null>(null);

  const selectedFamily = useMemo(() => families.find((item) => item.family_id === selectedFamilyId) ?? families[0] ?? null, [families, selectedFamilyId]);

  const refresh = async () => {
    const response = await loadTradexFamilies();
    setFamilies(response.items ?? []);
    const family = response.items?.find((item) => item.family_id === selectedFamilyId) ?? response.items?.[0] ?? null;
    if (family) {
      setSelectedFamilyId(family.family_id);
      writeTradexLocal(tradexStorageKeys.familyId, family.family_id);
      try {
        const compareResponse = await loadTradexFamilyCompare(family.family_id);
        setCompare(compareResponse.compare);
        setGate(null);
      } catch {
        setCompare(null);
        setGate(null);
      }
    }
  };

  useEffect(() => {
    void refresh().catch((err) => setError(err instanceof Error ? err.message : "failed to load adopt page"));
  }, []);

  const selectFamily = async (familyId: string) => {
    setSelectedFamilyId(familyId);
    writeTradexLocal(tradexStorageKeys.familyId, familyId);
    setError(null);
    setMessage(null);
    setGate(null);
    try {
      const compareResponse = await loadTradexFamilyCompare(familyId);
      setCompare(compareResponse.compare);
    } catch (err) {
      setCompare(null);
      setError(err instanceof Error ? err.message : "failed to load compare");
    }
  };

  const adopt = async (runId: string) => {
    if (!selectedFamily) return;
    setSubmittingRunId(runId);
    setError(null);
    setMessage(null);
    try {
      const result = await adoptTradexRun({ family_id: selectedFamily.family_id, run_id: runId, reason: "small-start gate", actor: "tradex-ui" });
      setMessage(`adopt ${result.status}: ${runId}`);
      await refresh();
      setGate(result.gate);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to adopt run");
    } finally {
      setSubmittingRunId(null);
    }
  };

  return (
    <div className="tradex-page tradex-adopt-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Adopt Gate</div>
            <div className="tradex-panel-caption">machine-checkable gate + record only. no live mutation.</div>
          </div>
          <Link className="tradex-secondary-action" to="/compare">
            compare
          </Link>
        </div>
        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}
        {message ? <div className="tradex-shell-alert is-success">{message}</div> : null}

        <div className="tradex-compare-layout">
          <aside className="tradex-compare-sidebar">
            <div className="tradex-section-title">families</div>
            <div className="tradex-candidate-select-list">
              {families.map((family) => (
                <button
                  key={family.family_id}
                  type="button"
                  className={`tradex-candidate-select-item${family.family_id === selectedFamily?.family_id ? " is-active" : ""}`}
                  onClick={() => void selectFamily(family.family_id)}
                >
                  <strong>{family.family_name}</strong>
                  <span>{family.family_id}</span>
                  <span>{family.status_summary?.candidate_runs ? `${family.status_summary.candidate_runs} candidates` : "no candidates"}</span>
                </button>
              ))}
            </div>
          </aside>

          <div className="tradex-compare-main">
            {selectedFamily ? (
              <div className="tradex-panel">
                <div className="tradex-panel-head">
                  <div>
                    <div className="tradex-panel-title">{selectedFamily.family_name}</div>
                    <div className="tradex-panel-caption">{selectedFamily.family_id}</div>
                  </div>
                  <span className="tradex-pill">{selectedFamily.frozen ? "frozen" : "open"}</span>
                </div>
                <div className="tradex-table-wrap">
                  <table className="tradex-table">
                    <thead>
                      <tr>
                        <th>run</th>
                        <th>status</th>
                        <th>shared symbols</th>
                        <th>gate</th>
                        <th>action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(compare?.candidate_results ?? []).map((candidate) => {
                        const runId = String(candidate.run_id);
                        const ready = ["succeeded", "compared"].includes(String(candidate.status));
                        return (
                          <tr key={runId}>
                            <td>
                              <strong>{String(candidate.plan_id)}</strong>
                              <div className="tradex-table-sub">{runId}</div>
                            </td>
                            <td>{String(candidate.status ?? "--")}</td>
                            <td>{String((candidate.symbol_summary as Record<string, unknown>)?.shared_symbols ?? "--")}</td>
                            <td>{JSON.stringify(candidate.primary_metric_deltas ?? {})}</td>
                            <td>
                              <button
                                type="button"
                                className="tradex-secondary-action"
                                onClick={() => void adopt(runId)}
                                disabled={!ready || submittingRunId === runId}
                              >
                                adopt
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                {gate ? (
                  <div className="tradex-inline-grid" style={{ marginTop: 16 }}>
                    <div><span>gate pass</span><strong>{String(gate.pass ?? false)}</strong></div>
                    <div><span>rerun match</span><strong>{String(gate.rerun_match ?? false)}</strong></div>
                    <div><span>detail reason</span><strong>{String(gate.detail_reason ?? "--")}</strong></div>
                    <div><span>reasons</span><strong>{JSON.stringify(gate.reasons ?? [])}</strong></div>
                  </div>
                ) : null}
                <details className="tradex-json-panel">
                  <summary>compare json</summary>
                  <pre>{JSON.stringify(compare, null, 2)}</pre>
                </details>
              </div>
            ) : (
              <div className="tradex-empty-state">
                <strong>no family selected</strong>
                <p>create a family in verify first.</p>
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
