import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadTradexFamilies, loadTradexFamilyCompare, type TradexCompare, type TradexFamily } from "../experimentApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

const fmt = (value: unknown, digits = 3) => (typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "--");

export default function TradexComparePage() {
  const [families, setFamilies] = useState<TradexFamily[]>([]);
  const [compare, setCompare] = useState<TradexCompare | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedFamilyId, setSelectedFamilyId] = useState(readTradexLocal<string>(tradexStorageKeys.familyId, ""));

  const selectedFamily = useMemo(() => families.find((item) => item.family_id === selectedFamilyId) ?? families[0] ?? null, [families, selectedFamilyId]);

  useEffect(() => {
    let active = true;
    void (async () => {
      setLoading(true);
      setError(null);
      try {
        const familiesResponse = await loadTradexFamilies();
        if (!active) return;
        setFamilies(familiesResponse.items ?? []);
        const family = familiesResponse.items?.find((item) => item.family_id === selectedFamilyId) ?? familiesResponse.items?.[0] ?? null;
        if (family) {
          setSelectedFamilyId(family.family_id);
          writeTradexLocal(tradexStorageKeys.familyId, family.family_id);
          try {
            const compareResponse = await loadTradexFamilyCompare(family.family_id);
            if (active) setCompare(compareResponse.compare);
          } catch (err) {
            if (active) setError(err instanceof Error ? err.message : "failed to load compare");
          }
        }
      } catch (err) {
        if (active) setError(err instanceof Error ? err.message : "failed to load families");
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => {
      active = false;
    };
  }, [selectedFamilyId]);

  const selectFamily = async (familyId: string) => {
    setSelectedFamilyId(familyId);
    writeTradexLocal(tradexStorageKeys.familyId, familyId);
    setLoading(true);
    setError(null);
    try {
      const response = await loadTradexFamilyCompare(familyId);
      setCompare(response.compare);
    } catch (err) {
      setCompare(null);
      setError(err instanceof Error ? err.message : "failed to load compare");
    } finally {
      setLoading(false);
    }
  };

  const compareRows = compare?.candidate_results ?? [];

  return (
    <div className="tradex-page tradex-compare-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Family Compare</div>
            <div className="tradex-panel-caption">baseline vs candidates under the same family lock.</div>
          </div>
          <Link className="tradex-secondary-action" to="/verify">
            verify
          </Link>
        </div>

        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}

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
                  <span>{family.status_summary?.total_runs ? `${family.status_summary.total_runs} runs` : "no runs"}</span>
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
                  <span className="tradex-pill">{loading ? "loading" : compareRows.length ? "compare ready" : "waiting"}</span>
                </div>
                <div className="tradex-compare-summaries">
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">baseline run</div>
                    <div className="tradex-summary-card-value">{compare?.baseline_run_id ?? selectedFamily.baseline_run_id ?? "--"}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">candidate count</div>
                    <div className="tradex-summary-card-value">{compareRows.length}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">frozen</div>
                    <div className="tradex-summary-card-value">{String(selectedFamily.frozen)}</div>
                  </article>
                </div>

                {compareRows.map((candidate) => (
                  <article key={String(candidate.run_id)} className="tradex-panel" style={{ marginTop: 16 }}>
                    <div className="tradex-panel-head">
                      <div>
                        <div className="tradex-panel-title">{String(candidate.plan_id)}</div>
                        <div className="tradex-panel-caption">{String(candidate.run_id)}</div>
                      </div>
                      <span className="tradex-pill">{String(candidate.status ?? "--")}</span>
                    </div>
                    <div className="tradex-compare-summaries">
                      {Object.entries((candidate.primary_metric_deltas as Record<string, unknown>) ?? {}).map(([key, value]) => (
                        <article key={key} className="tradex-summary-card">
                          <div className="tradex-summary-card-label">{key}</div>
                          <div className="tradex-summary-card-value">{fmt(value, 4)}</div>
                        </article>
                      ))}
                    </div>
                    <div className="tradex-inline-grid">
                      <div><span>target_symbol_count_delta</span><strong>{fmt(candidate.target_symbol_count_delta, 0)}</strong></div>
                      <div><span>baseline_symbols</span><strong>{String((candidate.symbol_summary as Record<string, unknown>)?.baseline_symbols ?? "--")}</strong></div>
                      <div><span>candidate_symbols</span><strong>{String((candidate.symbol_summary as Record<string, unknown>)?.candidate_symbols ?? "--")}</strong></div>
                      <div><span>shared_symbols</span><strong>{String((candidate.symbol_summary as Record<string, unknown>)?.shared_symbols ?? "--")}</strong></div>
                    </div>
                    <div className="tradex-inline-grid">
                      <div>
                        <span>baseline signal dates</span>
                        <strong>{JSON.stringify((candidate.signal_date_deltas as Record<string, unknown>)?.baseline ?? [])}</strong>
                      </div>
                      <div>
                        <span>candidate signal dates</span>
                        <strong>{JSON.stringify((candidate.signal_date_deltas as Record<string, unknown>)?.candidate ?? [])}</strong>
                      </div>
                    </div>
                    <div className="tradex-inline-grid">
                      <div>
                        <span>top conditions</span>
                        <strong>{JSON.stringify(candidate.top_conditions ?? [])}</strong>
                      </div>
                      <div>
                        <span>winning examples</span>
                        <strong>{JSON.stringify(candidate.winning_examples ?? [])}</strong>
                      </div>
                      <div>
                        <span>losing examples</span>
                        <strong>{JSON.stringify(candidate.losing_examples ?? [])}</strong>
                      </div>
                    </div>
                    <details className="tradex-json-panel">
                      <summary>candidate compare json</summary>
                      <pre>{JSON.stringify(candidate, null, 2)}</pre>
                    </details>
                  </article>
                ))}
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

