import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadTradexFamilies, loadTradexFamilyCompare, type TradexCompare, type TradexFamily } from "../experimentApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

const fmt = (value: unknown, digits = 3) => (typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "--");
const asArray = (value: unknown) => (Array.isArray(value) ? value : []);
const asRecord = (value: unknown) => (value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {});
const fmtDirection = (value: unknown) => (value === "lower" ? "lower is better" : "higher is better");

function SummaryMetric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "ok" | "warn" }) {
  return (
    <div className={`tradex-summary-metric ${tone === "ok" ? "is-ok" : tone === "warn" ? "is-warn" : ""}`}>
      <span className="tradex-summary-metric-label">{label}</span>
      <strong className="tradex-summary-metric-value">{value}</strong>
    </div>
  );
}

function MetricTable({ title, rows }: { title: string; rows: Array<Record<string, unknown>> }) {
  if (!rows.length) return null;
  return (
    <article className="tradex-panel" style={{ marginTop: 12 }}>
      <div className="tradex-panel-head">
        <div className="tradex-panel-title">{title}</div>
      </div>
      <div className="tradex-table-wrap">
        <table className="tradex-table">
          <thead>
            <tr>
              <th>metric</th>
              <th>direction</th>
              <th>baseline</th>
              <th>candidate</th>
              <th>delta</th>
              <th>pass</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={String(row.metric ?? "")}>
                <td>{String(row.metric ?? "--")}</td>
                <td>{fmtDirection(row.direction)}</td>
                <td>{fmt(row.baseline, 4)}</td>
                <td>{fmt(row.candidate, 4)}</td>
                <td>{fmt(row.delta, 4)}</td>
                <td>{String(row.pass ?? false)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}

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
                <div className="tradex-summary-strip" aria-label="family summary">
                  <SummaryMetric label="baseline run" value={compare?.baseline_run_id ?? selectedFamily.baseline_run_id ?? "--"} />
                  <SummaryMetric label="candidate count" value={String(compareRows.length)} />
                  <SummaryMetric label="frozen" value={String(selectedFamily.frozen)} tone={selectedFamily.frozen ? "ok" : "neutral"} />
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
                    <div className="tradex-inline-grid">
                      <div><span>baseline overall_score</span><strong>{fmt(asRecord(candidate.baseline_absolute).overall_score, 4)}</strong></div>
                      <div><span>candidate overall_score</span><strong>{fmt(asRecord(candidate.candidate_absolute).overall_score, 4)}</strong></div>
                      <div><span>baseline by_period_stability</span><strong>{fmt(asRecord(candidate.baseline_absolute).by_period_stability, 4)}</strong></div>
                      <div><span>candidate by_period_stability</span><strong>{fmt(asRecord(candidate.candidate_absolute).by_period_stability, 4)}</strong></div>
                      <div><span>baseline symbol_concentration</span><strong>{fmt(asRecord(candidate.baseline_absolute).symbol_concentration, 4)}</strong></div>
                      <div><span>candidate symbol_concentration</span><strong>{fmt(asRecord(candidate.candidate_absolute).symbol_concentration, 4)}</strong></div>
                    </div>
                    <MetricTable title="absolute metrics" rows={asArray(candidate.absolute_metric_comparisons)} />
                    <div className="tradex-summary-strip tradex-summary-strip--compact" aria-label="candidate metric deltas">
                      {Object.entries((candidate.primary_metric_deltas as Record<string, unknown>) ?? {}).map(([key, value]) => (
                        <SummaryMetric key={key} label={key} value={fmt(value, 4)} />
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
                        <strong>{JSON.stringify(asArray(candidate.top_conditions))}</strong>
                      </div>
                      <div>
                        <span>winning examples</span>
                        <strong>{JSON.stringify(asArray(candidate.winning_examples))}</strong>
                      </div>
                      <div>
                        <span>losing examples</span>
                        <strong>{JSON.stringify(asArray(candidate.losing_examples))}</strong>
                      </div>
                    </div>
                    <div className="tradex-inline-grid">
                      <div>
                        <span>review focus</span>
                        <strong>{JSON.stringify(asArray(candidate.review_focus).map((item) => `${String((item as Record<string, unknown>).code ?? "")}:${String((item as Record<string, unknown>).source ?? "")}`))}</strong>
                      </div>
                      <div>
                        <span>metric directions</span>
                        <strong>{JSON.stringify(asRecord(candidate.metric_directions))}</strong>
                      </div>
                    </div>
                    <div className="tradex-action-row">
                      {asArray(candidate.review_focus).map((item) => {
                        const focus = item as Record<string, unknown>;
                        const code = String(focus.code ?? "");
                        return code ? (
                          <Link key={code} className="tradex-secondary-action" to={`/detail/${encodeURIComponent(String(candidate.run_id))}?code=${encodeURIComponent(code)}`}>
                            {String(focus.label ?? code)}
                          </Link>
                        ) : null;
                      })}
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
