import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  createTradexFamily,
  createTradexRun,
  loadTradexFamilies,
  loadTradexRun,
  type TradexFamily,
  type TradexPlanSpec,
  type TradexPeriodSegment,
} from "../experimentApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

const defaultUniverse = ["7203", "6758", "9984", "8306", "9983", "4063", "9432", "7974", "6902", "6861", "6098", "8035", "6501", "6954", "4519", "8801", "9020", "6503", "8308", "8058"];

const defaultSegments: TradexPeriodSegment[] = [
  { label: "phase-1", start_date: "2025-01-01", end_date: "2025-01-31" },
  { label: "phase-2", start_date: "2025-02-01", end_date: "2025-02-28" }
];

const defaultBaselinePlan: TradexPlanSpec = {
  plan_id: "baseline",
  plan_version: "v1",
  label: "Baseline",
  minimum_confidence: 0.4,
  minimum_ready_rate: 0.4,
  signal_bias: "balanced",
  top_k: 3
};

const defaultCandidatePlans: TradexPlanSpec[] = [
  { plan_id: "candidate-a", plan_version: "v1", label: "Candidate A / stronger", minimum_confidence: 0.58, minimum_ready_rate: 0.55, signal_bias: "balanced", top_k: 4 },
  { plan_id: "candidate-b", plan_version: "v1", label: "Candidate B / simpler", minimum_confidence: 0.36, minimum_ready_rate: 0.25, signal_bias: "balanced", top_k: 2 },
  { plan_id: "candidate-c", plan_version: "v1", label: "Candidate C / alternative", minimum_confidence: 0.48, minimum_ready_rate: 0.4, signal_bias: "sell", top_k: 3 }
];

const pretty = (value: unknown) => JSON.stringify(value, null, 2);

function FamilyCard({ family, active, onSelect }: { family: TradexFamily; active: boolean; onSelect: (familyId: string) => void }) {
  return (
    <button type="button" className={`tradex-candidate-select-item${active ? " is-active" : ""}`} onClick={() => onSelect(family.family_id)}>
      <strong>{family.family_name}</strong>
      <span>{family.family_id}</span>
      <span>{family.status_summary?.total_runs ? `${family.status_summary.total_runs} runs` : "no runs"}</span>
    </button>
  );
}

export default function TradexVerifyPage() {
  const [families, setFamilies] = useState<TradexFamily[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedFamilyId, setSelectedFamilyId] = useState<string>(readTradexLocal<string>(tradexStorageKeys.familyId, ""));
  const [submitting, setSubmitting] = useState(false);
  const [familyName, setFamilyName] = useState("small-start family");
  const [universeText, setUniverseText] = useState(defaultUniverse.join(", "));
  const [segmentsText, setSegmentsText] = useState(pretty(defaultSegments));
  const [baselinePlanText, setBaselinePlanText] = useState(pretty(defaultBaselinePlan));
  const [candidatePlansText, setCandidatePlansText] = useState(pretty(defaultCandidatePlans));
  const selectedFamily = useMemo(() => families.find((item) => item.family_id === selectedFamilyId) ?? families[0] ?? null, [families, selectedFamilyId]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await loadTradexFamilies();
      setFamilies(response.items ?? []);
      const stored = readTradexLocal<string>(tradexStorageKeys.familyId, "");
      if (stored && response.items?.some((item) => item.family_id === stored)) {
        setSelectedFamilyId(stored);
      } else if (!selectedFamilyId && response.items?.[0]) {
        setSelectedFamilyId(response.items[0].family_id);
        writeTradexLocal(tradexStorageKeys.familyId, response.items[0].family_id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to load families");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const selectFamily = (familyId: string) => {
    setSelectedFamilyId(familyId);
    writeTradexLocal(tradexStorageKeys.familyId, familyId);
  };

  const handleCreateFamily = async () => {
    setSubmitting(true);
    setError(null);
    try {
      const universe = universeText
        .split(/[\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean);
      const period = JSON.parse(segmentsText) as { label?: string; start_date: string; end_date: string }[];
      const baselinePlan = JSON.parse(baselinePlanText) as TradexPlanSpec;
      const candidatePlans = JSON.parse(candidatePlansText) as TradexPlanSpec[];
      const response = await createTradexFamily({
        family_name: familyName,
        universe,
        period: { segments: period },
        baseline_plan: baselinePlan,
        candidate_plans: candidatePlans,
      });
      setSelectedFamilyId(response.family.family_id);
      writeTradexLocal(tradexStorageKeys.familyId, response.family.family_id);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to create family");
    } finally {
      setSubmitting(false);
    }
  };

  const runBaseline = async () => {
    if (!selectedFamily) return;
    setSubmitting(true);
    setError(null);
    try {
      await createTradexRun(selectedFamily.family_id, { run_kind: "baseline", notes: "baseline run" });
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to run baseline");
    } finally {
      setSubmitting(false);
    }
  };

  const runCandidate = async (planId: string) => {
    if (!selectedFamily) return;
    setSubmitting(true);
    setError(null);
    try {
      await createTradexRun(selectedFamily.family_id, { run_kind: "candidate", plan_id: planId, notes: `candidate run: ${planId}` });
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to run candidate");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="tradex-page tradex-verify-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Family Verify</div>
            <div className="tradex-panel-caption">Create a frozen family, run baseline/candidates, and keep the research loop read-only except adopt.</div>
          </div>
          <button type="button" className="tradex-secondary-action" onClick={() => void refresh()} disabled={loading || submitting}>
            refresh
          </button>
        </div>
        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}

        <div className="tradex-compare-layout">
          <aside className="tradex-compare-sidebar">
            <div className="tradex-section-title">families</div>
            <div className="tradex-candidate-select-list">
              {families.map((family) => (
                <FamilyCard key={family.family_id} family={family} active={family.family_id === selectedFamily?.family_id} onSelect={selectFamily} />
              ))}
            </div>
          </aside>

          <div className="tradex-compare-main">
            <div className="tradex-panel">
              <div className="tradex-panel-head">
                <div>
                  <div className="tradex-panel-title">Create Family</div>
                  <div className="tradex-panel-caption">universe 20-50 symbols, period needs at least 2 segments.</div>
                </div>
              </div>
              <div className="tradex-form-stack">
                <label className="tradex-field">
                  <span>family name</span>
                  <input value={familyName} onChange={(event) => setFamilyName(event.target.value)} />
                </label>
                <label className="tradex-field">
                  <span>universe</span>
                  <textarea rows={3} value={universeText} onChange={(event) => setUniverseText(event.target.value)} />
                </label>
                <label className="tradex-field">
                  <span>period segments JSON</span>
                  <textarea rows={6} value={segmentsText} onChange={(event) => setSegmentsText(event.target.value)} />
                </label>
                <label className="tradex-field">
                  <span>baseline plan JSON</span>
                  <textarea rows={6} value={baselinePlanText} onChange={(event) => setBaselinePlanText(event.target.value)} />
                </label>
                <label className="tradex-field">
                  <span>candidate plans JSON</span>
                  <textarea rows={10} value={candidatePlansText} onChange={(event) => setCandidatePlansText(event.target.value)} />
                </label>
                <div className="tradex-action-row">
                  <button type="button" className="tradex-primary-action" onClick={() => void handleCreateFamily()} disabled={submitting}>
                    create family
                  </button>
                </div>
              </div>
            </div>

            {selectedFamily ? (
              <div className="tradex-panel">
                <div className="tradex-panel-head">
                  <div>
                    <div className="tradex-panel-title">{selectedFamily.family_name}</div>
                    <div className="tradex-panel-caption">{selectedFamily.family_id}</div>
                  </div>
                  <span className="tradex-pill">{selectedFamily.frozen ? "frozen" : "open"}</span>
                </div>
                <div className="tradex-inline-grid">
                  <div><span>baseline</span><strong>{selectedFamily.baseline_plan?.plan_id ?? "--"}</strong></div>
                  <div><span>input_dataset_version</span><strong>{selectedFamily.input_dataset_version}</strong></div>
                  <div><span>code_revision</span><strong>{selectedFamily.code_revision}</strong></div>
                  <div><span>confirmed_only</span><strong>{String(selectedFamily.confirmed_only)}</strong></div>
                </div>
                <div className="tradex-action-row">
                  <button type="button" className="tradex-primary-action" onClick={() => void runBaseline()} disabled={submitting || Boolean(selectedFamily.baseline_run_id)}>
                    run baseline
                  </button>
                  {selectedFamily.candidate_plans.map((plan) => (
                    <button key={plan.plan_id} type="button" className="tradex-secondary-action" onClick={() => void runCandidate(plan.plan_id)} disabled={submitting || !selectedFamily.baseline_run_id}>
                      run {plan.plan_id}
                    </button>
                  ))}
                </div>
                <details className="tradex-json-panel">
                  <summary>family json</summary>
                  <pre>{JSON.stringify(selectedFamily, null, 2)}</pre>
                </details>
              </div>
            ) : null}

            {selectedFamily ? (
              <div className="tradex-panel">
                <div className="tradex-panel-head">
                  <div>
                    <div className="tradex-panel-title">Runs</div>
                    <div className="tradex-panel-caption">baseline first, then up to 3 candidates.</div>
                  </div>
                  <Link className="tradex-secondary-action" to="/compare">
                    family compare
                  </Link>
                </div>
                <div className="tradex-table-wrap">
                  <table className="tradex-table">
                    <thead>
                      <tr>
                        <th>run</th>
                        <th>kind</th>
                        <th>plan</th>
                        <th>status</th>
                        <th>signals</th>
                        <th>detail</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(selectedFamily.run_ids ?? []).map((runId) => (
                        <RunRow key={runId} familyId={selectedFamily.family_id} runId={runId} />
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}

function RunRow({ familyId, runId }: { familyId: string; runId: string }) {
  const [run, setRun] = useState<Record<string, unknown> | null>(null);
  useEffect(() => {
    let active = true;
    void (async () => {
      try {
        const response = await loadTradexRun(runId);
        if (active) {
          setRun(response.run);
        }
      } catch {
        if (active) setRun(null);
      }
    })();
    return () => {
      active = false;
    };
  }, [runId]);

  const summary = (run?.summary as Record<string, unknown> | undefined) ?? {};
  const signals = summary.signal_count as number | undefined;
  const byCode = (run?.analysis as Record<string, unknown> | undefined)?.by_code as Record<string, unknown> | undefined;
  const sortedCodes = byCode
    ? Object.values(byCode)
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        .sort((left, right) => {
          const leftCount = typeof left.signal_count === "number" ? left.signal_count : 0;
          const rightCount = typeof right.signal_count === "number" ? right.signal_count : 0;
          return rightCount - leftCount || String(left.code ?? "").localeCompare(String(right.code ?? ""));
        })
    : [];
  const firstCode = sortedCodes.length > 0 ? String(sortedCodes[0].code ?? "") : "";
  return (
    <tr>
      <td>
        <strong>{runId}</strong>
        <div className="tradex-table-sub">{familyId}</div>
      </td>
      <td>{String(run?.run_kind ?? "--")}</td>
      <td>{String(run?.plan_id ?? "--")}</td>
      <td>{String(run?.status ?? "loading")}</td>
      <td>{typeof signals === "number" ? signals.toLocaleString("ja-JP") : "--"}</td>
      <td>
        {firstCode ? (
          <Link to={`/detail/${encodeURIComponent(runId)}?code=${encodeURIComponent(firstCode)}`} onClick={() => writeTradexLocal(tradexStorageKeys.runId, runId)}>
            detail
          </Link>
        ) : (
          <span className="tradex-inline-note">loading</span>
        )}
      </td>
    </tr>
  );
}
