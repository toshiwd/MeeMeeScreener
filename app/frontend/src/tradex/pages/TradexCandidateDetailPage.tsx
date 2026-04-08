import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { loadTradexRun, loadTradexRunDetail, type TradexDetail, type TradexRun } from "../experimentApi";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

function SummaryMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="tradex-summary-metric">
      <span className="tradex-summary-metric-label">{label}</span>
      <strong className="tradex-summary-metric-value">{value}</strong>
    </div>
  );
}

const resolvePreferredCode = (targetCode: string, byCode?: Record<string, Record<string, unknown>> | undefined) => {
  if (targetCode.trim()) return targetCode.trim();
  if (!byCode) return "";
  const firstCode = Object.values(byCode)
    .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
    .sort((left, right) => {
      const leftCount = typeof left.signal_count === "number" ? left.signal_count : 0;
      const rightCount = typeof right.signal_count === "number" ? right.signal_count : 0;
      return rightCount - leftCount || String(left.code ?? "").localeCompare(String(right.code ?? ""));
    })[0]?.code;
  return typeof firstCode === "string" ? firstCode.trim() : "";
};

export default function TradexCandidateDetailPage() {
  const { runId: runIdParam } = useParams<{ runId?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const [run, setRun] = useState<TradexRun | null>(null);
  const [detail, setDetail] = useState<TradexDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [code, setCode] = useState<string>(
    searchParams.get("code")?.trim() || readTradexLocal<string>(tradexStorageKeys.detailCode, "")
  );
  const codeRef = useRef(code);

  const runId = runIdParam?.trim() || readTradexLocal<string>(tradexStorageKeys.runId, "");

  const codeOptions = useMemo(() => {
    const byCode = (run?.analysis as Record<string, unknown> | undefined)?.by_code as Record<string, Record<string, unknown>> | undefined;
    if (!byCode) return [];
    return Object.values(byCode)
      .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
      .sort((left, right) => {
        const leftCount = typeof left.signal_count === "number" ? left.signal_count : 0;
        const rightCount = typeof right.signal_count === "number" ? right.signal_count : 0;
        return rightCount - leftCount || String(left.code ?? "").localeCompare(String(right.code ?? ""));
      })
      .map((item) => String(item.code ?? ""))
      .filter(Boolean);
  }, [run]);

  const load = useCallback(async (targetRunId: string, targetCode: string) => {
    setLoading(true);
    setError(null);
    try {
      const runResponse = await loadTradexRun(targetRunId);
      setRun(runResponse.run);
      const byCode = (runResponse.run.analysis as Record<string, unknown> | undefined)?.by_code as Record<string, Record<string, unknown>> | undefined;
      const nextCode = resolvePreferredCode(targetCode, byCode);
      codeRef.current = nextCode;
      setCode(nextCode);
      writeTradexLocal(tradexStorageKeys.runId, targetRunId);
      if (nextCode) {
        const detailResponse = await loadTradexRunDetail(targetRunId, nextCode);
        setDetail(detailResponse.detail);
        writeTradexLocal(tradexStorageKeys.detailCode, nextCode);
        setSearchParams({ code: nextCode });
      } else {
        setDetail(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to load detail");
      setRun(null);
      setDetail(null);
    } finally {
      setLoading(false);
    }
  }, [setSearchParams]);

  useEffect(() => {
    codeRef.current = code;
  }, [code]);

  useEffect(() => {
    if (!runId) return;
    void load(runId, codeRef.current);
  }, [runId, load]);

  const loadSelectedCode = async (selectedCode: string) => {
    if (!runId) return;
    codeRef.current = selectedCode;
    setCode(selectedCode);
    writeTradexLocal(tradexStorageKeys.detailCode, selectedCode);
    setSearchParams({ code: selectedCode });
    try {
      const response = await loadTradexRunDetail(runId, selectedCode);
      setDetail(response.detail);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to load detail");
    }
  };

  if (!runId) {
    return (
      <div className="tradex-page tradex-detail-page">
        <section className="tradex-panel">
          <div className="tradex-panel-title">Detail</div>
          <p className="tradex-inline-note">select a run in verify first.</p>
        </section>
      </div>
    );
  }

  return (
    <div className="tradex-page tradex-detail-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">Run Detail</div>
            <div className="tradex-panel-caption">{runId}</div>
          </div>
          <div className="tradex-panel-actions">
            <Link className="tradex-secondary-action" to="/compare">compare</Link>
            <Link className="tradex-secondary-action" to="/adopt">adopt</Link>
          </div>
        </div>

        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}

        <div className="tradex-inline-grid">
          <div><span>plan</span><strong>{run?.plan_id ?? "--"}</strong></div>
          <div><span>kind</span><strong>{run?.run_kind ?? "--"}</strong></div>
          <div><span>status</span><strong>{run?.status ?? "--"}</strong></div>
          <div><span>family</span><strong>{run?.family_id ?? "--"}</strong></div>
        </div>

        <div className="tradex-action-row">
          {codeOptions.map((item) => {
            const byCode = (run?.analysis as Record<string, unknown> | undefined)?.by_code as Record<string, Record<string, unknown>> | undefined;
            const signalCount = byCode?.[item]?.signal_count;
            return (
              <button key={item} type="button" className="tradex-secondary-action" onClick={() => void loadSelectedCode(item)} disabled={loading}>
                {item}
                {typeof signalCount === "number" ? ` (${signalCount})` : ""}
              </button>
            );
          })}
        </div>

        <label className="tradex-field">
          <span>code</span>
          <input value={code} onChange={(event) => setCode(event.target.value)} />
        </label>
        <div className="tradex-action-row">
          <button type="button" className="tradex-primary-action" onClick={() => void loadSelectedCode(code)} disabled={loading || !code.trim()}>
            load detail
          </button>
        </div>

        {detail ? (
          <>
            <div className="tradex-summary-strip tradex-summary-strip--compact" aria-label="detail summary">
              {Object.entries(detail.summary ?? {}).map(([key, value]) => (
                <SummaryMetric key={key} label={key} value={typeof value === "number" ? value.toFixed(4) : JSON.stringify(value)} />
              ))}
            </div>

            <div className="tradex-inline-grid">
              <div><span>winning examples</span><strong>{JSON.stringify((detail.examples as Record<string, unknown>)?.winning ?? [])}</strong></div>
              <div><span>losing examples</span><strong>{JSON.stringify((detail.examples as Record<string, unknown>)?.losing ?? [])}</strong></div>
            </div>

            <details className="tradex-json-panel">
              <summary>detail json</summary>
              <pre>{JSON.stringify(detail, null, 2)}</pre>
            </details>
          </>
        ) : (
          <div className="tradex-empty-state">
            <strong>{loading ? "loading" : "detail not ready"}</strong>
            <p>pick a code to generate lazy detail cache.</p>
          </div>
        )}
      </section>
    </div>
  );
}
