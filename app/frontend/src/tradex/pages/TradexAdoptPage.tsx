import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { buildComparisonDraft, findTradexCandidate } from "../data";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
};

export default function TradexAdoptPage() {
  const { data } = useTradexBootstrap();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialCandidateId = searchParams.get("candidateId") || readTradexLocal<string>(tradexStorageKeys.adoptCandidateId, "");
  const candidates = useMemo(() => data?.candidates ?? [], [data?.candidates]);
  const selectedCandidate = useMemo(
    () => findTradexCandidate(candidates, initialCandidateId) ?? candidates[0] ?? null,
    [candidates, initialCandidateId]
  );
  const [selectedId, setSelectedId] = useState<string | null>(selectedCandidate?.candidate_id ?? null);
  const [comparisonConfirmed, setComparisonConfirmed] = useState(false);
  const [savedNote, setSavedNote] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedCandidate) return;
    setSelectedId(selectedCandidate.candidate_id);
  }, [selectedCandidate]);

  const activeCandidate = findTradexCandidate(candidates, selectedId) ?? selectedCandidate ?? null;
  const comparison = activeCandidate
    ? buildComparisonDraft(data?.baseline ?? { logic_id: null, version: null, published_at: null, publish_id: null }, activeCandidate)
    : null;

  const selectCandidate = (candidateId: string) => {
    setSelectedId(candidateId);
    writeTradexLocal(tradexStorageKeys.adoptCandidateId, candidateId);
    setSearchParams({ candidateId });
  };

  const savePending = () => {
    if (!activeCandidate) return;
    writeTradexLocal(
      tradexStorageKeys.adoptCandidateId,
      JSON.stringify({ candidateId: activeCandidate.candidate_id, confirmed: comparisonConfirmed, at: new Date().toISOString() })
    );
    setSavedNote("採用は保留として記録しました。backend の最終強制はまだ未実装です。");
  };

  return (
    <div className="tradex-page tradex-adopt-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">反映判定</div>
            <div className="tradex-panel-caption">比較確認を済ませてから、採用するか保留にするかを決めます。</div>
          </div>
          <span className="tradex-pill is-warn">backend enforcement 保留</span>
        </div>
        <div className="tradex-adopt-layout">
          <aside className="tradex-compare-aside">
            <div className="tradex-subtitle">候補を選ぶ</div>
            <div className="tradex-candidate-select-list">
              {candidates.slice(0, 12).map((candidate) => (
                <button
                  key={candidate.candidate_id}
                  type="button"
                  className={`tradex-candidate-select-item${candidate.candidate_id === activeCandidate?.candidate_id ? " is-active" : ""}`}
                  onClick={() => selectCandidate(candidate.candidate_id)}
                >
                  <strong>{candidate.name}</strong>
                  <span>{candidate.candidate_id}</span>
                  <span>{candidate.status}</span>
                </button>
              ))}
            </div>
          </aside>

          <section className="tradex-adopt-main">
            {!activeCandidate || !comparison ? (
              <div className="tradex-inline-note">候補を選ぶと反映判定が表示されます。</div>
            ) : (
              <>
                <div className="tradex-warning-card">
                  <div className="tradex-subtitle">比較確認が前提</div>
                  <p>
                    ここでは現行版との差分を見たうえで、採用するか保留にするかを決めます。
                    最終採用の backend enforcement はまだ未実装なので、今は保留記録までに留めています。
                  </p>
                </div>

                <div className="tradex-inline-grid">
                  <div><span>candidate_id</span><strong>{activeCandidate.candidate_id}</strong></div>
                  <div><span>name</span><strong>{activeCandidate.name}</strong></div>
                  <div><span>baseline_publish_id</span><strong>{comparison.baseline_publish_id ?? "--"}</strong></div>
                  <div><span>expected_value_delta</span><strong>{formatNumber(comparison.metric_deltas.expected_value_delta, 4)}</strong></div>
                </div>

                <label className="tradex-check-row">
                  <input type="checkbox" checked={comparisonConfirmed} onChange={(event) => setComparisonConfirmed(event.target.checked)} />
                  <span>現行版との差分を確認した</span>
                </label>

                <div className="tradex-adopt-actions">
                  <button type="button" className="tradex-primary-action" disabled={!comparisonConfirmed} onClick={savePending}>
                    保留として記録
                  </button>
                  <Link to={`/compare?candidateId=${encodeURIComponent(activeCandidate.candidate_id)}`} className="tradex-secondary-action">
                    比較に戻る
                  </Link>
                  <Link to={`/detail/${encodeURIComponent(activeCandidate.candidate_id)}`} className="tradex-secondary-action">
                    候補詳細へ
                  </Link>
                </div>

                {savedNote ? <div className="tradex-inline-note">{savedNote}</div> : null}

                <details className="tradex-json-panel">
                  <summary>比較結果</summary>
                  <pre>{JSON.stringify(comparison, null, 2)}</pre>
                </details>
              </>
            )}
          </section>
        </div>
      </section>
    </div>
  );
}
