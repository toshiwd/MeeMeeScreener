import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { buildComparisonDraft, findTradexCandidate } from "../data";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import type { TradexCandidate } from "../contracts";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
};

function ComparisonMetric({ label, value }: { label: string; value: number | null | undefined }) {
  return (
    <div className="tradex-metric-pill">
      <span>{label}</span>
      <strong>{formatNumber(value, 4)}</strong>
    </div>
  );
}

function CandidateSelectList({
  candidates,
  selectedId,
  onSelect
}: {
  candidates: TradexCandidate[];
  selectedId: string | null;
  onSelect: (candidateId: string) => void;
}) {
  return (
    <div className="tradex-candidate-select-list">
      {candidates.slice(0, 12).map((candidate) => (
        <button
          key={candidate.candidate_id}
          type="button"
          className={`tradex-candidate-select-item${candidate.candidate_id === selectedId ? " is-active" : ""}`}
          onClick={() => onSelect(candidate.candidate_id)}
        >
          <strong>{candidate.name}</strong>
          <span>{candidate.candidate_id}</span>
          <span>{candidate.status}</span>
        </button>
      ))}
    </div>
  );
}

export default function TradexComparePage() {
  const { data } = useTradexBootstrap();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialCandidateId = searchParams.get("candidateId") || readTradexLocal<string>(tradexStorageKeys.compareCandidateId, "");
  const candidates = useMemo(() => data?.candidates ?? [], [data?.candidates]);
  const selectedCandidate = useMemo(
    () => findTradexCandidate(candidates, initialCandidateId) ?? candidates[0] ?? null,
    [candidates, initialCandidateId]
  );
  const [selectedId, setSelectedId] = useState<string | null>(selectedCandidate?.candidate_id ?? null);

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
    writeTradexLocal(tradexStorageKeys.compareCandidateId, candidateId);
    setSearchParams({ candidateId });
  };

  return (
    <div className="tradex-page tradex-compare-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">候補比較</div>
            <div className="tradex-panel-caption">候補同士と現行版との差分を、同じ高さで見比べます。</div>
          </div>
          <span className="tradex-pill is-muted">比較ファースト</span>
        </div>
        <div className="tradex-compare-layout">
          <aside className="tradex-compare-aside">
            <div className="tradex-subtitle">候補を選ぶ</div>
            <CandidateSelectList candidates={candidates} selectedId={activeCandidate?.candidate_id ?? null} onSelect={selectCandidate} />
          </aside>

          <section className="tradex-compare-main">
            <div className="tradex-compare-baseline">
              <div className="tradex-compare-card-head">
                <div className="tradex-subtitle">現行版</div>
                <span className="tradex-pill">{data?.baseline.publish_id ?? "--"}</span>
              </div>
              <div className="tradex-inline-grid">
                <div><span>logic_id</span><strong>{data?.baseline.logic_id ?? "--"}</strong></div>
                <div><span>version</span><strong>{data?.baseline.version ?? "--"}</strong></div>
                <div><span>published_at</span><strong>{data?.baseline.published_at ?? "--"}</strong></div>
              </div>
            </div>

            {!activeCandidate || !comparison ? (
              <div className="tradex-inline-note">候補を選ぶと比較結果が表示されます。</div>
            ) : (
              <>
                <div className="tradex-compare-candidate">
                  <div className="tradex-compare-card-head">
                    <div className="tradex-subtitle">選択候補</div>
                    <span className="tradex-pill">{activeCandidate.status}</span>
                  </div>
                  <div className="tradex-inline-grid">
                    <div><span>candidate_id</span><strong>{activeCandidate.candidate_id}</strong></div>
                    <div><span>name</span><strong>{activeCandidate.name}</strong></div>
                    <div><span>kind</span><strong>{activeCandidate.kind}</strong></div>
                    <div><span>sample_count</span><strong>{activeCandidate.sample_count == null ? "--" : activeCandidate.sample_count.toLocaleString("ja-JP")}</strong></div>
                  </div>
                </div>

                <div className="tradex-metric-row">
                  <ComparisonMetric label="総合点差" value={comparison.metric_deltas.total_score_delta} />
                  <ComparisonMetric label="最大損失差" value={comparison.metric_deltas.max_drawdown_delta} />
                  <ComparisonMetric label="件数差" value={comparison.metric_deltas.sample_count_delta} />
                  <ComparisonMetric label="勝率差" value={comparison.metric_deltas.win_rate_delta} />
                  <ComparisonMetric label="期待値差" value={comparison.metric_deltas.expected_value_delta} />
                </div>

                <div className="tradex-split-grid">
                  <article className="tradex-insight-card">
                    <div className="tradex-insight-label">ranking_impact</div>
                    <div className="tradex-insight-value">{comparison.ranking_impact.direction}</div>
                    <div className="tradex-insight-sub">{comparison.ranking_impact.note}</div>
                  </article>
                  <article className="tradex-insight-card">
                    <div className="tradex-insight-label">decision_summary</div>
                    <div className="tradex-insight-value">{comparison.decision_summary.headline}</div>
                    <div className="tradex-insight-sub">{comparison.decision_summary.detail}</div>
                  </article>
                </div>

                <details className="tradex-json-panel">
                  <summary>structured diff_vs_current</summary>
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
