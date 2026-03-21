import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { buildComparisonDraft, findTradexCandidate } from "../data";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import { tradexCandidateStatusLabel, tradexDecisionActionLabel, tradexDecisionDirectionLabel } from "../labels";
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
          <span>{tradexCandidateStatusLabel(candidate.status)}</span>
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

  const metricDeltas = comparison?.metric_deltas;
  const rankingImpact = comparison?.ranking_impact;
  const decisionSummary = comparison?.decision_summary;

  return (
    <div className="tradex-page tradex-compare-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">候補比較</div>
            <div className="tradex-panel-caption">現行版との差分を見てから、候補同士を比較します。採用判断の前に比較が必須です。</div>
          </div>
          <Link className="tradex-secondary-action" to="/adopt">反映判定へ</Link>
        </div>

        <div className="tradex-compare-layout">
          <aside className="tradex-compare-sidebar">
            <div className="tradex-section-title">候補一覧</div>
            <CandidateSelectList candidates={candidates} selectedId={selectedId} onSelect={selectCandidate} />
          </aside>

          <div className="tradex-compare-main">
            {activeCandidate && comparison ? (
              <>
                <div className="tradex-compare-header">
                  <div>
                    <div className="tradex-panel-title">{activeCandidate.name}</div>
                    <div className="tradex-panel-caption">
                      {activeCandidate.candidate_id} / {activeCandidate.logic_key}
                    </div>
                  </div>
                  <div className="tradex-compare-header-meta">
                    <span className="tradex-pill">{tradexCandidateStatusLabel(activeCandidate.status)}</span>
                    <span className="tradex-pill is-muted">基準 {comparison.baseline_publish_id ?? "--"}</span>
                    <span className="tradex-pill is-muted">比較ID {comparison.comparison_snapshot_id}</span>
                  </div>
                </div>

                <div className="tradex-compare-summaries">
                  <article className="tradex-summary-card is-ok">
                    <div className="tradex-summary-card-label">総合点</div>
                    <div className="tradex-summary-card-value">{formatNumber(metricDeltas?.total_score_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">最大損失</div>
                    <div className="tradex-summary-card-value">{formatNumber(metricDeltas?.max_drawdown_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">件数差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(metricDeltas?.sample_count_delta, 0)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">勝率差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(metricDeltas?.win_rate_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">期待値差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(metricDeltas?.expected_value_delta, 4)}</div>
                  </article>
                </div>

                <div className="tradex-compare-metric-row">
                  <ComparisonMetric label="現在順位" value={rankingImpact?.current_rank ?? null} />
                  <ComparisonMetric label="候補順位" value={rankingImpact?.candidate_rank ?? null} />
                  <ComparisonMetric label="順位変化" value={rankingImpact?.rank_shift ?? null} />
                  <div className="tradex-metric-pill">
                    <span>方向</span>
                    <strong>{tradexDecisionDirectionLabel(rankingImpact?.direction)}</strong>
                  </div>
                  <div className="tradex-metric-pill">
                    <span>推奨</span>
                    <strong>{tradexDecisionActionLabel(decisionSummary?.suggested_action)}</strong>
                  </div>
                </div>

                <div className="tradex-decision-box">
                  <div className="tradex-decision-box-title">判断サマリー</div>
                  <div className="tradex-decision-box-headline">{decisionSummary?.headline ?? "--"}</div>
                  <p className="tradex-decision-box-detail">{decisionSummary?.detail ?? "比較結果を確認してください。"}</p>
                </div>

                <details className="tradex-json-panel">
                  <summary>現行版との差分の詳細</summary>
                  <pre>{JSON.stringify(comparison, null, 2)}</pre>
                </details>
              </>
            ) : (
              <div className="tradex-empty-state">
                <strong>比較対象が見つかりません。</strong>
                <p>候補一覧から比較したい研究候補を選んでください。</p>
                <div className="tradex-empty-actions">
                  <Link to="/verify">検証へ</Link>
                </div>
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
