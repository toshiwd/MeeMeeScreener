import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { tradexFetchJson } from "../http";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { buildComparisonDraft, findTradexCandidate } from "../data";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import { tradexCandidateStatusLabel, tradexDecisionActionLabel, tradexDecisionDirectionLabel } from "../labels";
import type { TradexAdoptResponse, TradexCandidate } from "../contracts";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
};

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

function ComparisonMetric({ label, value }: { label: string; value: number | string | null }) {
  return (
    <div className="tradex-metric-pill">
      <span>{label}</span>
      <strong>{value ?? "--"}</strong>
    </div>
  );
}

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
  const [submissionState, setSubmissionState] = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [message, setMessage] = useState<string | null>(null);
  const [reason, setReason] = useState("");

  useEffect(() => {
    if (!selectedCandidate) return;
    setSelectedId(selectedCandidate.candidate_id);
  }, [selectedCandidate]);

  const activeCandidate = findTradexCandidate(candidates, selectedId) ?? selectedCandidate ?? null;
  const comparison = activeCandidate
    ? buildComparisonDraft(data?.baseline ?? { logic_id: null, version: null, published_at: null, publish_id: null }, activeCandidate)
    : null;
  const readyToSubmit = Boolean(activeCandidate && comparison && comparisonConfirmed && submissionState !== "submitting");
  const adoptable = comparison?.decision_summary.suggested_action === "採用";

  const selectCandidate = (candidateId: string) => {
    setSelectedId(candidateId);
    writeTradexLocal(tradexStorageKeys.adoptCandidateId, candidateId);
    setSearchParams({ candidateId });
    setComparisonConfirmed(false);
    setSubmissionState("idle");
    setMessage(null);
    setReason("");
  };

  const submitAdoption = async () => {
    if (!activeCandidate || !comparison || !comparisonConfirmed) return;
    setSubmissionState("submitting");
    setMessage(null);
    try {
      const response = await tradexFetchJson<TradexAdoptResponse>("/tradex/adopt", {
        method: "POST",
        body: JSON.stringify({
          candidate_id: activeCandidate.candidate_id,
          baseline_publish_id: comparison.baseline_publish_id ?? "",
          comparison_snapshot_id: comparison.comparison_snapshot_id,
          reason: reason.trim() || undefined,
          actor: "tradex-ui"
        })
      });

      if (response.ok) {
        setSubmissionState("success");
        setMessage("採用要求を送信しました。backend の判定結果をご確認ください。");
        return;
      }

      setSubmissionState("error");
      setMessage("採用要求は拒否されました。現行版との差分確認と基準 ID を見直してください。");
    } catch (error) {
      setSubmissionState("error");
      setMessage(error instanceof Error ? error.message : "採用要求の送信に失敗しました。");
    }
  };

  return (
    <div className="tradex-page tradex-adopt-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">反映判定</div>
            <div className="tradex-panel-caption">
              候補比較で差分を確認したうえで、現行版に反映するかを判断します。採用要求は backend で差分照合されます。
            </div>
          </div>
          <Link className="tradex-secondary-action" to="/compare">
            候補比較へ
          </Link>
        </div>

        <div className="tradex-compare-layout">
          <aside className="tradex-compare-sidebar">
            <div className="tradex-section-title">候補を選ぶ</div>
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
                    <span className="tradex-pill is-muted">比較 {comparison.comparison_snapshot_id}</span>
                  </div>
                </div>

                <div className="tradex-compare-summaries">
                  <article className={`tradex-summary-card${adoptable ? " is-ok" : ""}${comparisonConfirmed ? " is-warn" : ""}`}>
                    <div className="tradex-summary-card-label">総合点差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(comparison.metric_deltas.total_score_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">最大損失差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(comparison.metric_deltas.max_drawdown_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">件数差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(comparison.metric_deltas.sample_count_delta, 0)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">勝率差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(comparison.metric_deltas.win_rate_delta, 4)}</div>
                  </article>
                  <article className="tradex-summary-card">
                    <div className="tradex-summary-card-label">期待値差分</div>
                    <div className="tradex-summary-card-value">{formatNumber(comparison.metric_deltas.expected_value_delta, 4)}</div>
                  </article>
                </div>

                <div className="tradex-compare-metric-row">
                  <ComparisonMetric label="現行順位" value={comparison.ranking_impact.current_rank ?? null} />
                  <ComparisonMetric label="候補順位" value={comparison.ranking_impact.candidate_rank ?? null} />
                  <ComparisonMetric label="順位変化" value={comparison.ranking_impact.rank_shift ?? null} />
                  <div className="tradex-metric-pill">
                    <span>方向</span>
                    <strong>{tradexDecisionDirectionLabel(comparison.ranking_impact.direction)}</strong>
                  </div>
                  <div className="tradex-metric-pill">
                    <span>判断</span>
                    <strong>{tradexDecisionActionLabel(comparison.decision_summary.suggested_action)}</strong>
                  </div>
                </div>

                <div className="tradex-decision-box">
                  <div className="tradex-decision-box-title">判断要約</div>
                  <div className="tradex-decision-box-headline">{comparison.decision_summary.headline || "--"}</div>
                  <p className="tradex-decision-box-detail">{comparison.decision_summary.detail || "判断材料を確認してください。"}</p>
                </div>

                <div className="tradex-form-stack">
                  <label className="tradex-checkbox-row">
                    <input
                      type="checkbox"
                      checked={comparisonConfirmed}
                      onChange={(event) => setComparisonConfirmed(event.target.checked)}
                    />
                    <span>現行版との差分を確認し、内容を理解したうえで判定します。</span>
                  </label>

                  <label className="tradex-field">
                    <span>判定理由</span>
                    <textarea
                      rows={4}
                      value={reason}
                      onChange={(event) => setReason(event.target.value)}
                      placeholder="採用または保留の理由を簡潔に記入してください。"
                    />
                  </label>

                  <div className="tradex-action-row">
                    <button type="button" className="tradex-primary-action" disabled={!readyToSubmit} onClick={submitAdoption}>
                      採用を送信
                    </button>
                    <span className="tradex-inline-note">
                      採用要求は backend の比較照合を通した場合のみ送信されます。差分確認が未完了なら送信できません。
                    </span>
                  </div>

                  {message ? (
                    <div className={`tradex-shell-alert ${submissionState === "error" ? "is-error" : "is-success"}`}>{message}</div>
                  ) : null}
                </div>

                <details className="tradex-json-panel">
                  <summary>比較データの詳細</summary>
                  <pre>{JSON.stringify(comparison, null, 2)}</pre>
                </details>
              </>
            ) : (
              <div className="tradex-empty-state">
                <strong>比較対象が見つかりません。</strong>
                <p>候補一覧から選ぶか、検証画面から候補を確認してください。</p>
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
