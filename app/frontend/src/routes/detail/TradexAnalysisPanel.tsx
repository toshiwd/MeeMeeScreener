import type { TradexAnalysisOutput, TradexAnalysisReadResult } from "./detailTypes";

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatSignedPercentLabel = (value: number | null | undefined, digits?: number) => string;

type Props = {
  state: TradexAnalysisReadResult & { loading: boolean };
  formatPercentLabel: FormatPercentLabel;
  formatSignedPercentLabel: FormatSignedPercentLabel;
  formatNumber: FormatNumber;
};

const renderReason = (value: string) => value.trim() || "--";

const renderComparisonLabel = (item: TradexAnalysisOutput["candidateComparisons"][number]) => {
  const parts = [item.candidateKey];
  if (item.rank != null) {
    parts.push(`rank ${item.rank}`);
  }
  if (item.publishReady != null) {
    parts.push(item.publishReady ? "ready" : "not ready");
  }
  return parts.join(" / ");
};

export function TradexAnalysisPanel({ state, formatPercentLabel, formatSignedPercentLabel, formatNumber }: Props) {
  const analysis = state.analysis;
  const reasons = analysis?.reasons.slice(0, 3) ?? [];
  const comparisons = analysis?.candidateComparisons.slice(0, 3) ?? [];

  return (
    <div className="daily-memo-panel detail-analysis-panel">
      <div className="memo-panel-header">
        <h3>TRADEX Read-only</h3>
      </div>
      <div className="detail-analysis-body">
        {state.loading && <div className="detail-analysis-empty">TRADEX 分析を読込中です。</div>}
        {!state.loading && !state.available && (
          <div className="detail-analysis-empty">analysis unavailable: {state.reason ?? "analysis unavailable"}</div>
        )}
        {!state.loading && state.available && analysis && (
          <>
            <div className="detail-analysis-meta">symbol {analysis.symbol}</div>
            <div className="detail-analysis-meta">asof {analysis.asof}</div>
            <div className="detail-analysis-grid">
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">買い比率</div>
                <div className="detail-analysis-value detail-analysis-value--up">
                  {formatPercentLabel(analysis.sideRatios.buy)}
                </div>
              </div>
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">中立比率</div>
                <div className="detail-analysis-value detail-analysis-value--neutral">
                  {formatPercentLabel(analysis.sideRatios.neutral)}
                </div>
              </div>
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">売り比率</div>
                <div className="detail-analysis-value detail-analysis-value--down">
                  {formatPercentLabel(analysis.sideRatios.sell)}
                </div>
              </div>
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">Confidence</div>
                <div className="detail-analysis-value">
                  {analysis.confidence != null ? formatPercentLabel(analysis.confidence) : "--"}
                </div>
              </div>
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">Publish readiness</div>
                <div className="detail-analysis-value">
                  {analysis.publishReadiness.ready ? "ready" : analysis.publishReadiness.status}
                </div>
                <div className="detail-analysis-meta">
                  {analysis.publishReadiness.reasons.length
                    ? analysis.publishReadiness.reasons.slice(0, 2).join(" / ")
                    : "--"}
                </div>
              </div>
              <div className="detail-analysis-card">
                <div className="detail-analysis-label">Override state</div>
                <div className="detail-analysis-value">
                  {analysis.overrideState.present ? "present" : "none"}
                </div>
                <div className="detail-analysis-meta">
                  {analysis.overrideState.source ?? "--"}
                </div>
                <div className="detail-analysis-meta">
                  {analysis.overrideState.logicKey ?? "--"}
                  {analysis.overrideState.logicVersion ? ` / ${analysis.overrideState.logicVersion}` : ""}
                </div>
              </div>
            </div>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">Top 3 reasons</div>
              {reasons.length > 0 ? (
                <div className="detail-analysis-call-reason-list">
                  {reasons.map((reason) => (
                    <div key={reason} className="detail-analysis-call-reason">
                      {renderReason(reason)}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="detail-analysis-empty">理由はありません。</div>
              )}
            </div>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">Top 3 candidate comparisons</div>
              {comparisons.length > 0 ? (
                <div className="detail-analysis-call-reason-list">
                  {comparisons.map((item) => (
                    <div key={`${item.candidateKey}:${item.rank ?? 0}`} className="detail-analysis-call-reason">
                      <div>{renderComparisonLabel(item)}</div>
                      <div className="detail-analysis-meta">
                        {item.comparisonScope}
                        {item.score != null ? ` / score ${formatPercentLabel(item.score)}` : ""}
                        {item.scoreDelta != null ? ` / Δ ${formatSignedPercentLabel(item.scoreDelta)}` : ""}
                      </div>
                      {item.reasons.length > 0 && (
                        <div className="detail-analysis-meta">{item.reasons.slice(0, 2).join(" / ")}</div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="detail-analysis-empty">候補比較はありません。</div>
              )}
            </div>
            <div className="detail-analysis-meta">
              TRADEX output schema {analysis ? "fixed" : "--"} / confidence raw {formatNumber(analysis.confidence, 3)}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default TradexAnalysisPanel;
