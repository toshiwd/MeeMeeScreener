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

const resolveTone = (analysis: TradexAnalysisOutput) => {
  const { buy, neutral, sell } = analysis.sideRatios;
  if (buy >= sell && buy >= neutral) return { label: "買い寄り", tone: "up" as const };
  if (sell >= buy && sell >= neutral) return { label: "売り寄り", tone: "down" as const };
  return { label: "中立", tone: "neutral" as const };
};

const resolveVersion = (analysis: TradexAnalysisOutput) =>
  analysis.overrideState.logicVersion ?? analysis.overrideState.logicKey ?? "--";

export function TradexAnalysisPanel({ state, formatPercentLabel, formatSignedPercentLabel, formatNumber }: Props) {
  const analysis = state.analysis;
  const reasons = analysis?.reasons.slice(0, 3) ?? [];
  const comparisons = analysis?.candidateComparisons.slice(0, 3) ?? [];
  const toneInfo = analysis ? resolveTone(analysis) : null;
  const versionLabel = analysis ? resolveVersion(analysis) : "--";

  return (
    <div className="daily-memo-panel detail-analysis-panel">
      <div className="memo-panel-header">
        <h3>判定確認</h3>
        <div className="detail-analysis-header-note">published logic / read only</div>
      </div>
      <div className="detail-analysis-body">
        {state.loading && <div className="detail-analysis-empty">暫定取得中です。</div>}
        {!state.loading && !state.available && (
          <div className="detail-analysis-empty">analysis unavailable: {state.reason ?? "analysis unavailable"}</div>
        )}
        {!state.loading && state.available && analysis && (
          <>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">判定要点</div>
              <div className="detail-analysis-grid">
                <div className="detail-analysis-card">
                  <div className="detail-analysis-label">tone</div>
                  <div className="detail-analysis-value">
                    <span className={`detail-analysis-tone-badge detail-analysis-tone-badge--${toneInfo?.tone ?? "neutral"}`}>
                      {toneInfo?.label ?? "--"}
                    </span>
                  </div>
                </div>
                <div className="detail-analysis-card">
                  <div className="detail-analysis-label">confidence</div>
                  <div className="detail-analysis-value">
                    {analysis.confidence != null ? formatPercentLabel(analysis.confidence) : "--"}
                  </div>
                </div>
                <div className="detail-analysis-card">
                  <div className="detail-analysis-label">version</div>
                  <div className="detail-analysis-value">{versionLabel}</div>
                </div>
                <div className="detail-analysis-card">
                  <div className="detail-analysis-label">asof</div>
                  <div className="detail-analysis-value">{analysis.asof}</div>
                </div>
              </div>
              <div className="detail-analysis-meta">
                symbol {analysis.symbol} / publish {analysis.publishReadiness.ready ? "ready" : analysis.publishReadiness.status}
              </div>
              <div className="detail-analysis-meta">
                {analysis.publishReadiness.reasons.length
                  ? analysis.publishReadiness.reasons.slice(0, 2).join(" / ")
                  : "--"}
              </div>
              <div className="detail-analysis-meta">
                override {analysis.overrideState.present ? "present" : "none"}
                {analysis.overrideState.source ? ` / ${analysis.overrideState.source}` : ""}
                {analysis.overrideState.logicVersion ? ` / ${analysis.overrideState.logicVersion}` : ""}
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
              <div className="detail-analysis-section-title">Side ratios</div>
              <div className="detail-analysis-meta">
                買い {formatPercentLabel(analysis.sideRatios.buy)} / 中立 {formatPercentLabel(analysis.sideRatios.neutral)} / 売り {formatPercentLabel(analysis.sideRatios.sell)}
              </div>
              <div className="detail-analysis-meta">Top 3 candidate comparisons</div>
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
