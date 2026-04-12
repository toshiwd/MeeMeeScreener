import type {
  TradexAnalysisOutput,
  TradexAnalysisReadResult,
  TradexForecastSurfaceRow,
} from "./detailTypes";

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
  if (item.rank != null) parts.push(`rank ${item.rank}`);
  if (item.publishReady != null) parts.push(item.publishReady ? "ready" : "not ready");
  return parts.join(" / ");
};

const renderForecastRowLabel = (row: TradexForecastSurfaceRow) => {
  const parts = [row.side ?? "--"];
  if (row.code) parts.push(row.code);
  return parts.join(" / ");
};

const resolveTone = (analysis: TradexAnalysisOutput) => {
  const { buy, neutral, sell } = analysis.sideRatios;
  if (buy >= sell && buy >= neutral) return { label: "up", tone: "up" as const };
  if (sell >= buy && sell >= neutral) return { label: "down", tone: "down" as const };
  return { label: "neutral", tone: "neutral" as const };
};

const resolveVersion = (analysis: TradexAnalysisOutput) =>
  analysis.overrideState.logicVersion ?? analysis.overrideState.logicKey ?? "--";

const resolvePromotionState = (analysis: TradexAnalysisOutput) => {
  const review = analysis.promotionReview;
  if (!review) return { label: "shadow", tone: "neutral" as const, note: "promotion review unavailable" };
  const approval = review.approvalDecision?.decision ?? null;
  if (approval === "approved" && review.readinessPass) {
    return { label: "authoritative", tone: "up" as const, note: "promotion approved" };
  }
  if (approval === "hold") {
    return { label: "shadow", tone: "neutral" as const, note: "promotion on hold" };
  }
  if (approval === "rejected") {
    return { label: "shadow", tone: "down" as const, note: "promotion rejected" };
  }
  return {
    label: review.readinessPass ? "promotion-ready" : "shadow",
    tone: review.readinessPass ? ("up" as const) : ("neutral" as const),
    note: review.readinessPass ? "ready but not approved" : "promotion review pending",
  };
};

export function TradexAnalysisPanel({ state, formatPercentLabel, formatSignedPercentLabel, formatNumber }: Props) {
  const analysis = state.analysis;
  const forecastSurfaceRows = state.forecastSurface?.rows ?? analysis?.forecastSurface?.rows ?? [];
  const reasons = analysis?.reasons.slice(0, 3) ?? [];
  const comparisons = analysis?.candidateComparisons.slice(0, 3) ?? [];
  const toneInfo = analysis ? resolveTone(analysis) : null;
  const promotionInfo = analysis ? resolvePromotionState(analysis) : null;
  const versionLabel = analysis ? resolveVersion(analysis) : "--";
  const title = analysis?.displayLabel ?? "TRADEX";
  const hasForecastSurface = forecastSurfaceRows.length > 0;

  return (
    <div className="daily-memo-panel detail-analysis-panel">
      <div className="memo-panel-header">
        <h3>{title}</h3>
        <div className="detail-analysis-header-note">published logic / read only</div>
      </div>
      <div className="detail-analysis-body">
        {state.loading && <div className="detail-analysis-empty">loading detail analysis...</div>}
        {!state.loading && !state.available && !hasForecastSurface && (
          <div className="detail-analysis-empty">analysis unavailable: {state.reason ?? "analysis unavailable"}</div>
        )}
        {!state.loading && analysis && (
          <>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">Decision summary</div>
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
                <div className="detail-analysis-card">
                  <div className="detail-analysis-label">promotion</div>
                  <div className="detail-analysis-value">
                    <span className={`detail-analysis-tone-badge detail-analysis-tone-badge--${promotionInfo?.tone ?? "neutral"}`}>
                      {promotionInfo?.label ?? "--"}
                    </span>
                  </div>
                </div>
              </div>
              <div className="detail-analysis-meta">
                symbol {analysis.symbol} / publish {analysis.publishReadiness.ready ? "ready" : analysis.publishReadiness.status}
              </div>
              <div className="detail-analysis-meta">
                promotion {promotionInfo?.note ?? "--"}
                {analysis.promotionReview?.approvalDecision?.decision
                  ? ` / decision ${analysis.promotionReview.approvalDecision.decision}`
                  : ""}
              </div>
              {analysis.promotionReview && (
                <div className="detail-analysis-meta">
                  review {analysis.promotionReview.readinessPass ? "pass" : "hold"}
                  {analysis.promotionReview.sampleCount != null ? ` / samples ${analysis.promotionReview.sampleCount}` : ""}
                  {analysis.promotionReview.expectancyDelta != null
                    ? ` / delta ${formatSignedPercentLabel(analysis.promotionReview.expectancyDelta)}`
                    : ""}
                </div>
              )}
              <div className="detail-analysis-meta">source {analysis.source ?? "--"}</div>
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
                <div className="detail-analysis-empty">reason unavailable</div>
              )}
            </div>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">Side ratios</div>
              <div className="detail-analysis-meta">
                buy {formatPercentLabel(analysis.sideRatios.buy)} / neutral {formatPercentLabel(analysis.sideRatios.neutral)} / sell{" "}
                {formatPercentLabel(analysis.sideRatios.sell)}
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
                        {item.scoreDelta != null ? ` / delta ${formatSignedPercentLabel(item.scoreDelta)}` : ""}
                      </div>
                      {item.reasons.length > 0 && <div className="detail-analysis-meta">{item.reasons.slice(0, 2).join(" / ")}</div>}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="detail-analysis-empty">candidate comparisons unavailable</div>
              )}
            </div>
            <div className="detail-analysis-meta">
              TRADEX output schema {analysis ? "fixed" : "--"} / confidence raw {formatNumber(analysis.confidence, 3)}
            </div>
          </>
        )}
        {!state.loading && hasForecastSurface && (
          <div className="detail-analysis-section">
            <div className="detail-analysis-section-title">Forecast surface</div>
            {state.forecastSurface?.asof && <div className="detail-analysis-meta">asof {state.forecastSurface.asof}</div>}
            <div className="detail-analysis-call-reason-list">
              {forecastSurfaceRows.map((row, index) => (
                <div key={`${row.code ?? title}:${row.side ?? "side"}:${index}`} className="detail-analysis-call-reason">
                  <div>{renderForecastRowLabel(row)}</div>
                  <div className="detail-analysis-meta">
                    action {row.actionState ?? "--"} / p {formatPercentLabel(row.directionProb)} / expected_upside{" "}
                    {formatSignedPercentLabel(row.expectedUpside)} / expected_downside{" "}
                    {formatSignedPercentLabel(row.expectedDownside)}
                  </div>
                  <div className="detail-analysis-meta">
                    invalidation {row.invalidationPrice != null ? formatNumber(row.invalidationPrice) : "--"} / freshness{" "}
                    {row.freshnessState ?? "--"}
                  </div>
                  <div className="detail-analysis-meta">
                    {row.reasonCodes.length > 0 ? row.reasonCodes.slice(0, 4).join(" / ") : "--"}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default TradexAnalysisPanel;
