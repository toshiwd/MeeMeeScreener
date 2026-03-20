// @ts-nocheck
import type {
  AnalysisEntryPolicy,
  AnalysisEntryPolicySide,
  AnalysisResearchPrior,
  AnalysisSwingDiagnostics,
  AnalysisSwingPlan,
  AnalysisSwingSetupExpectancy,
  EnvironmentSummary,
} from "./detailTypes";
import ScreenPanel from "../../components/ScreenPanel";

type AnalysisDecisionSummary = {
  tone: "up" | "down" | "neutral";
  sideLabel: string | null;
  buyProb: number | null;
  sellProb: number | null;
  neutralProb: number | null;
};

type AnalysisGuidance = {
  confidenceRank: string;
  action: string;
  watchpoint: string;
  buyWidth: number;
  sellWidth: number;
  neutralWidth: number;
  buySetupProb: number | null;
  sellSetupProb: number | null;
  buySetupWidth: number;
  sellSetupWidth: number;
  buySetupState: string;
  sellSetupState: string;
};

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatSignedPercentLabel = (value: number | null | undefined, digits?: number) => string;

export type Props = {
  analysisAsOfTime: number | null;
  analysisBackfillActive: boolean;
  analysisRecalcSubmitting: "current" | "auto" | "batch" | null;
  analysisRecalcDisabled: boolean;
  analysisRecalcDisabledReason: string | null;
  submitAnalysisRecalc: () => Promise<void>;
  analysisDtLabel: string | null;
  cursorMode: boolean;
  analysisCursorDateLabel: string | null;
  canShowPhase: boolean;
  phaseReasons: readonly string[];
  canShowAnalysis: boolean;
  analysisDecision: AnalysisDecisionSummary;
  analysisSummaryLoading: boolean;
  analysisGuidance: AnalysisGuidance;
  analysisEntryPolicy: AnalysisEntryPolicy | null;
  patternSummary: EnvironmentSummary;
  analysisPreparationVisible: boolean;
  analysisBackfillProgressLabel: string | null;
  analysisBackfillMessage: string | null;
  sellAnalysisDtLabel: string | null;
  sellPredDtLabel: string | null;
  researchPriorRunId: string | null;
  analysisResearchPrior: AnalysisResearchPrior | null;
  researchPriorUpMeta: string;
  researchPriorDownMeta: string;
  edinetStatusMeta: string | null;
  edinetQualityMeta: string | null;
  edinetMetricsMeta: string | null;
  edinetBonusMeta: string | null;
  hasSwingData: boolean;
  swingPlan: AnalysisSwingPlan | null;
  swingSideLabel: string;
  swingReasonsLabel: string;
  swingDiagnostics: AnalysisSwingDiagnostics | null;
  swingSetupExpectancy: AnalysisSwingSetupExpectancy | null;
  analysisMissingDataVisible: boolean;
  formatPercentLabel: FormatPercentLabel;
  formatNumber: FormatNumber;
  formatSignedPercentLabel: FormatSignedPercentLabel;
  onSubmitAnalysisRecalc: () => void;
};

const formatSetupIntent = (side: "buy" | "sell", setupType?: string | null) => {
  if (!setupType) return side === "buy" ? "押し目待ち" : "戻り待ち";
  if (setupType === "target20_breakout" || setupType === "breakout20") return "20%値幅狙い";
  if (setupType === "breakout" || setupType === "breakout_trend") {
    return side === "buy" ? "上放れ追随" : "下抜け追随";
  }
  if (setupType === "breakdown") return "下抜け追随";
  if (setupType === "accumulation" || setupType === "accumulation_break") {
    return side === "buy" ? "押し目再開待ち" : "戻り売り再開待ち";
  }
  if (setupType === "continuation") return side === "buy" ? "上昇継続取り" : "下落継続取り";
  if (setupType === "pressure") return "売り圧継続";
  if (setupType === "rebound") return side === "buy" ? "反発初動狙い" : "戻り売り再始動";
  if (setupType === "turn") return side === "buy" ? "反転待ち" : "反落待ち";
  if (setupType === "watch" || setupType === "watchlist") {
    return side === "buy" ? "押し目条件待ち" : "戻り条件待ち";
  }
  if (setupType === "ml_fallback_down") return "下落継続の補完候補";
  return setupType;
};

const formatCandidateState = (side: "buy" | "sell", state?: string | null) => {
  if (state === "実行") return "実行候補";
  if (state === "監視") return "監視候補";
  if (state === "待機") return side === "buy" ? "押し目待ち" : "戻り待ち";
  return "--";
};

const formatHoldWindow = (policy: AnalysisEntryPolicySide | null) => {
  if (
    Number.isFinite(policy?.recommendedHoldMinDays ?? NaN) &&
    Number.isFinite(policy?.recommendedHoldMaxDays ?? NaN)
  ) {
    const minDays = Math.round(policy?.recommendedHoldMinDays ?? 0);
    const maxDays = Math.round(policy?.recommendedHoldMaxDays ?? 0);
    if (minDays > 0 && maxDays > 0 && minDays !== maxDays) return `${minDays}-${maxDays}営業日目安`;
  }
  if (Number.isFinite(policy?.recommendedHoldDays ?? NaN)) {
    return `${Math.round(policy?.recommendedHoldDays ?? 0)}営業日目安`;
  }
  return "--";
};

const formatInvalidationTrigger = (value?: string | null) => {
  if (!value) return "--";
  if (value === "box_break") return "Box下抜け";
  if (value === "box_reclaim") return "Box回復";
  if (value === "stop3") return "-3%下振れ";
  if (value === "stop5") return "-5%下振れ";
  if (value === "ma20") return "MA20割れ";
  return value;
};

const formatInvalidationAction = (value?: string | null) => {
  if (!value) return "--";
  if (value === "exit") return "撤退";
  if (value === "hold") return "様子見";
  if (value === "doten_opt") return "ドテン検討";
  if (value === "doten_remainder") return "一部ドテン";
  return value;
};

const formatExitPlan = (policy: AnalysisEntryPolicySide | null) => {
  const trigger = formatInvalidationTrigger(policy?.invalidationTrigger);
  const action = formatInvalidationAction(policy?.invalidationRecommendedAction);
  if (trigger === "--" && action === "--") return "--";
  let summary = `${trigger}で${action}`;
  if (
    policy?.invalidationDotenRecommended &&
    Number.isFinite(policy?.invalidationOppositeHoldDays ?? NaN)
  ) {
    summary += `、反対側へ${Math.round(policy?.invalidationOppositeHoldDays ?? 0)}日目安`;
  }
  return summary;
};

export function DetailAnalysisPanel(props: Props) {
  const {
    analysisAsOfTime,
    analysisBackfillActive,
    analysisRecalcSubmitting,
    analysisRecalcDisabled,
    analysisRecalcDisabledReason,
    submitAnalysisRecalc,
    analysisDtLabel,
    cursorMode,
    analysisCursorDateLabel,
    canShowPhase,
    phaseReasons,
    canShowAnalysis,
    analysisDecision,
    analysisSummaryLoading,
    analysisGuidance,
    analysisEntryPolicy,
    patternSummary,
    analysisPreparationVisible,
    analysisBackfillProgressLabel,
    analysisBackfillMessage,
    sellAnalysisDtLabel,
    sellPredDtLabel,
    researchPriorRunId,
    analysisResearchPrior,
    researchPriorUpMeta,
    researchPriorDownMeta,
    edinetStatusMeta,
    edinetQualityMeta,
    edinetMetricsMeta,
    edinetBonusMeta,
    hasSwingData,
    swingPlan,
    swingSideLabel,
    swingReasonsLabel,
    swingDiagnostics,
    swingSetupExpectancy,
    analysisMissingDataVisible,
    formatPercentLabel,
    formatNumber,
    formatSignedPercentLabel,
    onSubmitAnalysisRecalc,
  } = props;
  const buyPolicy = analysisEntryPolicy?.up ?? null;
  const sellPolicy = analysisEntryPolicy?.down ?? null;
  const hasAnalysisSummary = canShowAnalysis;

  return (
    <ScreenPanel title="判定確認" className="detail-analysis-panel">
      <div className="detail-analysis-body">
        {analysisDtLabel && <div className="detail-analysis-meta">基準日 {analysisDtLabel}</div>}
        {canShowAnalysis ? (
          <>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">要約</div>
              <div className={`detail-analysis-regime detail-analysis-regime--${analysisDecision.tone}`}>
                <div className="detail-analysis-call-head">
                  <span className={`detail-analysis-call-badge detail-analysis-call-badge--${analysisDecision.tone}`}>
                    判定 {analysisDecision.sideLabel}
                  </span>
                  <span className="detail-analysis-call-confidence">
                    確信度 {analysisSummaryLoading ? "暫定" : analysisGuidance.confidenceRank}
                  </span>
                </div>
                <div className="detail-analysis-call-action">狙い: {analysisDecision.patternLabel ?? "--"}</div>
                <div className="detail-analysis-call-pattern">地合い: {patternSummary.environmentLabel}</div>
                <div className="detail-analysis-regime-text">
                  今やること: {analysisSummaryLoading ? "暫定" : analysisGuidance.watchpoint}
                </div>
                <div className="detail-analysis-entry-plan detail-analysis-entry-plan--up">
                  <div className="detail-analysis-entry-plan-title">買い候補</div>
                  <div className="detail-analysis-entry-plan-item">
                    状態 {analysisGuidance.buySetupState} / {formatCandidateState("buy", analysisGuidance.buySetupState)}
                  </div>
                  <div className="detail-analysis-entry-plan-item">
                    狙い {formatSetupIntent("buy", buyPolicy?.setupType)}
                  </div>
                  <div className="detail-analysis-entry-plan-item">
                    出口 {formatHoldWindow(buyPolicy)} / {formatExitPlan(buyPolicy)}
                  </div>
                  {buyPolicy?.recommendedHoldReason && (
                    <div className="detail-analysis-entry-plan-item">補足 {buyPolicy.recommendedHoldReason}</div>
                  )}
                </div>
                <div className="detail-analysis-entry-plan detail-analysis-entry-plan--down">
                  <div className="detail-analysis-entry-plan-title">売り候補</div>
                  <div className="detail-analysis-entry-plan-item">
                    状態 {analysisGuidance.sellSetupState} / {formatCandidateState("sell", analysisGuidance.sellSetupState)}
                  </div>
                  <div className="detail-analysis-entry-plan-item">
                    狙い {formatSetupIntent("sell", sellPolicy?.setupType)}
                  </div>
                  <div className="detail-analysis-entry-plan-item">
                    出口 {formatHoldWindow(sellPolicy)} / {formatExitPlan(sellPolicy)}
                  </div>
                  {sellPolicy?.recommendedHoldReason && (
                    <div className="detail-analysis-entry-plan-item">補足 {sellPolicy.recommendedHoldReason}</div>
                  )}
                </div>
                <div className="detail-analysis-prob-meter-list">
                  <div className="detail-analysis-prob-meter-row tone-up">
                    <div className="detail-analysis-prob-meter-label">
                      上昇確率 {analysisSummaryLoading ? "暫定" : formatPercentLabel(analysisDecision.buyProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buyWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-down">
                    <div className="detail-analysis-prob-meter-label">
                      下落確率 {analysisSummaryLoading ? "暫定" : formatPercentLabel(analysisDecision.sellProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-neutral">
                    <div className="detail-analysis-prob-meter-label">
                      中立確率 {analysisSummaryLoading ? "暫定" : formatPercentLabel(analysisDecision.neutralProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.neutralWidth}%` }} />
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <details className="detail-analysis-details">
              <summary className="detail-analysis-details-summary">追加情報</summary>
              <div className="detail-analysis-details-body">
                <div className="detail-analysis-details-actions">
                  <button
                    type="button"
                    className="nav-btn"
                    disabled={
                      (!analysisRecalcDisabled && analysisAsOfTime == null) ||
                      analysisBackfillActive ||
                      analysisRecalcSubmitting != null
                    }
                    title={
                      analysisRecalcDisabled
                        ? "現在の基準日で売買判定を更新"
                        : analysisRecalcDisabledReason ?? undefined
                    }
                    onClick={() => {
                      void submitAnalysisRecalc();
                    }}
                  >
                    {analysisRecalcDisabled ? "売買判定を更新" : "基準日を中心に130本を再計算"}
                  </button>
                </div>
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">詳細</div>
                  {analysisCursorDateLabel && cursorMode && (
                    <div className="detail-analysis-meta">カーソル日 {analysisCursorDateLabel}</div>
                  )}
                  {canShowPhase && phaseReasons[0] && (
                    <div className="detail-analysis-meta">局面メモ {phaseReasons[0]}</div>
                  )}
                  {analysisPreparationVisible && analysisBackfillProgressLabel && (
                    <div className="detail-analysis-meta">{analysisBackfillProgressLabel}</div>
                  )}
                  {analysisPreparationVisible &&
                    analysisBackfillMessage &&
                    analysisBackfillMessage !== analysisBackfillProgressLabel && (
                      <div className="detail-analysis-meta">{analysisBackfillMessage}</div>
                    )}
                  {sellAnalysisDtLabel && <div className="detail-analysis-meta">売り基準日 {sellAnalysisDtLabel}</div>}
                  {sellPredDtLabel && <div className="detail-analysis-meta">予測スナップショット {sellPredDtLabel}</div>}
                  {researchPriorRunId && <div className="detail-analysis-meta">研究連携 Run {researchPriorRunId}</div>}
                  {analysisResearchPrior && <div className="detail-analysis-meta">{researchPriorUpMeta}</div>}
                  {analysisResearchPrior && <div className="detail-analysis-meta">{researchPriorDownMeta}</div>}
                  {edinetStatusMeta && <div className="detail-analysis-meta">{edinetStatusMeta}</div>}
                  {edinetQualityMeta && <div className="detail-analysis-meta">EDI品質 {edinetQualityMeta}</div>}
                  {edinetMetricsMeta && <div className="detail-analysis-meta">{edinetMetricsMeta}</div>}
                  {edinetBonusMeta && <div className="detail-analysis-meta">{edinetBonusMeta}</div>}
                </div>
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">仕込み指標</div>
                  <div className="detail-analysis-prob-meter-list">
                    <div className="detail-analysis-prob-meter-row tone-up">
                      <div className="detail-analysis-prob-meter-label">
                        買い仕込み {analysisSummaryLoading ? "暫定" : `${analysisGuidance.buySetupState} ${formatPercentLabel(analysisGuidance.buySetupProb)}`}
                      </div>
                      <div className="detail-analysis-prob-meter-track">
                        <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buySetupWidth}%` }} />
                      </div>
                    </div>
                    <div className="detail-analysis-prob-meter-row tone-down">
                      <div className="detail-analysis-prob-meter-label">
                        売り仕込み {analysisSummaryLoading ? "暫定" : `${analysisGuidance.sellSetupState} ${formatPercentLabel(analysisGuidance.sellSetupProb)}`}
                      </div>
                      <div className="detail-analysis-prob-meter-track">
                        <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellSetupWidth}%` }} />
                      </div>
                    </div>
                  </div>
                </div>
                {hasSwingData && (
                  <div className="detail-analysis-section">
                    <div className="detail-analysis-section-title">追加情報</div>
                    {swingPlan ? (
                      <>
                        <div className="detail-analysis-meta">
                          提案 {swingSideLabel} / Score {formatPercentLabel(swingPlan.score)} / Horizon {formatNumber(swingPlan.horizonDays, 0)}日
                        </div>
                        <div className="detail-analysis-meta">
                          Entry {formatNumber(swingPlan.entry, 2)} / Stop {formatNumber(swingPlan.stop, 2)}
                        </div>
                        <div className="detail-analysis-meta">
                          TP1 {formatNumber(swingPlan.tp1, 2)} / TP2 {formatNumber(swingPlan.tp2, 2)} / TimeStop {formatNumber(swingPlan.timeStopDays, 0)}日
                        </div>
                        {swingReasonsLabel && <div className="detail-analysis-meta">理由 {swingReasonsLabel}</div>}
                      </>
                    ) : (
                      <div className="detail-analysis-meta">現在条件では swing 提案なし</div>
                    )}
                    {swingDiagnostics && (
                      <div className="detail-analysis-meta">
                        Edge {formatPercentLabel(swingDiagnostics.edge)} / Risk {formatPercentLabel(swingDiagnostics.risk)} / RegimeFit {formatPercentLabel(swingDiagnostics.regimeFit)}
                      </div>
                    )}
                    {swingDiagnostics && (
                      <div className="detail-analysis-meta">
                        ATR {formatPercentLabel(swingDiagnostics.atrPct)} / 流動性20d {formatNumber(swingDiagnostics.liquidity20d, 0)}
                      </div>
                    )}
                    {swingSetupExpectancy && (
                      <div className="detail-analysis-meta">
                        Setup {swingSetupExpectancy.setupType ?? "--"} / n {formatNumber(swingSetupExpectancy.samples, 0)} / 勝率 {formatPercentLabel(swingSetupExpectancy.winRate)} / 平均 {formatSignedPercentLabel(swingSetupExpectancy.meanRet)} / 縮小平均 {formatSignedPercentLabel(swingSetupExpectancy.shrunkMeanRet)}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </details>
          </>
        ) : (
          <>
            <div className="detail-analysis-empty">分析データ未計算</div>
            <div className="detail-analysis-actions detail-analysis-actions--bottom">
              <button
                type="button"
                className="nav-btn"
                disabled={
                  (!analysisRecalcDisabled && analysisAsOfTime == null) ||
                  analysisBackfillActive ||
                  analysisRecalcSubmitting != null
                }
                title={
                  analysisRecalcDisabled
                    ? "現在の基準日で売買判定を更新"
                    : analysisRecalcDisabledReason ?? undefined
                }
                onClick={() => {
                  void submitAnalysisRecalc();
                }}
              >
                {analysisRecalcDisabled ? "売買判定を更新" : "基準日を中心に130本を再計算"}
              </button>
            </div>
          </>
        )}
      </div>
    </ScreenPanel>
  );
}
