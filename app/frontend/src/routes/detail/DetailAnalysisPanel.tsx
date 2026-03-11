import type {
  AnalysisResearchPrior,
  AnalysisSwingDiagnostics,
  AnalysisSwingPlan,
  AnalysisSwingSetupExpectancy,
  EnvironmentSummary,
} from "./detailTypes";

type AnalysisDecisionSummary = {
  tone: "up" | "down" | "neutral";
  sideLabel: string | null;
  buyProb: number | null;
  sellProb: number | null;
  neutralProb: number | null;
};

type AnalysisGuidance = {
  confidenceRank: string;
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
  analysisRecalcSubmitting: "current" | "auto" | null;
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
};

export function DetailAnalysisPanel(props: Props) {
  const {
    analysisAsOfTime,
    analysisBackfillActive,
    analysisRecalcSubmitting,
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
  } = props;

  return (
    <div className="daily-memo-panel detail-analysis-panel">
      <div className="memo-panel-header">
        <h3>解析結果</h3>
      </div>
      <div className="detail-analysis-actions">
        <button
          type="button"
          className="nav-btn"
          disabled={analysisAsOfTime == null || analysisBackfillActive || analysisRecalcSubmitting != null}
          onClick={() => {
            void submitAnalysisRecalc();
          }}
        >
          基準日を中心に130本を再計算
        </button>
      </div>
      <div className="detail-analysis-body">
        {analysisDtLabel && (
          <div className="detail-analysis-meta">基準日 {analysisDtLabel}</div>
        )}
        {cursorMode && analysisCursorDateLabel && (
          <div className="detail-analysis-meta">カーソル日 {analysisCursorDateLabel}</div>
        )}
        {canShowPhase && phaseReasons[0] && (
          <div className="detail-analysis-meta">局面メモ {phaseReasons[0]}</div>
        )}
        {canShowAnalysis ? (
          <>
            <div className="detail-analysis-section">
              <div className="detail-analysis-section-title">売買サマリー</div>
              <div className={`detail-analysis-regime detail-analysis-regime--${analysisDecision.tone}`}>
                <div className="detail-analysis-call-head">
                  <span className={`detail-analysis-call-badge detail-analysis-call-badge--${analysisDecision.tone}`}>
                    判定 {analysisDecision.sideLabel}
                  </span>
                  <span className="detail-analysis-call-confidence">
                    確信度 {analysisSummaryLoading ? "読込中..." : analysisGuidance.confidenceRank}
                  </span>
                </div>
                <div className="detail-analysis-regime-title">{patternSummary.environmentLabel}</div>
                <div className="detail-analysis-prob-meter-list">
                  <div className="detail-analysis-prob-meter-row tone-up">
                    <div className="detail-analysis-prob-meter-label">
                      上昇確率 {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.buyProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buyWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-down">
                    <div className="detail-analysis-prob-meter-label">
                      下落確率 {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.sellProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-neutral">
                    <div className="detail-analysis-prob-meter-label">
                      中立確率 {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.neutralProb)}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.neutralWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-up">
                    <div className="detail-analysis-prob-meter-label">
                      買い仕込み {analysisSummaryLoading ? "読込中..." : `${analysisGuidance.buySetupState} ${formatPercentLabel(analysisGuidance.buySetupProb)}`}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buySetupWidth}%` }} />
                    </div>
                  </div>
                  <div className="detail-analysis-prob-meter-row tone-down">
                    <div className="detail-analysis-prob-meter-label">
                      売り仕込み {analysisSummaryLoading ? "読込中..." : `${analysisGuidance.sellSetupState} ${formatPercentLabel(analysisGuidance.sellSetupProb)}`}
                    </div>
                    <div className="detail-analysis-prob-meter-track">
                      <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellSetupWidth}%` }} />
                    </div>
                  </div>
                </div>
              </div>
              {analysisSummaryLoading && (
                <div className="detail-analysis-meta">一部データ読込中のため、確率は暫定表示です。</div>
              )}
              {analysisPreparationVisible && analysisBackfillProgressLabel && (
                <div className="detail-analysis-meta">{analysisBackfillProgressLabel}</div>
              )}
              {analysisPreparationVisible &&
                analysisBackfillMessage &&
                analysisBackfillMessage !== analysisBackfillProgressLabel && (
                  <div className="detail-analysis-meta">{analysisBackfillMessage}</div>
                )}
              {analysisDtLabel && (
                <div className="detail-analysis-meta">買い基準日 {analysisDtLabel}</div>
              )}
              {sellAnalysisDtLabel && (
                <div className="detail-analysis-meta">売り基準日 {sellAnalysisDtLabel}</div>
              )}
              {sellPredDtLabel && (
                <div className="detail-analysis-meta">予測スナップショット {sellPredDtLabel}</div>
              )}
              {researchPriorRunId && (
                <div className="detail-analysis-meta">研究連携 Run {researchPriorRunId}</div>
              )}
              {analysisResearchPrior && (
                <div className="detail-analysis-meta">{researchPriorUpMeta}</div>
              )}
              {analysisResearchPrior && (
                <div className="detail-analysis-meta">{researchPriorDownMeta}</div>
              )}
              {edinetStatusMeta && (
                <div className="detail-analysis-meta">{edinetStatusMeta}</div>
              )}
              {edinetQualityMeta && (
                <div className="detail-analysis-meta">EDI品質 {edinetQualityMeta}</div>
              )}
              {edinetMetricsMeta && (
                <div className="detail-analysis-meta">{edinetMetricsMeta}</div>
              )}
              {edinetBonusMeta && (
                <div className="detail-analysis-meta">{edinetBonusMeta}</div>
              )}
            </div>
            {hasSwingData && (
              <div className="detail-analysis-section">
                <div className="detail-analysis-section-title">Swing Plan (10-25営業日)</div>
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
                    {swingReasonsLabel && (
                      <div className="detail-analysis-meta">理由 {swingReasonsLabel}</div>
                    )}
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
          </>
        ) : (
          <>
            <div className="detail-analysis-empty">
              {analysisPreparationVisible
                ? "解析準備中です。"
                : analysisMissingDataVisible
                  ? "分析データが未計算のため、自動で準備しています。"
                  : "分析データがありません。"}
            </div>
            {analysisPreparationVisible && analysisBackfillProgressLabel && (
              <div className="detail-analysis-meta">{analysisBackfillProgressLabel}</div>
            )}
            {analysisPreparationVisible &&
              analysisBackfillMessage &&
              analysisBackfillMessage !== analysisBackfillProgressLabel && (
                <div className="detail-analysis-meta">{analysisBackfillMessage}</div>
              )}
            {analysisPreparationVisible && (
              <div className="detail-analysis-meta">準備完了後に自動で再取得します。</div>
            )}
            {analysisMissingDataVisible && (
              <div className="detail-analysis-meta">初回だけ基準日を中心に130本分の未計算を自動で計算します。保存済みデータがあれば再計算しません。</div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
