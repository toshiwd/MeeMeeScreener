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
import { formatIsoDateLabel } from "../../utils/dateLabels";

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

type DecisionHistoryItem = {
  dtKey: number;
  tone: "up" | "down" | "neutral";
};

type IndividualResult = {
  setupType?: string | null;
  monthlyBoxState?: string | null;
  tradePriorityScore?: number | null;
  entryPriorityScore?: number | null;
  hybridScore?: number | null;
  entryQualified?: boolean | null;
  entryQualifiedByFallback?: boolean | null;
  entryQualifiedFallbackStage?: string | null;
  researchPatternTag?: string | null;
  researchDecisionReasons?: string[] | null;
  researchRiskWatch?: string[] | null;
  tradeDecisionReasons?: string[] | null;
  tradeRiskWatch?: string[] | null;
};

type QualificationTraceSummary = {
  todayState?: "buy" | "sell" | "wait" | "both" | null;
  lastBuyDateIso?: string | null;
  lastSellDateIso?: string | null;
};

type PersistedSignalEventItem = {
  signalDate?: string | null;
  side?: "buy" | "sell" | null;
  setup_type?: string | null;
  return_30d?: number | null;
  current_directional_return?: number | null;
  break_status?: string | null;
  break_reason?: string | null;
};

type RankingAppearanceItem = {
  date_iso?: string | null;
  dir?: "up" | "down" | null;
  rank?: number | null;
  break_status?: string | null;
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
  decisionHistory: DecisionHistoryItem[];
  individualResult: IndividualResult | null;
  qualificationTrace: QualificationTraceSummary | null;
  persistedSignalEvents?: PersistedSignalEventItem[];
  rankingAppearancesOnSelectedDate?: RankingAppearanceItem[];
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

const resolveEffectiveBuySetupType = (
  rawSetupType: string | null | undefined,
  researchPriorSide: AnalysisResearchPrior["up"] | null | undefined
) => {
  const normalized = rawSetupType ?? null;
  if (normalized && normalized !== "watch" && normalized !== "watchlist") return normalized;
  return researchPriorSide?.patternTag === "rebound_onset" ? "rebound" : normalized;
};

const formatDecisionToneLabel = (tone: DecisionHistoryItem["tone"]) => {
  if (tone === "up") return "買い";
  if (tone === "down") return "売り";
  return "中立";
};

const formatDecisionDateLabel = (dtKey: number) => {
  const value = String(Math.trunc(dtKey)).padStart(8, "0");
  if (!/^\d{8}$/.test(value)) return String(dtKey);
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
};

const formatTradeTodayState = (state?: "buy" | "sell" | "wait" | "both" | null) => {
  if (state === "buy") return "買い";
  if (state === "sell") return "売り";
  if (state === "both") return "買い/売り両方";
  return "見送り";
};

type PersistedSignalEventSummary = {
  side: "buy" | "sell";
  total: number;
  broken: number;
  clean: number;
  active: number;
  avgCurrentDirectionalReturn: number | null;
  avgReturn30d: number | null;
  topBreakReasons: Array<{ reason: string; count: number }>;
};

const summarizePersistedSignalEvents = (items: PersistedSignalEventItem[]): PersistedSignalEventSummary[] => {
  const grouped: Record<"buy" | "sell", PersistedSignalEventItem[]> = { buy: [], sell: [] };
  for (const item of items) {
    grouped[item.side === "sell" ? "sell" : "buy"].push(item);
  }

  return (["buy", "sell"] as const).map((side) => {
    const subset = grouped[side];
    const brokenItems = subset.filter((item) => item.break_status === "broken");
    const cleanItems = subset.filter((item) => item.break_status === "completed_clean");
    const activeCount = subset.length - brokenItems.length - cleanItems.length;

    const average = (selector: (item: PersistedSignalEventItem) => number | null | undefined) => {
      const values = subset.map(selector).filter((value): value is number => Number.isFinite(value));
      if (values.length === 0) return null;
      return values.reduce((sum, value) => sum + value, 0) / values.length;
    };

    const breakReasonCounts = new Map<string, number>();
    for (const item of brokenItems) {
      const reason = item.break_reason?.trim();
      if (!reason) continue;
      breakReasonCounts.set(reason, (breakReasonCounts.get(reason) ?? 0) + 1);
    }

    const topBreakReasons = [...breakReasonCounts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "ja"))
      .slice(0, 3)
      .map(([reason, count]) => ({ reason, count }));

    return {
      side,
      total: subset.length,
      broken: brokenItems.length,
      clean: cleanItems.length,
      active: activeCount,
      avgCurrentDirectionalReturn: average((item) => item.current_directional_return),
      avgReturn30d: average((item) => item.return_30d),
      topBreakReasons,
    };
  });
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
    decisionHistory,
    individualResult,
    qualificationTrace,
    persistedSignalEvents = [],
    rankingAppearancesOnSelectedDate = [],
    formatPercentLabel,
    formatNumber,
    formatSignedPercentLabel,
  } = props;
  const buyPolicy = analysisEntryPolicy?.up ?? null;
  const sellPolicy = analysisEntryPolicy?.down ?? null;
  const researchPriorUp = analysisResearchPrior?.up ?? null;
  const effectiveBuySetupType = resolveEffectiveBuySetupType(buyPolicy?.setupType, researchPriorUp);
  const persistedSignalEventSummaries = summarizePersistedSignalEvents(persistedSignalEvents);

  return (
    <ScreenPanel title="従来判定" className="detail-analysis-panel">
      <div className="detail-analysis-body">
        <div className="detail-analysis-meta">日々の売買判定 / read only</div>
        {analysisDtLabel && <div className="detail-analysis-meta">基準日 {analysisDtLabel}</div>}
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">新しい分析</div>
                  {persistedSignalEventSummaries.some((item) => item.total > 0) ? (
                    persistedSignalEventSummaries.map((summary) => (
                      <div key={summary.side} className="detail-analysis-meta">
                        {summary.side === "buy" ? "買い" : "売り"} / n {formatNumber(summary.total, 0)} / broken {formatNumber(summary.broken, 0)} / clean {formatNumber(summary.clean, 0)} / active {formatNumber(summary.active, 0)} / 現在 {formatSignedPercentLabel(summary.avgCurrentDirectionalReturn)} / 30日 {formatSignedPercentLabel(summary.avgReturn30d)}
                        {summary.topBreakReasons.length > 0 && (
                          <div className="detail-analysis-meta">
                            主因 {summary.topBreakReasons.map((reason) => `${reason.reason} x${reason.count}`).join(" / ")}
                          </div>
                        )}
                      </div>
                    ))
                  ) : (
                    <div className="detail-analysis-meta">新しい分析データなし</div>
                  )}
                </div>
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
                    狙い {formatSetupIntent("buy", effectiveBuySetupType)}
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
                  {researchPriorUp?.patternTag === "rebound_onset" && (
                    <div className="detail-analysis-meta">研究タグ 反発初動候補</div>
                  )}
                  {Number.isFinite(researchPriorUp?.fitScore ?? NaN) && (
                    <div className="detail-analysis-meta">適合度 {formatPercentLabel(researchPriorUp?.fitScore, 0)}</div>
                  )}
                  {Array.isArray(researchPriorUp?.adoptionReasons) && researchPriorUp.adoptionReasons.length > 0 && (
                    <div className="detail-analysis-meta">採用理由 {researchPriorUp.adoptionReasons.join(" / ")}</div>
                  )}
                  {edinetStatusMeta && <div className="detail-analysis-meta">{edinetStatusMeta}</div>}
                  {edinetQualityMeta && <div className="detail-analysis-meta">EDI品質 {edinetQualityMeta}</div>}
                  {edinetMetricsMeta && <div className="detail-analysis-meta">{edinetMetricsMeta}</div>}
                  {edinetBonusMeta && <div className="detail-analysis-meta">{edinetBonusMeta}</div>}
                </div>
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">persisted 判定</div>
                  {persistedSignalEvents.length > 0 ? (
                    persistedSignalEvents.map((item, index) => (
                      <div key={`${item.signalDate ?? "na"}-${item.side ?? index}`} className="detail-analysis-meta">
                        {formatIsoDateLabel(item.signalDate)} / {item.side === "sell" ? "売り" : "買い"} / {item.setup_type ?? "--"} / 現在 {formatSignedPercentLabel(item.current_directional_return)} / 30日 {formatSignedPercentLabel(item.return_30d)} / {item.break_status === "broken" ? `崩れ ${item.break_reason ?? ""}` : item.break_status === "completed_clean" ? "完走" : "継続中"}
                      </div>
                    ))
                  ) : (
                    <div className="detail-analysis-meta">persisted 判定なし</div>
                  )}
                  {rankingAppearancesOnSelectedDate.length > 0 && (
                    <div className="detail-analysis-meta">
                      ランキング掲載{" "}
                      {rankingAppearancesOnSelectedDate
                        .map((item) => `${item.dir === "down" ? "下落側" : "上昇側"} ${item.rank ?? "--"}位`)
                        .join(" / ")}
                    </div>
                  )}
                </div>
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">個別結果</div>
                  <div className="detail-analysis-meta">今日の判断 {formatTradeTodayState(qualificationTrace?.todayState)}</div>
                  <div className="detail-analysis-meta">前回買い厳選通過 {formatIsoDateLabel(qualificationTrace?.lastBuyDateIso)}</div>
                  <div className="detail-analysis-meta">前回売り厳選通過 {formatIsoDateLabel(qualificationTrace?.lastSellDateIso)}</div>
                  <div className="detail-analysis-meta">
                    厳選通過{" "}
                    {individualResult?.entryQualified === true
                      ? "通過"
                      : individualResult?.entryQualified === false
                        ? "未通過"
                        : "--"}
                  </div>
                  <div className="detail-analysis-meta">セットアップ {individualResult?.setupType ?? "--"}</div>
                  <div className="detail-analysis-meta">月足ボックス {individualResult?.monthlyBoxState ?? "--"}</div>
                  <div className="detail-analysis-meta">
                    tradePriorityScore {formatNumber(individualResult?.tradePriorityScore ?? null, 3)}
                  </div>
                  <div className="detail-analysis-meta">
                    entryPriorityScore {formatNumber(individualResult?.entryPriorityScore ?? null, 3)}
                  </div>
                  <div className="detail-analysis-meta">
                    hybridScore {formatNumber(individualResult?.hybridScore ?? null, 3)}
                  </div>
                  {individualResult?.entryQualifiedByFallback === true && (
                    <div className="detail-analysis-meta">
                      補完通過 {individualResult?.entryQualifiedFallbackStage ?? "--"}
                    </div>
                  )}
                  {individualResult?.researchPatternTag && (
                    <div className="detail-analysis-meta">研究タグ {individualResult.researchPatternTag}</div>
                  )}
                  {Array.isArray(
                    individualResult?.tradeDecisionReasons?.length
                      ? individualResult.tradeDecisionReasons
                      : individualResult?.researchDecisionReasons
                  ) &&
                    (individualResult?.tradeDecisionReasons?.length || individualResult?.researchDecisionReasons?.length) ? (
                      <div className="detail-analysis-meta">
                        判定理由{" "}
                        {(individualResult?.tradeDecisionReasons?.length
                          ? individualResult.tradeDecisionReasons
                          : individualResult?.researchDecisionReasons ?? []
                        ).join(" / ")}
                      </div>
                    ) : null}
                  {Array.isArray(
                    individualResult?.tradeRiskWatch?.length
                      ? individualResult.tradeRiskWatch
                      : individualResult?.researchRiskWatch
                  ) &&
                    (individualResult?.tradeRiskWatch?.length || individualResult?.researchRiskWatch?.length) ? (
                      <div className="detail-analysis-meta">
                        注意点{" "}
                        {(individualResult?.tradeRiskWatch?.length
                          ? individualResult.tradeRiskWatch
                          : individualResult?.researchRiskWatch ?? []
                        ).join(" / ")}
                      </div>
                    ) : null}
                </div>
                <div className="detail-analysis-section">
                  <div className="detail-analysis-section-title">判定履歴</div>
                  {decisionHistory.length > 0 ? (
                    <>
                      <div className="detail-analysis-meta">表示中レンジ内の直近12件</div>
                      {decisionHistory
                        .slice()
                        .reverse()
                        .slice(0, 12)
                        .map((item) => (
                          <div key={`${item.dtKey}-${item.tone}`} className="detail-analysis-meta">
                            {formatDecisionDateLabel(item.dtKey)} / {formatDecisionToneLabel(item.tone)}
                          </div>
                        ))}
                    </>
                  ) : (
                    <div className="detail-analysis-meta">判定履歴なし</div>
                  )}
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
