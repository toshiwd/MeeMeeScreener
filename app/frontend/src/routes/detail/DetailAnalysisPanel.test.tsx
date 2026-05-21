import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  DetailAnalysisPanel,
  type Props as DetailAnalysisPanelProps,
} from "./DetailAnalysisPanel";
import type {
  AnalysisEntryPolicy,
  AnalysisEntryPolicySide,
  AnalysisResearchPrior,
  EnvironmentSummary,
} from "./detailTypes";

const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtSignedPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null ? "--" : value.toFixed(digits);

const buildPolicySide = (setupType: string, holdDays: number, reason: string): AnalysisEntryPolicySide => ({
  setupType,
  recommendedHoldDays: holdDays,
  recommendedHoldMinDays: null,
  recommendedHoldMaxDays: null,
  recommendedHoldReason: reason,
  invalidationTrigger: null,
  invalidationConservativeAction: null,
  invalidationAggressiveAction: null,
  invalidationRecommendedAction: null,
  invalidationDotenRecommended: false,
  invalidationOppositeHoldDays: null,
  invalidationExpectedDeltaMean: null,
  invalidationPolicyNote: null,
  playbookScoreBonus: null,
});

const baseProps = {
  analysisAsOfTime: 1773878400,
  analysisBackfillActive: false,
  analysisRecalcSubmitting: null,
  analysisRecalcDisabled: false,
  analysisRecalcDisabledReason: null,
  submitAnalysisRecalc: async () => {},
  analysisDtLabel: "2026-03-19",
  cursorMode: true,
  analysisCursorDateLabel: "2026-03-19",
  canShowPhase: true,
  phaseReasons: ["phase=trend"],
  canShowAnalysis: true,
  analysisDecision: {
    tone: "up",
    sideLabel: "buy",
    buyProb: 0.64,
    sellProb: 0.21,
    neutralProb: 0.15,
  },
  analysisSummaryLoading: false,
  analysisGuidance: {
    confidenceRank: "A",
    action: "watch",
    watchpoint: "watch",
    buyWidth: 64,
    sellWidth: 21,
    neutralWidth: 15,
    buySetupProb: 0.51,
    sellSetupProb: 0.27,
    buySetupWidth: 51,
    sellSetupWidth: 27,
    buySetupState: "watch",
    sellSetupState: "wait",
  },
  analysisEntryPolicy: {
    riskMode: "balanced",
    up: buildPolicySide("breakout", 5, "x"),
    down: buildPolicySide("breakdown", 3, "y"),
  } satisfies AnalysisEntryPolicy,
  patternSummary: {
    environmentLabel: "strong",
    environmentTone: "neutral",
    markerTone: null,
    markerIsSetup: false,
    scenarios: [],
  } satisfies EnvironmentSummary,
  analysisPreparationVisible: false,
  analysisBackfillProgressLabel: null,
  analysisBackfillMessage: null,
  sellAnalysisDtLabel: "2026-03-18",
  sellPredDtLabel: "2026-03-20",
  researchPriorRunId: "run-1",
  analysisResearchPrior: {
    runId: "run-1",
    up: {
      aligned: true,
      rank: 1,
      universe: 5,
      bonus: 0.018,
      asOf: "2026-03-28",
      patternTag: "rebound_onset",
      fitScore: 0.8,
      adoptionReasons: ["reason-a", "reason-b"],
    },
    down: null,
  } satisfies AnalysisResearchPrior,
  researchPriorUpMeta: "up-meta",
  researchPriorDownMeta: "down-meta",
  edinetStatusMeta: "EDINET ok",
  edinetQualityMeta: "good",
  edinetMetricsMeta: "metrics",
  edinetBonusMeta: "bonus",
  hasSwingData: true,
  swingPlan: null,
  swingSideLabel: "--",
  swingReasonsLabel: "",
  swingDiagnostics: null,
  swingSetupExpectancy: null,
  analysisMissingDataVisible: true,
  decisionHistory: [],
  individualResult: null,
  qualificationTrace: null,
  formatPercentLabel: fmtPercent,
  formatNumber: fmtNumber,
  formatSignedPercentLabel: fmtSignedPercent,
  onSubmitAnalysisRecalc: () => {},
} satisfies DetailAnalysisPanelProps;

describe("DetailAnalysisPanel", () => {
  it("keeps the empty state compact", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        canShowAnalysis={false}
        analysisPreparationVisible={false}
      />
    );

    expect(markup).toContain("read only");
    expect(markup).not.toContain("detail-analysis-regime");
  });

  it("keeps the main analysis view summary-first and moves extras into details", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        analysisGuidance={{
          ...baseProps.analysisGuidance,
          watchpoint: "watch",
        }}
        analysisPreparationVisible={true}
        analysisBackfillProgressLabel="preparing"
        analysisBackfillMessage="preparing more"
      />
    );

    expect(markup).toContain("detail-analysis-regime--up");
    expect(markup).toContain("detail-analysis-entry-plan--up");
    expect(markup).toContain("detail-analysis-entry-plan--down");
    expect(markup).toContain("detail-analysis-details");
  });

  it("shows rebound research metadata and promotes rebound only for watch-like buy setup", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        analysisEntryPolicy={{
          ...baseProps.analysisEntryPolicy,
          up: {
            ...baseProps.analysisEntryPolicy.up!,
            setupType: "watch",
          },
        }}
      />
    );

    expect(markup).toContain("detail-analysis-entry-plan--up");
    expect(markup).toContain("detail-analysis-details");
    expect(markup).toContain("reason-a");
  });

  it("shows individual ranking result details when available", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        qualificationTrace={{
          todayState: "sell",
          lastBuyDateIso: "2026-03-03",
          lastSellDateIso: "2026-03-31",
        }}
        individualResult={{
          setupType: "breakout",
          monthlyBoxState: "box_upper",
          tradePriorityScore: 0.712,
          entryPriorityScore: 0.664,
          hybridScore: 0.551,
          entryQualified: true,
          entryQualifiedByFallback: false,
          researchPatternTag: "upper_rejection_short",
          tradeDecisionReasons: ["reason-1", "reason-2"],
          tradeRiskWatch: ["risk-1"],
          researchDecisionReasons: ["reason-3", "reason-4"],
          researchRiskWatch: ["risk-2"],
        }}
      />
    );

    expect(markup).toContain("tradePriorityScore 0.712");
    expect(markup).toContain("2026/03/03 (火)");
    expect(markup).toContain("2026/03/31 (火)");
    expect(markup).toContain("upper_rejection_short");
  });

  it("shows the new persisted signal analysis summary", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        persistedSignalEvents={[
          {
            signalDate: "2026-03-18",
            side: "buy",
            setup_type: "breakout",
            current_directional_return: 0.012,
            return_30d: 0.031,
            break_status: "broken",
            break_reason: "stop5",
          },
          {
            signalDate: "2026-03-19",
            side: "buy",
            setup_type: "breakout",
            current_directional_return: 0.006,
            return_30d: 0.018,
            break_status: "completed_clean",
            break_reason: null,
          },
          {
            signalDate: "2026-03-20",
            side: "sell",
            setup_type: "breakdown",
            current_directional_return: -0.022,
            return_30d: -0.054,
            break_status: "broken",
            break_reason: "box_reclaim",
          },
        ]}
      />
    );

    expect(markup).toContain("新しい分析");
    expect(markup).toContain("買い");
    expect(markup).toContain("売り");
    expect(markup).toContain("stop5");
    expect(markup).toContain("box_reclaim");
  });

});
