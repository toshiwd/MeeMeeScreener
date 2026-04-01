import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { DetailAnalysisPanel } from "./DetailAnalysisPanel";

const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtSignedPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null ? "--" : value.toFixed(digits);

const baseProps = {
  analysisAsOfTime: 1773878400,
  analysisBackfillActive: false,
  analysisRecalcSubmitting: null,
  analysisRecalcDisabled: false,
  analysisRecalcDisabledReason: null,
  submitAnalysisRecalc: async () => {},
  analysisDtLabel: "26/03/19",
  cursorMode: true,
  analysisCursorDateLabel: "26-03-19",
  canShowPhase: true,
  phaseReasons: ["phase=trend"],
  analysisDecision: {
    tone: "up",
    sideLabel: "買い",
    patternLabel: "breakout",
    environmentLabel: "strong",
    confidence: 0.72,
    buyProb: 0.64,
    sellProb: 0.21,
    neutralProb: 0.15,
    version: "v1",
    scenarios: [],
  },
  analysisSummaryLoading: true,
  analysisGuidance: {
    confidenceRank: "A",
    action: "watch",
    watchpoint: "watchpoint",
    buyWidth: 64,
    sellWidth: 21,
    neutralWidth: 15,
    buySetupProb: 0.51,
    sellSetupProb: 0.27,
    buySetupWidth: 51,
    sellSetupWidth: 27,
    buySetupState: "監視",
    sellSetupState: "待機",
  },
  analysisEntryPolicy: {
    up: { setupType: "breakout", recommendedHoldDays: 5, recommendedHoldReason: "x" },
    down: { setupType: "breakdown", recommendedHoldDays: 3, recommendedHoldReason: "y" },
  },
  patternSummary: { environmentLabel: "strong" },
  analysisPreparationVisible: false,
  analysisBackfillProgressLabel: null,
  analysisBackfillMessage: null,
  sellAnalysisDtLabel: "26/03/18",
  sellPredDtLabel: "26/03/20",
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
      adoptionReasons: ["120MA上", "陽線引け"],
    },
    down: null,
  } as never,
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
  formatPercentLabel: fmtPercent,
  formatNumber: fmtNumber,
  formatSignedPercentLabel: fmtSignedPercent,
  onSubmitAnalysisRecalc: () => {},
};

describe("DetailAnalysisPanel", () => {
  it("keeps the empty state compact", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        canShowAnalysis={false}
        analysisPreparationVisible={false}
      />
    );

    expect(markup).toContain("従来判定");
    expect(markup).toContain("日々の売買判定 / read only");
    expect(markup).toContain("基準日 26/03/19");
    expect(markup).toContain("分析データ未計算");
    expect(markup).toContain("基準日を中心に130本を再計算");
    expect(markup).not.toContain("更新すると分析を準備します。");
    expect(markup).not.toContain("初回だけ基準日を中心に130本分");
    expect(markup).not.toContain("準備ができたら更新してください。");
  });

  it("keeps the main analysis view summary-first and moves extras into details", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        canShowAnalysis={true}
        analysisSummaryLoading={false}
        decisionHistory={[
          { dtKey: 20260331, tone: "up" },
          { dtKey: 20260330, tone: "down" },
        ]}
        analysisGuidance={{
          ...baseProps.analysisGuidance,
          watchpoint: "watch",
        }}
        analysisPreparationVisible={true}
        analysisBackfillProgressLabel="準備中"
        analysisBackfillMessage="未計算を準備しています。"
      />
    );

    expect(markup).toContain("要約");
    expect(markup).toContain("判定 買い");
    expect(markup).toContain("確信度 A");
    expect(markup).toContain("狙い: breakout");
    expect(markup).toContain("地合い: strong");
    expect(markup).toContain("買い候補");
    expect(markup).toContain("売り候補");
    expect(markup).toContain("上昇確率");
    expect(markup).toContain("下落確率");
    expect(markup).toContain("中立確率");
    expect(markup).toContain("<details");
    expect(markup).toContain("判定履歴");
    expect(markup).toContain("2026-03-31 / 買い");
    expect(markup).toContain("2026-03-30 / 売り");
    expect(markup).toContain("追加情報");
    expect(markup).toContain("基準日を中心に130本を再計算");
  });

  it("shows rebound research metadata and promotes rebound only for watch-like buy setup", () => {
    const markup = renderToStaticMarkup(
      <DetailAnalysisPanel
        {...baseProps}
        canShowAnalysis={true}
        analysisSummaryLoading={false}
        analysisEntryPolicy={{
          up: { setupType: "watch", recommendedHoldDays: 5, recommendedHoldReason: "x" },
          down: { setupType: "breakdown", recommendedHoldDays: 3, recommendedHoldReason: "y" },
        } as never}
      />
    );

    expect(markup).toContain("研究タグ 反発初動候補");
    expect(markup).toContain("適合度 80%");
    expect(markup).toContain("採用理由 120MA上 / 陽線引け");
    expect(markup).toContain("反発初動狙い");
  });
});
