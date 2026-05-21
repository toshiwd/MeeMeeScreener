import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { DetailJudgementPanel } from "./DetailJudgementPanel";
import type { AnalysisEntryPolicy, AnalysisResearchPrior, EnvironmentSummary } from "./detailTypes";

const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtSignedPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtNumber = (value: number | null | undefined, digits = 2) =>
  value == null ? "--" : value.toFixed(digits);

const baseProps = {
  analysisDtLabel: "2026-03-19",
  analysisSummaryLoading: false,
  analysisMissingDataVisible: true,
  analysisDecision: {
    tone: "up",
    sideLabel: "買い優先",
    patternLabel: "rank 1 / score 64.0%",
    confidence: 0.72,
    buyProb: 0.64,
    sellProb: 0.21,
    neutralProb: 0.15,
  },
  analysisGuidance: {
    confidenceRank: "A",
    action: "買い継続監視",
    watchpoint: "rank 1 / score 64.0% / breakout / 監視",
    buyWidth: 64,
    sellWidth: 21,
    neutralWidth: 15,
    buySetupProb: 0.64,
    sellSetupProb: 0.21,
    buySetupWidth: 64,
    sellSetupWidth: 21,
    buySetupState: "監視",
    sellSetupState: "待機",
  },
  analysisEntryPolicy: {
    riskMode: "balanced",
    up: {
      setupType: "breakout",
      recommendedHoldDays: null,
      recommendedHoldMinDays: null,
      recommendedHoldMaxDays: null,
      recommendedHoldReason: "優先度 64.0% / rank 1 / score 64.0%",
      invalidationTrigger: "stop5",
      invalidationConservativeAction: "exit",
      invalidationAggressiveAction: "hold",
      invalidationRecommendedAction: "exit",
      invalidationDotenRecommended: false,
      invalidationOppositeHoldDays: null,
      invalidationExpectedDeltaMean: null,
      invalidationPolicyNote: null,
      playbookScoreBonus: null,
    },
    down: {
      setupType: "breakdown",
      recommendedHoldDays: null,
      recommendedHoldMinDays: null,
      recommendedHoldMaxDays: null,
      recommendedHoldReason: "優先度 21.0% / rank 4 / score 21.0%",
      invalidationTrigger: "box_reclaim",
      invalidationConservativeAction: "exit",
      invalidationAggressiveAction: "hold",
      invalidationRecommendedAction: "exit",
      invalidationDotenRecommended: true,
      invalidationOppositeHoldDays: 2,
      invalidationExpectedDeltaMean: null,
      invalidationPolicyNote: null,
      playbookScoreBonus: null,
    },
  } satisfies AnalysisEntryPolicy,
  patternSummary: {
    environmentLabel: "ランキング 2026-03-19",
    environmentTone: "up",
    markerTone: "up",
    markerIsSetup: false,
    scenarios: [
      { key: "up", label: "買い優先", tone: "up", score: 0.72, reasons: ["rank 1", "breakout"] },
      { key: "range", label: "中立", tone: "neutral", score: 0.16, reasons: ["wait"] },
      { key: "down", label: "売り優先", tone: "down", score: 0.12, reasons: ["risk"] },
    ],
  } satisfies EnvironmentSummary,
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
    down: {
      aligned: false,
      rank: 4,
      universe: 5,
      bonus: -0.004,
      asOf: "2026-03-28",
      patternTag: "risk_off",
      fitScore: 0.3,
      adoptionReasons: ["reason-x"],
    },
  } satisfies AnalysisResearchPrior,
  formatPercentLabel: fmtPercent,
  formatNumber: fmtNumber,
  formatSignedPercentLabel: fmtSignedPercent,
};

describe("DetailJudgementPanel", () => {
  it("shows ranking-based priority and hold guidance", () => {
    const markup = renderToStaticMarkup(<DetailJudgementPanel {...baseProps} />);

    expect(markup).toContain("判定サマリー");
    expect(markup).toContain("選択日のランキング appearance を基準にした判定サマリー");
    expect(markup).toContain("買い / 中立 / 売りの優先度");
    expect(markup).toContain("継続保有 / 仕込み");
    expect(markup).toContain("判定要素");
    expect(markup).toContain("保有取引実績");
    expect(markup).toContain("買い優先");
    expect(markup).toContain("売り優先");
  });

  it("shows an explicit no-judgement state when ranking appearance is missing", () => {
    const markup = renderToStaticMarkup(
      <DetailJudgementPanel
        {...baseProps}
        analysisMissingDataVisible={true}
        analysisDecision={{
          tone: "neutral",
          sideLabel: null,
          patternLabel: null,
          confidence: null,
          buyProb: null,
          sellProb: null,
          neutralProb: null,
        }}
        analysisGuidance={{
          ...baseProps.analysisGuidance,
          confidenceRank: "low",
          action: "neutral_watch",
          watchpoint: "no ranking appearance data",
          buyWidth: 0,
          sellWidth: 0,
          neutralWidth: 100,
        }}
        patternSummary={{
          ...baseProps.patternSummary,
          environmentLabel: "ranking",
          environmentTone: "neutral",
          markerTone: "neutral",
          scenarios: [],
        }}
        analysisEntryPolicy={{
          riskMode: "balanced",
          up: null,
          down: null,
        }}
      />
    );

    expect(markup).toContain("判定不可");
    expect(markup).toContain("買い/売り判定に使えるランキング根拠なし");
    expect(markup).toContain("買い・売り候補なし");
    expect(markup).not.toContain("暫定");
  });
  it("shows daily chart shape as display-only context", () => {
    const markup = renderToStaticMarkup(
      <DetailJudgementPanel
        {...baseProps}
        dailyChartShape={{
          confirmed: true,
          shape_label: "gap_up_stall_fade",
          shape_family: "gap_failure",
          bias: "caution",
          actionability: "avoid_chase",
          description: "large gap up, then fade",
          confidence: 0.82,
          window: 10,
          reasons: ["large_gap_up", "failed_to_extend_after_gap"],
          metrics: {
            gap_pct: 15.2,
            post_gap_return_pct: -8.1,
          },
          multi_window: {
            event_shape: {
              shape_label: "gap_up_stall_fade",
              shape_family: "gap_failure",
              bias: "caution",
              actionability: "avoid_chase",
              window: 10,
            },
            context_shape: {
              shape_label: "sideways_range",
              shape_family: "range",
              bias: "neutral",
              actionability: "wait",
              window: 20,
            },
            trend_shape: {
              shape_label: "steady_uptrend",
              shape_family: "trend",
              bias: "bullish_watch",
              actionability: "watch",
              window: 60,
            },
            conflict_flags: ["short_term_caution_but_context_bullish"],
          },
        }}
      />
    );

    expect(markup).toContain("GU後に失速");
    expect(markup).toContain("短期 10本");
    expect(markup).toContain("中期 20本");
    expect(markup).toContain("流れ 60本");
    expect(markup).toContain("横ばいレンジ");
    expect(markup).toContain("安定上昇");
    expect(markup).toContain("追いかけない");
    expect(markup).toContain("表示用の日足形状");
    expect(markup).toContain("TRADEX研究結果は変更しません");
  });
});
