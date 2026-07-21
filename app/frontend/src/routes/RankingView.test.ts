import { describe, expect, it } from "vitest";

import { resolveIntegratedEntryAnnotation, resolveTradeActionChip } from "./RankingView";

describe("resolveTradeActionChip", () => {
  it("labels high-zone chart read states without promoting them to buy entries", () => {
    expect(
      resolveTradeActionChip(
        {
          code: "4101",
          tradeReviewBucket: "high_zone_chart",
          highZoneChartState: "trend_follow"
        },
        false
      )
    ).toMatchObject({ label: "高値追随可", className: "signal-chip entry-tier tier-b" });

    expect(
      resolveTradeActionChip(
        {
          code: "4102",
          tradeReviewBucket: "high_zone_chart",
          highZoneChartState: "wait_for_pullback"
        },
        false
      )
    ).toMatchObject({ label: "押し待ち", className: "signal-chip warning" });

    expect(
      resolveTradeActionChip(
        {
          code: "4103",
          tradeReviewBucket: "high_zone_chart",
          highZoneChartState: "high_grab_risk"
        },
        false
      )
    ).toMatchObject({ label: "高値づかみ", className: "signal-chip no-entry" });

    expect(
      resolveTradeActionChip(
        {
          code: "4104",
          tradeReviewBucket: "high_zone_chart",
          highZoneChartState: "research_needed",
          highZoneEvidenceResearchRequired: true
        },
        false
      )
    ).toMatchObject({ label: "研究待ち", className: "signal-chip warning" });
  });
});

describe("resolveIntegratedEntryAnnotation", () => {
  it("shows an actionable buy as display-only current-regime guidance", () => {
    expect(resolveIntegratedEntryAnnotation({
      code: "3479",
      integratedEntryState: "official",
      integratedEntrySide: "buy",
      integratedEntryActionable: true,
      integratedEntryDisplayPriority: 1,
      integratedEntryReason: "上昇ルール合致",
      integratedEntrySevereLossProbability: 0.08,
      integratedEntryDisplayOnly: true,
      integratedEntryCurrentRegimeOnly: true,
    })).toEqual({
      header: "買い候補",
      isWatch: false,
      priority: "表示優先 #1",
      context: "現行相場のみ・表示補助",
      reason: "理由: 上昇ルール合致",
      avoidReason: null,
      severeLoss: "大損確率 8.0%",
    });
  });

  it("labels a non-top3 short watch row as explicitly skipped", () => {
    expect(resolveIntegratedEntryAnnotation({
      code: "2433",
      integratedEntryState: "watch",
      integratedEntrySide: "sell",
      integratedEntryActionable: false,
      integratedEntryDisplayPriority: 4,
      integratedEntryAvoidReason: "上位3件外",
      integratedEntryDisplayOnly: true,
    })).toMatchObject({
      header: "空売り候補・見送り",
      isWatch: true,
      priority: "表示優先 #4",
      context: "現行相場のみ・表示補助",
      avoidReason: "見送り理由: 上位3件外",
    });
  });
});
