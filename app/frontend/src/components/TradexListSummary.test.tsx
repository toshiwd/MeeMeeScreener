import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import TradexListSummary from "./TradexListSummary";

describe("TradexListSummary", () => {
  it("renders a compact read-only summary", () => {
    const markup = renderToStaticMarkup(
      <TradexListSummary
        summary={{
          code: "7203",
          asof: "2026-03-19",
          available: true,
          reason: null,
          dominantTone: "buy",
          confidence: 0.84,
          publishReadiness: {
            ready: true,
            status: "ready",
            reasons: ["validation_pass"],
            candidateKey: "candidate:7203",
            approved: true,
          },
          reasons: ["tone=up", "pattern=breakout", "ignored"],
        }}
      />
    );

    expect(markup).toContain("検証 買い");
    expect(markup).toContain("信頼度 84%");
    expect(markup).toContain("採用確認: 確認済み");
    expect(markup).not.toContain("tone=up");
    expect(markup).not.toContain("pattern=breakout");
    expect(markup).not.toContain("ignored");
  });

  it("renders unavailable state with a stable reason", () => {
    const markup = renderToStaticMarkup(
      <TradexListSummary
        summary={{
          code: "7203",
          asof: null,
          available: false,
          reason: "analysis unavailable",
          dominantTone: null,
          confidence: null,
          publishReadiness: null,
          reasons: [],
        }}
      />
    );

    expect(markup).toContain("分析を確認できません");
    expect(markup).toContain("分析データ未準備");
  });

  it("renders the short lifecycle overlay as review-only support", () => {
    const markup = renderToStaticMarkup(
      <TradexListSummary
        summary={{
          code: "5016",
          asof: "2026-06-05",
          available: true,
          reason: null,
          dominantTone: null,
          confidence: null,
          publishReadiness: null,
          reasons: [],
          shortLifecycle: {
            state: "Probe",
            rank: 1,
            signalYmd: "20260526",
            expectedDownsidePct: 0.1378,
            riskRewardToSl8: 1.51,
            setupState: "SetupReady",
            continuationStatus: "ContinuationPermit",
            finalReviewStatus: "BlockShort",
            reasons: ["setup_ready_confirmed_continuation"],
            reviewOnly: true,
            artifactCreatedAt: "2026-06-05T02:48:15+00:00",
          },
        }}
      />
    );

    expect(markup).toContain("TRADEX売り監視: 打診");
    expect(markup).toContain("期待下げ 13.8%");
    expect(markup).toContain("RR 1.51");
    expect(markup).toContain("signal 20260526");
    expect(markup).not.toContain("信頼度");
  });
});
