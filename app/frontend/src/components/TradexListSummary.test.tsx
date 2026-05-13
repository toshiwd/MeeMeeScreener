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
});
