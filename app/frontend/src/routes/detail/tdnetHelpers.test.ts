import { describe, expect, it } from "vitest";

import {
  buildTdnetHighlights,
  formatTdnetEventTypeLabel,
  formatTdnetSentimentLabel,
  shouldAutoRefreshTdnet,
} from "./detailHelpers";

describe("TDNET helper display", () => {
  it("formats event and sentiment labels", () => {
    expect(formatTdnetEventTypeLabel("forecast_revision")).toBe("業績予想修正");
    expect(formatTdnetSentimentLabel("negative")).toBe("悪材料");
  });

  it("builds highlight cards from recent disclosures", () => {
    const items = buildTdnetHighlights(
      [
        {
          disclosureId: "a",
          title: "自己株式取得に係る事項の決定",
          publishedAt: "2026-03-12T08:30:00+09:00",
          fetchedAt: "2026-03-12T08:32:00+09:00",
          tdnetUrl: "https://example.com/a",
          pdfUrl: null,
          xbrlUrl: null,
          summaryText: "買付上限を設定",
          eventType: "share_buyback",
          sentiment: "positive",
          importanceScore: 0.9,
          tags: ["share_buyback"],
        },
      ],
      3
    );

    expect(items[0]).toMatchObject({
      disclosureId: "a",
      eventLabel: "自社株買い",
      sentimentLabel: "好材料",
      importanceLabel: "重要",
      tone: "up",
    });
  });

  it("requests refresh when TDNET data is missing or stale", () => {
    expect(shouldAutoRefreshTdnet([], Date.parse("2026-03-12T12:00:00+09:00"))).toBe(true);
    expect(
      shouldAutoRefreshTdnet(
        [
          {
            disclosureId: "a",
            title: "決算短信",
            publishedAt: "2026-03-11T15:30:00+09:00",
            fetchedAt: "2026-03-11T16:00:00+09:00",
            tdnetUrl: null,
            pdfUrl: null,
            xbrlUrl: null,
            summaryText: null,
            eventType: "earnings",
            sentiment: "neutral",
            importanceScore: 0.8,
            tags: ["earnings"],
          },
        ],
        Date.parse("2026-03-12T09:30:00+09:00")
      )
    ).toBe(false);
    expect(
      shouldAutoRefreshTdnet(
        [
          {
            disclosureId: "a",
            title: "決算短信",
            publishedAt: "2026-03-10T15:30:00+09:00",
            fetchedAt: "2026-03-10T16:00:00+09:00",
            tdnetUrl: null,
            pdfUrl: null,
            xbrlUrl: null,
            summaryText: null,
            eventType: "earnings",
            sentiment: "neutral",
            importanceScore: 0.8,
            tags: ["earnings"],
          },
        ],
        Date.parse("2026-03-12T12:30:00+09:00")
      )
    ).toBe(true);
  });
});
