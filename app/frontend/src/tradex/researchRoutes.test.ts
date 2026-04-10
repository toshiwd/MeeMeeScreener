import { buildTradexResearchCsvHref, TRADEX_RESEARCH_ENDPOINTS, tradexResearchRoute } from "./researchRoutes";

describe("tradex research routes", () => {
  it("builds named API routes under the tradex research namespace", () => {
    expect(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.stateEvalActionQueue)).toBe("/tradex/research/state-eval-action-queue");
    expect(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.replayProgress)).toBe("/tradex/research/replay-progress");
    expect(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.stateEvalTrends)).toBe("/tradex/research/state-eval-trends");
    expect(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.stateEvalPromotionReview)).toBe("/tradex/research/state-eval-promotion-review");
  });

  it("builds CSV download URLs under the tradex research namespace", () => {
    const params = new URLSearchParams({ side: "long", limit: "200" });
    expect(buildTradexResearchCsvHref(TRADEX_RESEARCH_ENDPOINTS.stateEvalTagsCsv, params, "/api")).toBe(
      "/api/tradex/research/state-eval-tags.csv?side=long&limit=200"
    );
    expect(buildTradexResearchCsvHref(TRADEX_RESEARCH_ENDPOINTS.stateEvalDailySummaryCsv, params, "/api")).toBe(
      "/api/tradex/research/state-eval-daily-summary.csv?side=long&limit=200"
    );
  });
});
