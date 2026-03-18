import { describe, expect, it } from "vitest";

import { buildEdinetFinancialDisplay } from "./detailHelpers";

describe("buildEdinetFinancialDisplay", () => {
  it("combines the latest financial point with EDINET summary metrics", () => {
    const display = buildEdinetFinancialDisplay({
      latestFinancialPoint: {
        fiscalYear: 2025,
        label: "2025",
        revenue: 1_000_000_000,
        grossProfit: 240_000_000,
        operatingIncome: 120_000_000,
        netIncome: -50_000_000,
        grossMargin: 0.24,
        operatingMargin: 0.12,
        netMargin: -0.05,
        roe: 0.14,
        roa: 0.05,
        eps: 100,
        bps: 800,
        dividendPerShare: 40,
        equityRatio: 0.45,
        netInterestBearingDebt: -30_000_000,
      },
      latestPrice: 1250,
      edinetSummary: {
        status: "ok",
        mapped: true,
        freshnessDays: 2,
        metricCount: 6,
        qualityScore: 1,
        dataScore: 0.92,
        scoreBonus: 0.03,
        featureFlagApplied: true,
        ebitdaMetric: 180_000_000,
        roe: 0.14,
        equityRatio: 0.45,
        debtRatio: 0.85,
        operatingCfMargin: 0.09,
        revenueGrowthYoy: 0.08,
      },
    });

    expect(display.cards.find((item) => item.label === "営業利益率")).toMatchObject({
      value: "12.0%",
      tone: "up",
    });
    expect(display.cards.find((item) => item.label === "純利益")).toMatchObject({
      tone: "down",
    });
    expect(display.stats.find((item) => item.label === "D/E")).toMatchObject({
      value: "0.85",
      tone: "up",
    });
    expect(display.stats.find((item) => item.label === "売上成長率")).toMatchObject({
      value: "8.0%",
      tone: "up",
    });
    expect(display.stats.find((item) => item.label === "PER")?.value.startsWith("12.5")).toBe(true);
  });
});
