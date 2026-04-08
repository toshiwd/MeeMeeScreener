import { describe, expect, it } from "vitest";

import { formatEdinetStatus, normalizeEdinetFinancialPanel } from "./detailHelpers";

describe("normalizeEdinetFinancialPanel", () => {
  it("reads the extended EDINET payload shape", () => {
    const panel = normalizeEdinetFinancialPanel({
      status: "ok",
      statusDetail: "bootstrap_active",
      mapped: "1",
      fetchedAt: "2026-03-20T09:00:00.000Z",
      lastCheckedAt: "2026-03-20T10:00:00.000Z",
      bootstrapState: {
        active: true,
        mode: "daily_watch",
        jobId: "job-1",
        message: "Running",
      },
      summary: {
        latestFiscalYear: 2025,
        equityRatio: 0.41,
        eps: 123.4,
        bps: 800,
        dividendPerShare: 40,
        netInterestBearingDebt: -1_000_000,
      },
      series: [
        {
          fiscalYear: 2025,
          label: "2025",
          revenue: 100,
          grossProfit: 40,
          operatingIncome: 20,
          netIncome: 10,
          grossMargin: 0.4,
          operatingMargin: 0.2,
          netMargin: 0.1,
          roe: 0.12,
          roa: 0.05,
          eps: 123.4,
          bps: 800,
          dividendPerShare: 40,
          equityRatio: 0.41,
          netInterestBearingDebt: -1_000_000,
        },
      ],
      analysisSummary: {
        asOf: "2025-12-31",
        items: [
          { label: "概要", value: "利益率が改善しています。" },
          { label: "強み", value: "価格改定が寄与しています。" },
        ],
      },
      textHighlights: [{ blockName: "strategy", fiscalYear: "2025", excerpt: "戦略方針を要約しています。" }],
      officialFilings: [
        {
          docId: "S100TEST1",
          submitDateTime: "2026-03-28 15:00",
          docDescription: "有価証券報告書",
          formCode: "030000",
          periodLabel: "2025-04-01 - 2026-03-31",
          filerName: "Target",
          hasCsv: true,
          hasPdf: true,
          hasXbrl: true,
          searchUrl: "https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx?code=1301",
        },
      ],
    });

    expect(panel).not.toBeNull();
    expect(panel?.status).toBe("ok");
    expect(panel?.statusDetail).toBe("bootstrap_active");
    expect(panel?.mapped).toBe(true);
    expect(panel?.bootstrapState?.mode).toBe("daily_watch");
    expect(panel?.analysisSummary?.items).toHaveLength(2);
    expect(panel?.textHighlights).toHaveLength(1);
    expect(panel?.textHighlights[0]?.blockName).toBe("strategy");
    expect(panel?.officialFilings).toHaveLength(1);
    expect(panel?.officialFilings[0]?.docId).toBe("S100TEST1");
  });
});

describe("formatEdinetStatus", () => {
  it("renders the refined EDINET statuses", () => {
    expect(formatEdinetStatus("empty_tables")).toBe("未投入");
    expect(formatEdinetStatus("loading")).toBe("取得中");
    expect(formatEdinetStatus("error")).toBe("取得失敗");
  });
});
