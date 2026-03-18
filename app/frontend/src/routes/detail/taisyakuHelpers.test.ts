import { describe, expect, it } from "vitest";

import { buildTaisyakuDisplay, normalizeTaisyakuSnapshot, shouldAutoRefreshTaisyaku } from "./detailHelpers";

describe("taisyaku helpers", () => {
  it("normalizes snapshot payload", () => {
    const snapshot = normalizeTaisyakuSnapshot({
      code: "1306",
      fetchedAt: "2026-03-12T09:10:00+09:00",
      latestBalance: {
        applicationDate: 20260311,
        financeBalanceShares: 14410,
        stockBalanceShares: 48030,
        loanRatio: 0.3000208203,
      },
      balanceHistory: [],
      latestFee: null,
      restrictions: [],
    });

    expect(snapshot?.code).toBe("1306");
    expect(snapshot?.latestBalance?.applicationDate).toBe(20260311);
    expect(snapshot?.latestBalance?.loanRatio).toBeCloseTo(0.3000208203);
  });

  it("builds display cards and history rows", () => {
    const display = buildTaisyakuDisplay({
      code: "1306",
      issue: null,
      fetchedAt: "2026-03-12T09:10:00+09:00",
      latestBalance: {
        applicationDate: 20260311,
        settlementDate: 20260313,
        issueName: "NEXT TOPIX ETF",
        marketName: "東証",
        reportType: "確報",
        financeBalanceShares: 14410,
        stockBalanceShares: 48030,
        netBalanceShares: -33620,
        loanRatio: 0.3000208203,
        fetchedAt: "2026-03-12T09:10:00+09:00",
      },
      balanceHistory: [
        {
          applicationDate: 20260311,
          settlementDate: 20260313,
          issueName: "NEXT TOPIX ETF",
          marketName: "東証",
          reportType: "確報",
          financeBalanceShares: 14410,
          stockBalanceShares: 48030,
          netBalanceShares: -33620,
          loanRatio: 0.3000208203,
          fetchedAt: "2026-03-12T09:10:00+09:00",
        },
      ],
      latestFee: {
        applicationDate: 20260311,
        settlementDate: 20260313,
        issueName: "NEXT TOPIX ETF",
        marketName: "東証",
        reasonType: "臨時",
        reasonValue: "20260331",
        priceYen: 3883,
        stockExcessShares: 48030,
        maxFeeYen: 9,
        currentFeeYen: 0,
        feeDays: 3,
        priorFeeYen: 0,
        fetchedAt: "2026-03-12T09:10:00+09:00",
      },
      restrictions: [
        {
          issueName: "NEXT TOPIX ETF",
          announcementKind: null,
          measureType: "注意喚起",
          measureDetail: "新規売り",
          noticeDate: 20260312,
          afternoonStop: null,
          fetchedAt: "2026-03-12T09:10:00+09:00",
        },
      ],
    });

    expect(display.watchLabel).toBe("需給警戒");
    expect(display.cards[0]).toMatchObject({ label: "貸借倍率", value: "0.30倍" });
    expect(display.history[0]).toMatchObject({ dateLabel: "2026/03/11", feeLabel: "0.00円" });
  });

  it("requests refresh when snapshot is missing or stale", () => {
    expect(shouldAutoRefreshTaisyaku(null, Date.parse("2026-03-12T12:00:00+09:00"))).toBe(true);
    expect(
      shouldAutoRefreshTaisyaku(
        {
          code: "1306",
          issue: null,
          latestBalance: null,
          balanceHistory: [],
          latestFee: null,
          restrictions: [],
          fetchedAt: "2026-03-12T08:00:00+09:00",
        },
        Date.parse("2026-03-12T12:00:00+09:00")
      )
    ).toBe(false);
    expect(
      shouldAutoRefreshTaisyaku(
        {
          code: "1306",
          issue: null,
          latestBalance: null,
          balanceHistory: [],
          latestFee: null,
          restrictions: [],
          fetchedAt: "2026-03-11T08:00:00+09:00",
        },
        Date.parse("2026-03-12T12:30:00+09:00")
      )
    ).toBe(true);
  });
});
