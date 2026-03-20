import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { DetailFinancialPanel } from "./DetailFinancialPanel";

const fmtNumber = (value: number | null | undefined, digits = 0) =>
  value == null ? "--" : value.toFixed(digits);
const fmtPercent = (value: number | null | undefined, digits = 1) =>
  value == null ? "--" : `${(value * 100).toFixed(digits)}%`;
const fmtFinancialAmount = (value: number | null | undefined) =>
  value == null ? "--" : `${value.toLocaleString("ja-JP")}円`;

describe("DetailFinancialPanel", () => {
  it("shows primary KPI cards without a details toggle", () => {
    const markup = renderToStaticMarkup(
      <DetailFinancialPanel
        financialPanelRef={{ current: null } as never}
        financialPanel={{ summary: { latestFiscalYear: 2025 }, mapped: true } as never}
        financialFetchedLabel="2026-03-19"
        financialLoading={false}
        financialSeries={[
          {
            label: "2024",
            revenue: 100,
            operatingIncome: 10,
            netIncome: 8,
            grossMargin: 0.4,
            operatingMargin: 0.1,
            netMargin: 0.08,
            roe: 0.12,
            roa: 0.06,
          },
        ]}
        financialCards={[{ label: "売上高", value: "100", tone: "neutral" }]}
        financialKeyStats={[{ label: "EPS", value: "8.0", tone: "up" }]}
        tdnetHighlights={[
          {
            disclosureId: "tdnet-1",
            title: "適時開示タイトル",
            publishedLabel: "2026-03-19",
            eventLabel: "決算",
            sentimentLabel: "positive",
            summaryText: "summary",
            tone: "up",
            importanceLabel: "high",
            tdnetUrl: "#",
            pdfUrl: null,
            xbrlUrl: null,
          },
        ]}
        tdnetLoading={false}
        tdnetStatusLabel="TDNET ready"
        taisyakuCards={[{ label: "貸借倍率", value: "1.2", tone: "neutral" }]}
        taisyakuHistory={[
          {
            dateLabel: "2026-03-18",
            loanRatioLabel: "1.2",
            financeLabel: "10",
            stockLabel: "20",
            feeLabel: "0.1",
          },
        ]}
        taisyakuRestrictions={[]}
        taisyakuLoading={false}
        taisyakuStatusLabel="貸借 ready"
        taisyakuWatchLabel="watch"
        formatNumber={fmtNumber}
        formatPercentLabel={fmtPercent}
        formatFinancialAmountLabel={fmtFinancialAmount}
      />
    );

    expect(markup).toContain("EDINET / TDNET / 貸借");
    expect(markup).toContain("主要KPI");
    expect(markup).toContain("補助指標");
    expect(markup).toContain("TDNET動向");
    expect(markup).toContain("貸借情報");
    expect(markup).toContain("売上・利益");
    expect(markup).toContain("利益率 / ROE");
    expect(markup).not.toContain("詳細");
  });
});
