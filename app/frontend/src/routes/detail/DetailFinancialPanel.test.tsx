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
  it("renders EDINET status, analysis summary, text highlights, and official filings", () => {
    const markup = renderToStaticMarkup(
      <DetailFinancialPanel
        financialPanelRef={{ current: null } as never}
        financialPanel={{
          status: "empty_tables",
          statusDetail: "empty_tables",
          mapped: true,
          fetchedAt: "2026-03-19T09:00:00.000Z",
          lastCheckedAt: "2026-03-20T09:00:00.000Z",
          bootstrapState: {
            active: true,
            mode: "backfill_700",
            jobId: "job-1",
            message: "Running EDINET backfill...",
          },
          summary: {
            latestFiscalYear: 2025,
            equityRatio: null,
            eps: null,
            bps: null,
            dividendPerShare: null,
            netInterestBearingDebt: null,
          },
          series: [],
          analysisSummary: {
            asOf: "2025-12-31",
            items: [
              { label: "概要", value: "利益率が改善しています。" },
              { label: "強み", value: "価格改定が寄与しています。" },
            ],
          },
          textHighlights: [{ blockName: "business", fiscalYear: "2025", excerpt: "事業の概要サマリー" }],
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
        }}
        financialFetchedLabel="2026-03-19"
        financialLoading={false}
        financialSeries={[]}
        financialCards={[]}
        financialKeyStats={[]}
        tdnetHighlights={[]}
        tdnetLoading={false}
        tdnetStatusLabel={null}
        taisyakuCards={[]}
        taisyakuHistory={[]}
        taisyakuRestrictions={[]}
        taisyakuLoading={false}
        taisyakuStatusLabel={null}
        taisyakuWatchLabel={null}
        formatNumber={fmtNumber}
        formatPercentLabel={fmtPercent}
        formatFinancialAmountLabel={fmtFinancialAmount}
      />
    );

    expect(markup).toContain("EDINET / TDNET / 貸借");
    expect(markup).toContain("EDINETデータはまだ未投入です。");
    expect(markup).toContain("EDINET分析要点");
    expect(markup).toContain("有報要約");
    expect(markup).toContain("公式提出書類");
    expect(markup).toContain("有価証券報告書");
    expect(markup).toContain("S100TEST1");
  });

  it("shows a loading message when EDINET data is being fetched", () => {
    const markup = renderToStaticMarkup(
      <DetailFinancialPanel
        financialPanelRef={{ current: null } as never}
        financialPanel={{
          status: "loading",
          statusDetail: "bootstrap_active",
          mapped: true,
          fetchedAt: null,
          lastCheckedAt: null,
          bootstrapState: {
            active: true,
            mode: "daily_watch",
            jobId: "job-2",
            message: "Waiting in queue...",
          },
          summary: null,
          series: [],
          analysisSummary: null,
          textHighlights: [],
          officialFilings: [],
        }}
        financialFetchedLabel={null}
        financialLoading={false}
        financialSeries={[]}
        financialCards={[]}
        financialKeyStats={[]}
        tdnetHighlights={[]}
        tdnetLoading={false}
        tdnetStatusLabel={null}
        taisyakuCards={[]}
        taisyakuHistory={[]}
        taisyakuRestrictions={[]}
        taisyakuLoading={false}
        taisyakuStatusLabel={null}
        taisyakuWatchLabel={null}
        formatNumber={fmtNumber}
        formatPercentLabel={fmtPercent}
        formatFinancialAmountLabel={fmtFinancialAmount}
      />
    );

    expect(markup).toContain("EDINETデータを取得中です。");
    expect(markup).toContain("日次取得");
  });
  it("renders TDNET free-provider metadata and report links", () => {
    const markup = renderToStaticMarkup(
      <DetailFinancialPanel
        financialPanelRef={{ current: null } as never}
        financialPanel={null}
        financialFetchedLabel={null}
        financialLoading={false}
        financialSeries={[]}
        financialCards={[]}
        financialKeyStats={[]}
        tdnetHighlights={[
          {
            disclosureId: "yanoshin:1244728",
            title: "FY2026 financial results",
            publishedLabel: "2026-05-08 13:55",
            eventLabel: "earnings",
            sentimentLabel: "neutral",
            summaryText: null,
            tone: "neutral",
            importanceLabel: null,
            tdnetUrl: "https://example.test/report.pdf",
            pdfUrl: "https://example.test/report.pdf",
            xbrlUrl: "https://example.test/report.zip",
            sourceProvider: "yanoshin",
            markets: "TSE/NSE",
            reportLinks: [
              { label: "Summary", url: "https://example.test/summary.pdf" },
              { label: "XBRL", url: "https://example.test/report.zip" },
            ],
          },
        ]}
        tdnetLoading={false}
        tdnetStatusLabel="TDNET latest fetched"
        taisyakuCards={[]}
        taisyakuHistory={[]}
        taisyakuRestrictions={[]}
        taisyakuLoading={false}
        taisyakuStatusLabel={null}
        taisyakuWatchLabel={null}
        formatNumber={fmtNumber}
        formatPercentLabel={fmtPercent}
        formatFinancialAmountLabel={fmtFinancialAmount}
      />
    );

    expect(markup).toContain("yanoshin");
    expect(markup).toContain("TSE/NSE");
    expect(markup).toContain("Summary");
    expect(markup).toContain('href="https://example.test/summary.pdf"');
    expect(markup).toContain('href="https://example.test/report.zip"');
  });
});
