import type { Ref } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import ScreenPanel from "../../components/ScreenPanel";
import type { EdinetFinancialPanel, EdinetFinancialPoint } from "./detailTypes";
import { formatDateTimeLabel } from "../../utils/dateLabels";

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatFinancialAmountLabel = (value: number | null | undefined) => string;

type FinancialCard = {
  readonly label: string;
  readonly value: string;
  readonly tone: "up" | "down" | "neutral";
};

type FinancialKeyStat = {
  readonly label: string;
  readonly value: string;
  readonly tone: "up" | "down" | "neutral";
};

type TdnetHighlight = {
  readonly disclosureId: string;
  readonly title: string;
  readonly publishedLabel: string;
  readonly eventLabel: string;
  readonly sentimentLabel: string | null;
  readonly summaryText: string | null;
  readonly tone: "up" | "down" | "neutral";
  readonly importanceLabel: string | null;
  readonly tdnetUrl: string | null;
  readonly pdfUrl: string | null;
  readonly xbrlUrl: string | null;
};

type TaisyakuCard = {
  readonly label: string;
  readonly value: string;
  readonly tone: "up" | "down" | "neutral";
};

type TaisyakuHistoryRow = {
  readonly dateLabel: string;
  readonly loanRatioLabel: string;
  readonly financeLabel: string;
  readonly stockLabel: string;
  readonly feeLabel: string;
};

type TaisyakuRestriction = {
  readonly measureType: string | null;
  readonly measureDetail: string | null;
  readonly noticeDate: number | null;
};

const formatEdinetPanelStatus = (status: string | null | undefined, statusDetail: string | null | undefined) => {
  if (status === "ok") return null;
  if (status === "loading") return "EDINETデータを取得中です。";
  if (status === "empty_tables") return "EDINETデータはまだ未投入です。";
  if (status === "error") return "EDINETデータの取得に失敗しました。";
  if (status === "unmapped") return "EDINETのマッピングがありません。";
  if (status === "no_payload") return "EDINETの取得対象データがありません。";
  if (statusDetail) return `EDINET状態: ${statusDetail}`;
  return null;
};

const formatBootstrapStateLabel = (state: EdinetFinancialPanel["bootstrapState"] | undefined) => {
  if (!state?.active) return null;
  const modeLabel = state.mode === "backfill_700" ? "初回バックフィル" : "日次取得";
  return state.message ? `${modeLabel}: ${state.message}` : `${modeLabel}を実行中です。`;
};

export type Props = {
  financialPanelRef: Ref<HTMLDivElement>;
  financialPanel: EdinetFinancialPanel | null;
  financialFetchedLabel: string | null;
  financialLoading: boolean;
  financialSeries: EdinetFinancialPoint[];
  financialCards: readonly FinancialCard[];
  financialKeyStats: readonly FinancialKeyStat[];
  tdnetHighlights: readonly TdnetHighlight[];
  tdnetLoading: boolean;
  tdnetStatusLabel: string | null;
  taisyakuCards: readonly TaisyakuCard[];
  taisyakuHistory: readonly TaisyakuHistoryRow[];
  taisyakuRestrictions: readonly TaisyakuRestriction[];
  taisyakuLoading: boolean;
  taisyakuStatusLabel: string | null;
  taisyakuWatchLabel: string | null;
  formatNumber: FormatNumber;
  formatPercentLabel: FormatPercentLabel;
  formatFinancialAmountLabel: FormatFinancialAmountLabel;
};

export function DetailFinancialPanel(props: Props) {
  const {
    financialPanelRef,
    financialPanel,
    financialFetchedLabel,
    financialLoading,
    financialSeries,
    financialCards,
    financialKeyStats,
    tdnetHighlights,
    tdnetLoading,
    tdnetStatusLabel,
    taisyakuCards,
    taisyakuHistory,
    taisyakuRestrictions,
    taisyakuLoading,
    taisyakuStatusLabel,
    taisyakuWatchLabel,
    formatNumber,
    formatPercentLabel,
    formatFinancialAmountLabel,
  } = props;

  const hasFinancialSeries = financialSeries.length > 0;
  const hasPrimaryKpi = financialCards.length > 0 || financialKeyStats.length > 0;
  const analysisItems = financialPanel?.analysisSummary?.items ?? [];
  const textHighlights = financialPanel?.textHighlights ?? [];
  const officialFilings = financialPanel?.officialFilings ?? [];
  const hasEdinetAnalysisSection = Boolean(financialPanel?.analysisSummary || financialPanel?.status);
  const hasEdinetTextSection = textHighlights.length > 0;
  const hasOfficialFilingsSection = officialFilings.length > 0;
  const hasTdnetSection = tdnetLoading || tdnetHighlights.length > 0;
  const hasTaisyakuSection =
    taisyakuLoading ||
    taisyakuCards.length > 0 ||
    taisyakuHistory.length > 0 ||
    taisyakuRestrictions.length > 0;
  const panelStatusLabel = formatEdinetPanelStatus(financialPanel?.status, financialPanel?.statusDetail);
  const bootstrapStateLabel = formatBootstrapStateLabel(financialPanel?.bootstrapState);
  const hasContent =
    hasPrimaryKpi ||
    hasFinancialSeries ||
    hasEdinetAnalysisSection ||
    hasEdinetTextSection ||
    hasOfficialFilingsSection ||
    hasTdnetSection ||
    hasTaisyakuSection ||
    panelStatusLabel != null ||
    bootstrapStateLabel != null;

  return (
    <ScreenPanel
      ref={financialPanelRef}
      title="EDINET / TDNET / 貸借"
      summary={
        financialPanel?.summary?.latestFiscalYear != null
          ? `最新年度 ${financialPanel.summary.latestFiscalYear}`
          : undefined
      }
      details={financialFetchedLabel ? `取得 ${financialFetchedLabel}` : undefined}
      className="detail-analysis-panel detail-financial-panel"
    >
      <div className="detail-analysis-body detail-financial-body">
        {(financialPanel?.summary?.latestFiscalYear != null ||
          financialFetchedLabel ||
          financialPanel?.lastCheckedAt ||
          panelStatusLabel ||
          bootstrapStateLabel) && (
          <div className="detail-financial-meta-row">
            {financialPanel?.summary?.latestFiscalYear != null && (
              <div className="detail-financial-meta-pill">最新年度 {financialPanel.summary.latestFiscalYear}</div>
            )}
            {financialFetchedLabel && <div className="detail-financial-meta-pill">取得 {financialFetchedLabel}</div>}
            {financialPanel?.lastCheckedAt && (
              <div className="detail-financial-meta-pill">
                最終確認 {formatDateTimeLabel(financialPanel.lastCheckedAt)}
              </div>
            )}
          </div>
        )}
        {panelStatusLabel && <div className="detail-analysis-meta">{panelStatusLabel}</div>}
        {bootstrapStateLabel && <div className="detail-analysis-meta">{bootstrapStateLabel}</div>}

        {financialLoading ? (
          <div className="detail-analysis-empty">EDINETデータを取得中です。</div>
        ) : !hasContent ? (
          <div className="detail-analysis-empty">
            {financialPanel?.mapped === false ? "EDINETのマッピングがありません。" : "EDINETデータがありません。"}
          </div>
        ) : (
          <div className="detail-financial-scroll">
            {hasPrimaryKpi && (
              <section className="detail-analysis-section detail-financial-section detail-financial-section--primary">
                {financialCards.length > 0 && (
                  <>
                    <div className="detail-analysis-section-title">主要KPI</div>
                    <div className="detail-financial-card-grid">
                      {financialCards.map((card) => (
                        <div key={card.label} className="detail-financial-card">
                          <div className="detail-financial-card-label">{card.label}</div>
                          <div className={`detail-financial-card-value detail-analysis-value--${card.tone}`}>{card.value}</div>
                        </div>
                      ))}
                    </div>
                  </>
                )}

                {financialKeyStats.length > 0 && (
                  <>
                    <div className="detail-analysis-section-title">補助指標</div>
                    <div className="detail-financial-stats-grid detail-financial-stats-grid--primary">
                      {financialKeyStats.map((item) => (
                        <div key={item.label} className="detail-financial-stat">
                          <div className="detail-financial-stat-label">{item.label}</div>
                          <div className={`detail-financial-stat-value detail-analysis-value--${item.tone}`}>{item.value}</div>
                        </div>
                      ))}
                    </div>
                  </>
                )}
              </section>
            )}

            {hasEdinetAnalysisSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">EDINET分析要点</div>
                {financialPanel?.analysisSummary?.asOf && (
                  <div className="detail-analysis-meta">asOf {financialPanel.analysisSummary.asOf}</div>
                )}
                {analysisItems.length > 0 ? (
                  <div className="detail-financial-edinet-list">
                    {analysisItems.map((item) => (
                      <div key={item.label} className="detail-financial-edinet-item">
                        <div className="detail-financial-edinet-label">{item.label}</div>
                        <div className="detail-financial-edinet-value">{item.value}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="detail-analysis-empty">{panelStatusLabel ?? "EDINET分析要点はありません。"}</div>
                )}
              </section>
            )}

            {hasEdinetTextSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">有報要約</div>
                <div className="detail-financial-edinet-text-list">
                  {textHighlights.map((item) => (
                    <div
                      key={`${item.blockName}:${item.fiscalYear ?? ""}:${item.excerpt.slice(0, 24)}`}
                      className="detail-financial-edinet-text-item"
                    >
                      <div className="detail-financial-edinet-text-head">
                        <span className="detail-financial-edinet-text-title">{item.blockName}</span>
                        {item.fiscalYear && <span className="detail-financial-edinet-text-year">{item.fiscalYear}</span>}
                      </div>
                      <div className="detail-financial-edinet-text-excerpt">{item.excerpt}</div>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {hasOfficialFilingsSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">公式提出書類</div>
                <div className="detail-financial-edinet-text-list">
                  {officialFilings.map((item) => (
                    <div key={item.docId} className="detail-financial-edinet-text-item">
                      <div className="detail-financial-edinet-text-head">
                        <span className="detail-financial-edinet-text-title">{item.docDescription ?? item.docId}</span>
                        {item.submitDateTime && (
                          <span className="detail-financial-edinet-text-year">{item.submitDateTime}</span>
                        )}
                      </div>
                      <div className="detail-financial-edinet-text-excerpt">
                        {[item.filerName, item.periodLabel, item.formCode].filter(Boolean).join(" / ")}
                      </div>
                      <div className="detail-financial-tdnet-pills">
                        {item.hasCsv && <span className="detail-financial-tdnet-pill">CSV</span>}
                        {item.hasPdf && <span className="detail-financial-tdnet-pill">PDF</span>}
                        {item.hasXbrl && <span className="detail-financial-tdnet-pill">XBRL</span>}
                        <span className="detail-financial-tdnet-pill">{item.docId}</span>
                      </div>
                      {item.searchUrl && (
                        <div className="detail-financial-tdnet-links">
                          <a href={item.searchUrl} target="_blank" rel="noreferrer">
                            EDINET
                          </a>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </section>
            )}

            {hasTdnetSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">TDNET開示</div>
                {tdnetStatusLabel && <div className="detail-analysis-meta">{tdnetStatusLabel}</div>}
                {tdnetLoading ? (
                  <div className="detail-analysis-empty">TDNETデータを取得中です。</div>
                ) : tdnetHighlights.length > 0 ? (
                  <div className="detail-financial-tdnet-list">
                    {tdnetHighlights.map((item) => (
                      <div key={item.disclosureId} className="detail-financial-tdnet-item">
                        <div className="detail-financial-tdnet-head">
                          <div className={`detail-financial-tdnet-title detail-analysis-value--${item.tone}`}>{item.title}</div>
                          <div className="detail-financial-tdnet-pills">
                            <span className="detail-financial-tdnet-pill">{item.eventLabel}</span>
                            {item.sentimentLabel && <span className="detail-financial-tdnet-pill">{item.sentimentLabel}</span>}
                            {item.importanceLabel && <span className="detail-financial-tdnet-pill">{item.importanceLabel}</span>}
                          </div>
                        </div>
                        <div className="detail-analysis-meta">{item.publishedLabel}</div>
                        {item.summaryText && <div className="detail-financial-tdnet-summary">{item.summaryText}</div>}
                        <div className="detail-financial-tdnet-links">
                          {item.tdnetUrl && (
                            <a href={item.tdnetUrl} target="_blank" rel="noreferrer">
                              TDNET
                            </a>
                          )}
                          {item.pdfUrl && (
                            <a href={item.pdfUrl} target="_blank" rel="noreferrer">
                              PDF
                            </a>
                          )}
                          {item.xbrlUrl && (
                            <a href={item.xbrlUrl} target="_blank" rel="noreferrer">
                              XBRL
                            </a>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="detail-analysis-empty">TDNET開示はありません。</div>
                )}
              </section>
            )}

            {hasTaisyakuSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">貸借情報</div>
                {taisyakuStatusLabel && <div className="detail-analysis-meta">{taisyakuStatusLabel}</div>}
                {taisyakuWatchLabel && <div className="detail-financial-meta-pill">{taisyakuWatchLabel}</div>}
                {taisyakuLoading ? (
                  <div className="detail-analysis-empty">貸借データを取得中です。</div>
                ) : taisyakuCards.length > 0 ? (
                  <>
                    <div className="detail-financial-card-grid detail-financial-card-grid--compact">
                      {taisyakuCards.map((card) => (
                        <div key={card.label} className="detail-financial-card">
                          <div className="detail-financial-card-label">{card.label}</div>
                          <div className={`detail-financial-card-value detail-analysis-value--${card.tone}`}>{card.value}</div>
                        </div>
                      ))}
                    </div>
                    {taisyakuRestrictions.length > 0 && (
                      <div className="detail-financial-taisyaku-alerts">
                        {taisyakuRestrictions.map((item, index) => (
                          <div
                            key={`${item.measureType ?? "measure"}:${item.noticeDate ?? index}`}
                            className="detail-financial-taisyaku-alert"
                          >
                            <span className="detail-financial-tdnet-pill">{item.measureType ?? "種別"}</span>
                            <span>{item.measureDetail ?? "--"}</span>
                            {item.noticeDate != null && <span className="detail-analysis-meta">{item.noticeDate}</span>}
                          </div>
                        ))}
                      </div>
                    )}
                    {taisyakuHistory.length > 0 && (
                      <div className="detail-financial-taisyaku-table">
                        <div className="detail-financial-taisyaku-row detail-financial-taisyaku-row--head">
                          <span>日付</span>
                          <span>貸借倍率</span>
                          <span>融資残</span>
                          <span>貸株残</span>
                          <span>品貸料</span>
                        </div>
                        {taisyakuHistory.map((item) => (
                          <div key={item.dateLabel} className="detail-financial-taisyaku-row">
                            <span>{item.dateLabel}</span>
                            <span>{item.loanRatioLabel}</span>
                            <span>{item.financeLabel}</span>
                            <span>{item.stockLabel}</span>
                            <span>{item.feeLabel}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                ) : (
                  <div className="detail-analysis-empty">貸借情報はありません。</div>
                )}
              </section>
            )}

            {hasFinancialSeries && (
              <>
                <section className="detail-analysis-section detail-financial-section">
                  <div className="detail-analysis-section-title">損益推移</div>
                  <div className="detail-financial-chart">
                    <ResponsiveContainer width="100%" height={240}>
                      <BarChart data={financialSeries}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.18)" />
                        <XAxis dataKey="label" stroke="#64748b" tickLine={false} axisLine={false} />
                        <YAxis
                          stroke="#64748b"
                          tickLine={false}
                          axisLine={false}
                          tickFormatter={(value) => `${formatNumber(Number(value) / 100_000_000, 0)}億`}
                        />
                        <Tooltip formatter={(value) => formatFinancialAmountLabel(Number(value))} />
                        <Legend wrapperStyle={{ fontSize: 12, paddingTop: 8, color: "#475569" }} iconSize={10} />
                        <Bar dataKey="revenue" name="売上高" fill="#38bdf8" radius={[4, 4, 0, 0]} />
                        <Bar dataKey="operatingIncome" name="営業利益" fill="#4ade80" radius={[4, 4, 0, 0]} />
                        <Bar dataKey="netIncome" name="純利益" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </section>
                <section className="detail-analysis-section detail-financial-section">
                  <div className="detail-analysis-section-title">利益率 / ROE</div>
                  <div className="detail-financial-chart">
                    <ResponsiveContainer width="100%" height={240}>
                      <LineChart data={financialSeries}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.18)" />
                        <XAxis dataKey="label" stroke="#64748b" tickLine={false} axisLine={false} />
                        <YAxis
                          stroke="#64748b"
                          tickLine={false}
                          axisLine={false}
                          tickFormatter={(value) => `${formatNumber(Number(value) * 100, 0)}%`}
                        />
                        <Tooltip formatter={(value) => formatPercentLabel(Number(value))} />
                        <Legend wrapperStyle={{ fontSize: 12, paddingTop: 8, color: "#475569" }} iconSize={10} />
                        <Line type="monotone" dataKey="grossMargin" name="売上総利益率" stroke="#38bdf8" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="operatingMargin" name="営業利益率" stroke="#4ade80" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="netMargin" name="純利益率" stroke="#f59e0b" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="roe" name="ROE" stroke="#f87171" strokeWidth={2} strokeDasharray="6 4" />
                        <Line type="monotone" dataKey="roa" name="ROA" stroke="#c084fc" strokeWidth={2} strokeDasharray="6 4" />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </section>
              </>
            )}
          </div>
        )}
      </div>
    </ScreenPanel>
  );
}
