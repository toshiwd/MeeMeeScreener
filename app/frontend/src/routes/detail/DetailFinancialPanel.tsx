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
  const hasTdnetSection = tdnetLoading || tdnetHighlights.length > 0;
  const hasTaisyakuSection =
    taisyakuLoading ||
    taisyakuCards.length > 0 ||
    taisyakuHistory.length > 0 ||
    taisyakuRestrictions.length > 0;

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
        {(financialPanel?.summary?.latestFiscalYear != null || financialFetchedLabel) && (
          <div className="detail-financial-meta-row">
            {financialPanel?.summary?.latestFiscalYear != null && (
              <div className="detail-financial-meta-pill">最新年度 {financialPanel.summary.latestFiscalYear}</div>
            )}
            {financialFetchedLabel && <div className="detail-financial-meta-pill">取得 {financialFetchedLabel}</div>}
          </div>
        )}

        {financialLoading ? (
          <div className="detail-analysis-empty">財務データを取得中です。</div>
        ) : !hasPrimaryKpi && !hasFinancialSeries ? (
          <div className="detail-analysis-empty">
            {financialPanel?.mapped === false ? "EDINETのマッピングがありません。" : "財務データがありません。"}
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
                          <div className={`detail-financial-card-value detail-analysis-value--${card.tone}`}>
                            {card.value}
                          </div>
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
                          <div className={`detail-financial-stat-value detail-analysis-value--${item.tone}`}>
                            {item.value}
                          </div>
                        </div>
                      ))}
                    </div>
                  </>
                )}
              </section>
            )}

            {hasTdnetSection && (
              <section className="detail-analysis-section detail-financial-section">
                <div className="detail-analysis-section-title">TDNET動向</div>
                {tdnetStatusLabel && <div className="detail-analysis-meta">{tdnetStatusLabel}</div>}
                {tdnetLoading ? (
                  <div className="detail-analysis-empty">TDNETデータを取得中です。</div>
                ) : tdnetHighlights.length > 0 ? (
                  <div className="detail-financial-tdnet-list">
                    {tdnetHighlights.map((item) => (
                      <div key={item.disclosureId} className="detail-financial-tdnet-item">
                        <div className="detail-financial-tdnet-head">
                          <div className={`detail-financial-tdnet-title detail-analysis-value--${item.tone}`}>
                            {item.title}
                          </div>
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
                  <div className="detail-analysis-empty">TDNET動向はありません。</div>
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
                          <div className={`detail-financial-card-value detail-analysis-value--${card.tone}`}>
                            {card.value}
                          </div>
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
                            <span className="detail-financial-tdnet-pill">{item.measureType ?? "規制"}</span>
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
                          <span>融資/逆日歩</span>
                          <span>株数</span>
                          <span>貸株料</span>
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
                  <div className="detail-analysis-section-title">売上・利益</div>
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
