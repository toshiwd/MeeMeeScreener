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

import type { EdinetFinancialPanel, EdinetFinancialPoint } from "./detailTypes";

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatFinancialAmountLabel = (value: number | null | undefined) => string;

type FinancialCard = {
  readonly label: string;
  readonly value: string;
  readonly tone: "up" | "down" | "neutral";
};

export type Props = {
  financialPanelRef: Ref<HTMLDivElement>;
  financialPanel: EdinetFinancialPanel | null;
  financialFetchedLabel: string | null;
  financialLoading: boolean;
  financialSeries: EdinetFinancialPoint[];
  financialCards: readonly FinancialCard[];
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
    formatNumber,
    formatPercentLabel,
    formatFinancialAmountLabel,
  } = props;

  return (
    <div ref={financialPanelRef} className="daily-memo-panel detail-analysis-panel detail-financial-panel">
      <div className="memo-panel-header">
        <h3>EDINET財務</h3>
      </div>
      <div className="detail-analysis-body">
        {(financialPanel?.summary?.latestFiscalYear != null || financialFetchedLabel) && (
          <div className="detail-financial-meta-row">
            {financialPanel?.summary?.latestFiscalYear != null && (
              <div className="detail-financial-meta-pill">最新年度 {financialPanel.summary.latestFiscalYear}</div>
            )}
            {financialFetchedLabel && (
              <div className="detail-financial-meta-pill">取得日 {financialFetchedLabel}</div>
            )}
          </div>
        )}
        {financialLoading ? (
          <div className="detail-analysis-empty">財務データを読込中です。</div>
        ) : financialSeries.length > 0 ? (
          <>
            <div className="detail-financial-card-grid">
              {financialCards.map((card) => (
                <div key={card.label} className="detail-financial-card">
                  <div className="detail-financial-card-label">{card.label}</div>
                  <div className={`detail-financial-card-value detail-analysis-value--${card.tone}`}>{card.value}</div>
                </div>
              ))}
            </div>
            <div className="detail-analysis-section detail-financial-section">
              <div className="detail-analysis-section-title">売上・利益の推移</div>
              <div className="detail-financial-chart">
                <ResponsiveContainer width="100%" height={256}>
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
            </div>
            <div className="detail-analysis-section detail-financial-section">
              <div className="detail-analysis-section-title">利益率・ROEの推移</div>
              <div className="detail-financial-chart">
                <ResponsiveContainer width="100%" height={256}>
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
                    <Line type="monotone" dataKey="grossMargin" name="粗利率" stroke="#38bdf8" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="operatingMargin" name="営業利益率" stroke="#4ade80" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="netMargin" name="純利益率" stroke="#f59e0b" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="roe" name="ROE" stroke="#f87171" strokeWidth={2} strokeDasharray="6 4" />
                    <Line type="monotone" dataKey="roa" name="ROA" stroke="#c084fc" strokeWidth={2} strokeDasharray="6 4" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </>
        ) : (
          <div className="detail-analysis-empty">
            {financialPanel?.mapped === false ? "EDINETコード未対応です。" : "財務データがありません。"}
          </div>
        )}
      </div>
    </div>
  );
}
