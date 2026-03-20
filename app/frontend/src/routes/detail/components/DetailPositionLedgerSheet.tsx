type LedgerViewMode = "iizuka" | "stock";

type LedgerIizukaRow = {
  date: string;
  kindLabel: string;
  deltaLong: number;
  deltaShort: number;
  longLots: number;
  shortLots: number;
  avgLongPrice: number | null;
  avgShortPrice: number | null;
  realizedDelta: number;
};

type LedgerStockRow = {
  date: string;
  kindLabel: string;
  qtyShares: number;
  deltaSellShares: number;
  deltaBuyShares: number;
  closeSellShares: number;
  closeBuyShares: number;
  buyAvgPrice: number | null;
  sellAvgPrice: number | null;
  realizedDelta: number;
};

type LedgerIizukaGroup = {
  brokerKey: string;
  brokerLabel: string;
  account: string;
  rows: LedgerIizukaRow[];
};

type LedgerStockGroup = {
  brokerKey: string;
  brokerLabel: string;
  account: string;
  rows: LedgerStockRow[];
};

type Props = {
  isOpen: boolean;
  expanded: boolean;
  ledgerViewMode: LedgerViewMode;
  ledgerEligible: boolean;
  ledgerIizukaGroups: LedgerIizukaGroup[];
  ledgerStockGroups: LedgerStockGroup[];
  onToggleExpanded: () => void;
  onClose: () => void;
  onChangeLedgerViewMode: (mode: LedgerViewMode) => void;
  formatLedgerDate: (value: string) => string;
  formatNumber: (value: number | null | undefined, digits?: number) => string;
  formatSignedNumber: (value: number | null | undefined, digits?: number) => string;
};

const formatLotValue = (value: number) => {
  if (!Number.isFinite(value)) return "0";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
};

const formatSignedLot = (value: number) => {
  if (value === 0) return "0";
  const sign = value > 0 ? "+" : "-";
  return `${sign}${formatLotValue(Math.abs(value))}`;
};

const formatShares = (shares: number | null | undefined) => {
  if (shares == null || !Number.isFinite(shares)) return "--";
  return shares.toLocaleString("ja-JP", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
};

const buildIizukaSummary = (group: LedgerIizukaGroup) => {
  const lastRow = group.rows[group.rows.length - 1] ?? null;
  if (!lastRow) return null;
  return {
    date: lastRow.date,
    status: `売玉 ${formatLotValue(lastRow.shortLots)} / 買玉 ${formatLotValue(lastRow.longLots)}`,
    pnl: formatSignedLot(lastRow.realizedDelta),
  };
};

const buildStockSummary = (group: LedgerStockGroup) => {
  const lastRow = group.rows[group.rows.length - 1] ?? null;
  if (!lastRow) return null;
  return {
    date: lastRow.date,
    status: `売株 ${formatShares(lastRow.closeSellShares)} / 買株 ${formatShares(lastRow.closeBuyShares)}`,
    pnl: formatSignedLot(lastRow.realizedDelta),
  };
};

const renderIizukaTable = (
  key: string,
  group: LedgerIizukaGroup,
  formatLedgerDate: Props["formatLedgerDate"],
  formatNumber: Props["formatNumber"],
  formatSignedNumber: Props["formatSignedNumber"]
) => (
  <div key={key} className={`position-ledger-group broker-${group.brokerKey}`}>
    <div className="position-ledger-group-header">
      <span className="broker-badge">{group.brokerLabel}</span>
      {group.account && <span className="position-ledger-account">{group.account}</span>}
    </div>
    <div className="position-ledger-table is-iizuka">
      <div className="position-ledger-row position-ledger-head">
        <span className="position-ledger-cell position-ledger-sticky-left" title="日付">
          日付
        </span>
        <span className="position-ledger-cell position-ledger-sticky-left second" title="取引種別">
          区分
        </span>
        <span className="position-ledger-cell align-right" title="売玉の増減">
          当日Δ（売玉）
        </span>
        <span className="position-ledger-cell align-right" title="買玉の増減">
          当日Δ（買玉）
        </span>
        <span className="position-ledger-cell align-right" title="当日引けの売玉">
          当日引け（売玉）
        </span>
        <span className="position-ledger-cell align-right" title="当日引けの買玉">
          当日引け（買玉）
        </span>
        <span className="position-ledger-cell align-right" title="建玉表記（売-買）">
          建玉表記
        </span>
        <span className="position-ledger-cell align-right" title="買い単価（玉）">
          買い単価
        </span>
        <span className="position-ledger-cell align-right" title="売り単価（玉）">
          売り単価
        </span>
        <span className="position-ledger-cell align-right" title="実現損益（返済・現渡などで確定した分）">
          損益（実現）
        </span>
      </div>
      {group.rows.map((row, index) => {
        const realizedClass =
          row.realizedDelta === 0
            ? "position-ledger-pnl"
            : row.realizedDelta > 0
              ? "position-ledger-pnl up"
              : "position-ledger-pnl down";
        return (
          <div
            className={`position-ledger-row ${index % 2 === 0 ? "is-even" : "is-odd"}`}
            key={`${row.date}-${index}`}
          >
            <span className="position-ledger-cell position-ledger-sticky-left">{formatLedgerDate(row.date)}</span>
            <span className="position-ledger-cell position-ledger-sticky-left second position-ledger-kind">
              {row.kindLabel}
            </span>
            <span className="position-ledger-cell align-right">{formatSignedLot(row.deltaShort)}</span>
            <span className="position-ledger-cell align-right">{formatSignedLot(row.deltaLong)}</span>
            <span className="position-ledger-cell align-right">{formatLotValue(row.shortLots)}</span>
            <span className="position-ledger-cell align-right">{formatLotValue(row.longLots)}</span>
            <span className="position-ledger-cell align-right">{`${formatLotValue(row.shortLots)}-${formatLotValue(row.longLots)}`}</span>
            <span className="position-ledger-cell align-right">
              {row.avgLongPrice != null ? formatNumber(row.avgLongPrice, 2) : "--"}
            </span>
            <span className="position-ledger-cell align-right">
              {row.avgShortPrice != null ? formatNumber(row.avgShortPrice, 2) : "--"}
            </span>
            <span className={`position-ledger-cell align-right ${realizedClass}`}>
              {formatSignedNumber(row.realizedDelta, 0)}
            </span>
          </div>
        );
      })}
    </div>
  </div>
);

const renderStockTable = (
  key: string,
  group: LedgerStockGroup,
  formatLedgerDate: Props["formatLedgerDate"],
  formatNumber: Props["formatNumber"],
  formatSignedNumber: Props["formatSignedNumber"]
) => (
  <div key={key} className={`position-ledger-group broker-${group.brokerKey}`}>
    <div className="position-ledger-group-header">
      <span className="broker-badge">{group.brokerLabel}</span>
      {group.account && <span className="position-ledger-account">{group.account}</span>}
    </div>
    <div className="position-ledger-table is-stock">
      <div className="position-ledger-row position-ledger-head">
        <span className="position-ledger-cell position-ledger-sticky-left" title="日付">
          日付
        </span>
        <span className="position-ledger-cell position-ledger-sticky-left second" title="取引種別">
          区分
        </span>
        <span className="position-ledger-cell align-right" title="約定数量（株）。100株=1玉。">
          数量（株）
        </span>
        <span className="position-ledger-cell align-right" title="売株の増減">
          当日Δ（売株）
        </span>
        <span className="position-ledger-cell align-right" title="買株の増減">
          当日Δ（買株）
        </span>
        <span className="position-ledger-cell align-right" title="当日引けの売株">
          当日引け（売株）
        </span>
        <span className="position-ledger-cell align-right" title="当日引けの買株">
          当日引け（買株）
        </span>
        <span className="position-ledger-cell align-right" title="買い単価（株）">
          買い単価
        </span>
        <span className="position-ledger-cell align-right" title="売り単価（株）">
          売り単価
        </span>
        <span className="position-ledger-cell align-right" title="実現損益（返済・現渡などで確定した分）">
          損益（実現）
        </span>
      </div>
      {group.rows.map((row, index) => {
        const realizedClass =
          row.realizedDelta === 0
            ? "position-ledger-pnl"
            : row.realizedDelta > 0
              ? "position-ledger-pnl up"
              : "position-ledger-pnl down";
        return (
          <div
            className={`position-ledger-row ${index % 2 === 0 ? "is-even" : "is-odd"}`}
            key={`${row.date}-${index}`}
          >
            <span className="position-ledger-cell position-ledger-sticky-left">{formatLedgerDate(row.date)}</span>
            <span className="position-ledger-cell position-ledger-sticky-left second position-ledger-kind">
              {row.kindLabel}
            </span>
            <span className="position-ledger-cell align-right">{formatShares(row.qtyShares)}</span>
            <span className="position-ledger-cell align-right">{formatSignedNumber(row.deltaSellShares, 0)}</span>
            <span className="position-ledger-cell align-right">{formatSignedNumber(row.deltaBuyShares, 0)}</span>
            <span className="position-ledger-cell align-right">{formatShares(row.closeSellShares)}</span>
            <span className="position-ledger-cell align-right">{formatShares(row.closeBuyShares)}</span>
            <span className="position-ledger-cell align-right">
              {row.buyAvgPrice != null ? formatNumber(row.buyAvgPrice, 2) : "--"}
            </span>
            <span className="position-ledger-cell align-right">
              {row.sellAvgPrice != null ? formatNumber(row.sellAvgPrice, 2) : "--"}
            </span>
            <span className={`position-ledger-cell align-right ${realizedClass}`}>
              {formatSignedNumber(row.realizedDelta, 0)}
            </span>
          </div>
        );
      })}
    </div>
  </div>
);

export default function DetailPositionLedgerSheet({
  isOpen,
  expanded,
  ledgerViewMode,
  ledgerEligible,
  ledgerIizukaGroups,
  ledgerStockGroups,
  onToggleExpanded,
  onClose,
  onChangeLedgerViewMode,
  formatLedgerDate,
  formatNumber,
  formatSignedNumber,
}: Props) {
  if (!isOpen) return null;

  const summaryGroups = ledgerViewMode === "iizuka" ? ledgerIizukaGroups : ledgerStockGroups;

  return (
    <div className={`position-ledger-sheet ${expanded ? "is-expanded" : "is-mini"}`}>
      <button
        type="button"
        className="position-ledger-handle"
        onClick={onToggleExpanded}
        aria-label={expanded ? "建玉推移を折りたたむ" : "建玉推移を展開する"}
      />
      <div className="position-ledger-header">
        <div className="position-ledger-header-main">
          <div>
            <div className="position-ledger-title">建玉推移（証券会社別）</div>
            <div className="position-ledger-sub">まず現在建玉を確認</div>
          </div>
          <div className="position-ledger-toggle" role="tablist" aria-label="表示モード">
            <span className="position-ledger-toggle-label">表示モード:</span>
            <button
              type="button"
              className={ledgerViewMode === "iizuka" ? "is-active" : ""}
              onClick={() => onChangeLedgerViewMode("iizuka")}
            >
              飯塚式（玉）
            </button>
            <button
              type="button"
              className={ledgerViewMode === "stock" ? "is-active" : ""}
              onClick={() => onChangeLedgerViewMode("stock")}
            >
              株式（株）
            </button>
          </div>
        </div>
        <button
          type="button"
          className="position-ledger-close"
          onClick={onClose}
          aria-label="建玉推移を閉じる"
        >
          x
        </button>
      </div>
      {!ledgerEligible ? (
        <div className="position-ledger-empty">建玉推移の対象データがありません。</div>
      ) : (
        <div className="position-ledger-group-list">
          <div className="position-ledger-summary">
            <div className="position-ledger-summary-title">現在建玉</div>
            <div className="position-ledger-summary-list">
              {summaryGroups.map((group) => {
                const summary =
                  ledgerViewMode === "iizuka"
                    ? buildIizukaSummary(group as LedgerIizukaGroup)
                    : buildStockSummary(group as LedgerStockGroup);
                if (!summary) return null;
                return (
                  <div key={`${group.brokerKey}-${group.account}`} className={`position-ledger-summary-item broker-${group.brokerKey}`}>
                    <span className="broker-badge">{group.brokerLabel}</span>
                    {group.account && <span className="position-ledger-account">{group.account}</span>}
                    <span className="position-ledger-summary-date">{formatLedgerDate(summary.date)}</span>
                    <span className="position-ledger-summary-status">{summary.status}</span>
                    <span className="position-ledger-summary-pnl">{summary.pnl}</span>
                  </div>
                );
              })}
            </div>
          </div>
          {expanded && (
            <>
              {ledgerViewMode === "iizuka"
                ? ledgerIizukaGroups.map((group) =>
                    renderIizukaTable(`${group.brokerKey}-${group.account}`, group, formatLedgerDate, formatNumber, formatSignedNumber)
                  )
                : ledgerStockGroups.map((group) =>
                    renderStockTable(`${group.brokerKey}-${group.account}`, group, formatLedgerDate, formatNumber, formatSignedNumber)
                  )}
            </>
          )}
        </div>
      )}
    </div>
  );
}
