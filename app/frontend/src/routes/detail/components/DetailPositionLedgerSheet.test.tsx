import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import DetailPositionLedgerSheet from './DetailPositionLedgerSheet';

describe('DetailPositionLedgerSheet', () => {
  const formatLedgerDate = (value: string) => `L${value}`;
  const formatNumber = (value: number | null | undefined, digits = 0) =>
    value == null ? '--' : value.toFixed(digits);
  const formatSignedNumber = (value: number | null | undefined, digits = 0) =>
    value == null ? '--' : `${value >= 0 ? '+' : ''}${value.toFixed(digits)}`;

  it('shows a compact summary before expanding the table', () => {
    const miniMarkup = renderToStaticMarkup(
      <DetailPositionLedgerSheet
        isOpen={true}
        expanded={false}
        ledgerViewMode="iizuka"
        ledgerEligible={true}
        ledgerIizukaGroups={[
          {
            brokerKey: 'sbi',
            brokerLabel: 'SBI',
            account: 'A1',
            rows: [
              {
                date: '2026-03-18',
                kindLabel: 'new',
                deltaLong: 1,
                deltaShort: 0,
                longLots: 3,
                shortLots: 1,
                avgLongPrice: 100,
                avgShortPrice: 101,
                realizedDelta: 50,
              },
            ],
          },
        ]}
        ledgerStockGroups={[]}
        onToggleExpanded={() => {}}
        onClose={() => {}}
        onChangeLedgerViewMode={() => {}}
        formatLedgerDate={formatLedgerDate}
        formatNumber={formatNumber}
        formatSignedNumber={formatSignedNumber}
      />
    );

    expect(miniMarkup).toContain('現在建玉');
    expect(miniMarkup).toContain('L2026-03-18');
    expect(miniMarkup).not.toContain('当日Δ（売玉）');

    const expandedMarkup = renderToStaticMarkup(
      <DetailPositionLedgerSheet
        isOpen={true}
        expanded={true}
        ledgerViewMode="stock"
        ledgerEligible={true}
        ledgerIizukaGroups={[]}
        ledgerStockGroups={[
          {
            brokerKey: 'rakuten',
            brokerLabel: 'RAKUTEN',
            account: 'B1',
            rows: [
              {
                date: '2026-03-18',
                kindLabel: 'buy',
                qtyShares: 200,
                deltaSellShares: 0,
                deltaBuyShares: 100,
                closeSellShares: 50,
                closeBuyShares: 150,
                buyAvgPrice: 100,
                sellAvgPrice: 101,
                realizedDelta: 75,
              },
            ],
          },
        ]}
        onToggleExpanded={() => {}}
        onClose={() => {}}
        onChangeLedgerViewMode={() => {}}
        formatLedgerDate={formatLedgerDate}
        formatNumber={formatNumber}
        formatSignedNumber={formatSignedNumber}
      />
    );

    expect(expandedMarkup).toContain('現在建玉');
    expect(expandedMarkup).toContain('当日Δ（売株）');
    expect(expandedMarkup).toContain('数量（株）');
  });
});
