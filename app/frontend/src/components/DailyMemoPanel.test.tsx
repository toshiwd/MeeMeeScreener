// @vitest-environment jsdom
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import DailyMemoPanel from './DailyMemoPanel';

describe('DailyMemoPanel', () => {
  it('separates day info from the memo copy action', () => {
    const markup = renderToStaticMarkup(
      <DailyMemoPanel
        code="7203"
        selectedDate="2026-03-19"
        selectedBarData={{
          time: 1773878400,
          open: 100,
          high: 110,
          low: 95,
          close: 108,
          volume: 12345,
        }}
        maValues={{ ma7: 101, ma20: 99 }}
        maTrends={{ ma7: '上3 / 下0' }}
        position={{ buy: 2, sell: 1 }}
        prevDayData={{ close: 100, change: 8, changePercent: 8 }}
        cursorMode={true}
        onToggleCursorMode={() => {}}
        onPrevDay={() => {}}
        onNextDay={() => {}}
        onCopyForConsult={() => {}}
      />
    );

    expect(markup).toContain('日足情報');
    expect(markup).toContain('日付メモ (100字以内)');
    expect(markup).toContain('相談用にコピー');
    expect(markup).toContain('前日比');
    expect(markup).toContain('出来高');
  });
});
