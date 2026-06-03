// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import DailyMemoPanel from "./DailyMemoPanel";

const { getMock, putMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  putMock: vi.fn(),
}));

vi.mock("../api", () => ({
  api: {
    get: getMock,
    put: putMock,
  },
}));

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

afterEach(() => {
  getMock.mockReset();
  putMock.mockReset();
});

describe("DailyMemoPanel", () => {
  it("separates day info from the memo copy action", () => {
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
        maTrends={{ ma7: "上3 / 下0" }}
        position={{ buy: 2, sell: 1 }}
        prevDayData={{ close: 100, change: 8, changePercent: 8 }}
        cursorMode={true}
        onToggleCursorMode={() => {}}
        onPrevDay={() => {}}
        onNextDay={() => {}}
        onCopyForConsult={() => {}}
      />
    );

    expect(markup).toContain("日足情報");
    expect(markup).toContain("日付メモ (100文字以内)");
    expect(markup).toContain("相談用にコピー");
    expect(markup).toContain("前日比");
    expect(markup).toContain("出来高");
  });

  it("renders a compact annotation-rail summary with price and position labels", () => {
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
        position={{ buy: 2, sell: 1 }}
        title="選択日の情報"
        compact
        cursorMode={true}
        onPrevDay={() => {}}
        onNextDay={() => {}}
        onCopyForConsult={() => {}}
      />
    );

    expect(markup).toContain("daily-memo-panel is-compact");
    expect(markup).toContain("選択日の情報");
    expect(markup).toContain('aria-label="選択日の価格情報"');
    expect(markup).toContain('aria-label="選択日の建玉"');
  });

  it("hides day info when cursor mode is off", () => {
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
        cursorMode={false}
        onToggleCursorMode={() => {}}
        onPrevDay={() => {}}
        onNextDay={() => {}}
        onCopyForConsult={() => {}}
      />
    );

    expect(markup).toContain("日付選択をONにすると日足情報を表示します");
    expect(markup).not.toContain("前日比");
    expect(markup).not.toContain("出来高");
  });

  it("ignores stale memo responses after the selected date changes", async () => {
    let resolveFirst: ((value: { data: { memo: string; updated_at: string | null } }) => void) | null = null;
    let resolveSecond: ((value: { data: { memo: string; updated_at: string | null } }) => void) | null = null;

    getMock
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirst = resolve;
          })
      )
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveSecond = resolve;
          })
      );

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(
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
          cursorMode={true}
          onPrevDay={() => {}}
          onNextDay={() => {}}
          onCopyForConsult={() => {}}
        />
      );
    });

    await act(async () => {
      root.render(
        <DailyMemoPanel
          code="7203"
          selectedDate="2026-03-20"
          selectedBarData={{
            time: 1773964800,
            open: 102,
            high: 112,
            low: 96,
            close: 109,
            volume: 12345,
          }}
          cursorMode={true}
          onPrevDay={() => {}}
          onNextDay={() => {}}
          onCopyForConsult={() => {}}
        />
      );
    });

    await act(async () => {
      resolveFirst?.({ data: { memo: "old memo", updated_at: "2026-03-19T00:00:00Z" } });
      await Promise.resolve();
    });

    expect(container.textContent).not.toContain("old memo");

    await act(async () => {
      resolveSecond?.({ data: { memo: "new memo", updated_at: "2026-03-20T00:00:00Z" } });
      await Promise.resolve();
    });

    expect(container.textContent).toContain("new memo");
    expect(container.textContent).not.toContain("old memo");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
