// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ChartListCard from "./ChartListCard";
import { buildThumbnailCacheKey, clearThumbnailCache, setThumbnailCache } from "./thumbnailCache";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const maSettings = [] as const;
const thumbnailCanvasMock = vi.hoisted(() => ({
  render: vi.fn((props: Record<string, unknown>) => <div data-testid="thumbnail-canvas" />)
}));

vi.mock("./ThumbnailCanvas", () => ({
  default: (props: Record<string, unknown>) => thumbnailCanvasMock.render(props)
}));

describe("ChartListCard", () => {
  let root: ReturnType<typeof createRoot> | null = null;
  let container: HTMLDivElement | null = null;
  const onEnterView = vi.fn();
  const onOpenDetail = vi.fn();

  beforeEach(() => {
    clearThumbnailCache();
    document.documentElement.setAttribute("data-theme", "light");
    onEnterView.mockReset();
    onOpenDetail.mockReset();
    thumbnailCanvasMock.render.mockReset();

    class IntersectionObserverMock {
      constructor(
        private readonly callback: IntersectionObserverCallback
      ) {}

      observe = vi.fn((element: Element) => {
        this.callback(
          [
            {
              isIntersecting: true,
              target: element,
              intersectionRatio: 1,
              time: performance.now(),
              boundingClientRect: element.getBoundingClientRect(),
              intersectionRect: element.getBoundingClientRect(),
              rootBounds: null
            } as IntersectionObserverEntry
          ],
          this as unknown as IntersectionObserver
        );
      });

      disconnect = vi.fn();
      unobserve = vi.fn();
      takeRecords = vi.fn(() => []);
    }

    vi.stubGlobal(
      "IntersectionObserver",
      IntersectionObserverMock as unknown as typeof IntersectionObserver
    );

    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root?.unmount();
    });
    container?.remove();
    root = null;
    container = null;
    vi.unstubAllGlobals();
    clearThumbnailCache();
  });

  it("does not show a permanent loading label when idle and no chart payload exists", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="idle"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          thumbnailTimeframe="daily"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(container?.textContent).toContain("チャート未取得");
    expect(container?.textContent).not.toContain("読み込み中...");
  });

  it("shows loading only while the card chart request is active", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="loading"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          thumbnailTimeframe="daily"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(container?.textContent).toContain("読み込み中...");
  });

  it("shows a clear error state when the chart request fails", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="error"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          thumbnailTimeframe="daily"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(container?.textContent).toContain("読み込み失敗");
  });

  it("uses a cached thumbnail instead of an idle placeholder when no live payload exists", async () => {
    const cacheKey = buildThumbnailCacheKey(
      "7203",
      "daily",
      false,
      maSettings as unknown as never[],
      "light"
    );
    setThumbnailCache(cacheKey, "data:image/png;base64,ZmFrZQ==");

    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="idle"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          thumbnailTimeframe="daily"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(container?.querySelector(".thumb-canvas-image")).not.toBeNull();
    expect(container?.textContent).not.toContain("チャート未取得");
    expect(container?.textContent).not.toContain("読み込み中...");
  });

  it("re-triggers the visible fetch when the range changes", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="success"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          deferUntilInView={true}
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(onEnterView).toHaveBeenCalledTimes(1);
    expect(onEnterView).toHaveBeenLastCalledWith("7203");

    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={null}
          status="success"
          maSettings={maSettings as unknown as never[]}
          rangeBars={120}
          deferUntilInView={true}
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(onEnterView).toHaveBeenCalledTimes(2);
    expect(onEnterView).toHaveBeenLastCalledWith("7203");
  });

  it("keeps live payload bars even when maxDate is older", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={{
            bars: [
              [20260411, 100, 102, 98, 101, 10],
              [20260412, 101, 111, 100, 110, 20]
            ],
            ma: { ma7: [], ma20: [], ma60: [] }
          }}
          fallbackSeries={null}
          status="success"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          maxDate="2026-04-11"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(thumbnailCanvasMock.render).toHaveBeenCalled();
    const lastCall = thumbnailCanvasMock.render.mock.calls.at(-1);
    expect(lastCall?.[0]).toMatchObject({
      payload: {
        bars: [
          [20260411, 100, 102, 98, 101, 10],
          [20260412, 101, 111, 100, 110, 20]
        ]
      }
    });
  });

  it("still clips fallback series to maxDate", async () => {
    await act(async () => {
      root?.render(
        <ChartListCard
          code="7203"
          name="Toyota"
          payload={null}
          fallbackSeries={[
            [20260411, 100, 102, 98, 101, 10],
            [20260412, 101, 111, 100, 110, 20]
          ]}
          status="success"
          maSettings={maSettings as unknown as never[]}
          rangeBars={60}
          maxDate="2026-04-11"
          onEnterView={onEnterView}
          onOpenDetail={onOpenDetail}
        />
      );
    });

    expect(thumbnailCanvasMock.render).toHaveBeenCalled();
    const lastCall = thumbnailCanvasMock.render.mock.calls.at(-1);
    expect(lastCall?.[0]).toMatchObject({
      payload: {
        bars: [[20260411, 100, 102, 98, 101, 10]]
      }
    });
  });
});
