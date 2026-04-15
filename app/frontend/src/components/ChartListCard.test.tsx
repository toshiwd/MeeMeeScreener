// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ChartListCard from "./ChartListCard";

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
