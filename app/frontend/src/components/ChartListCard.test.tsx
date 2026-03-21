// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ChartListCard from "./ChartListCard";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const maSettings = [] as const;

describe("ChartListCard", () => {
  let root: ReturnType<typeof createRoot> | null = null;
  let container: HTMLDivElement | null = null;
  const onEnterView = vi.fn();
  const onOpenDetail = vi.fn();

  beforeEach(() => {
    onEnterView.mockReset();
    onOpenDetail.mockReset();

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
});
