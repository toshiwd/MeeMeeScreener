// @vitest-environment jsdom
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ThumbnailCanvas, { resolveThumbnailCrosshairLeft } from "./ThumbnailCanvas";
import {
  buildThumbnailSizeKey,
  buildThumbnailSnapshotCacheKey,
  clearThumbnailCache,
  getThumbnailCache
} from "./thumbnailCache";
import type { BarsPayload } from "../store";
import { installCanvasMock, type CanvasMockHandle } from "../test/canvasMock";
import { renderClient, type RenderClientHandle } from "../test/renderClient";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

type ResizeTrigger = (() => void) | null;

const makePayload = (): BarsPayload => ({
  bars: [
    [20260318, 100, 102, 98, 101, 10],
    [20260319, 101, 112, 99, 110, 100]
  ],
  ma: { ma7: [], ma20: [], ma60: [] },
  provenance: null
});

const buildRenderKey = (payload: BarsPayload) => {
  const bars = payload.bars ?? [];
  const last = bars[bars.length - 1];
  return `${bars.length}-${bars[0]?.[0]}-${last?.[0]}-${last?.[4]}-${last?.[5]}-0-none-false--60-true-dark`;
};

let currentWidth = 240;
let currentHeight = 120;
let resizeTrigger: ResizeTrigger = null;

describe("ThumbnailCanvas", () => {
  let render: RenderClientHandle | null = null;
  let canvasMock: CanvasMockHandle | null = null;

  beforeEach(async () => {
    currentWidth = 240;
    currentHeight = 120;
    resizeTrigger = null;
    clearThumbnailCache();
    vi.useFakeTimers();
    Object.defineProperty(window, "devicePixelRatio", {
      configurable: true,
      value: 2
    });

    Object.defineProperty(HTMLElement.prototype, "clientWidth", {
      configurable: true,
      get() {
        return this.classList?.contains("thumb-canvas") ? currentWidth : 0;
      }
    });
    Object.defineProperty(HTMLElement.prototype, "clientHeight", {
      configurable: true,
      get() {
        return this.classList?.contains("thumb-canvas") ? currentHeight : 0;
      }
    });

    class ResizeObserverMock {
      constructor(callback: ResizeObserverCallback) {
        resizeTrigger = () => callback([] as ResizeObserverEntry[], this as unknown as ResizeObserver);
      }
      observe = vi.fn();
      disconnect = vi.fn();
      unobserve = vi.fn();
    }

    vi.stubGlobal("ResizeObserver", ResizeObserverMock as unknown as typeof ResizeObserver);
    vi.stubGlobal(
      "requestAnimationFrame",
      ((callback: FrameRequestCallback) => window.setTimeout(() => callback(performance.now()), 0)) as typeof requestAnimationFrame
    );
    vi.stubGlobal("cancelAnimationFrame", ((handle: number) => window.clearTimeout(handle)) as typeof cancelAnimationFrame);

    canvasMock = installCanvasMock();
    render = await renderClient(
      <ThumbnailCanvas
        payload={makePayload()}
        boxes={[]}
        showBoxes={false}
        maSettings={[]}
        showAxes={true}
        theme="dark"
        cacheKey="thumbnail"
      />
    );
  });

  afterEach(() => {
    render?.cleanup();
    render = null;
    canvasMock?.restore();
    canvasMock = null;
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
  });

  const renderCanvas = (payload = makePayload()) => {
    act(() => {
      render?.root.render(
        <ThumbnailCanvas
          payload={payload}
          boxes={[]}
          showBoxes={false}
          maSettings={[]}
          showAxes={true}
          theme="dark"
          cacheKey="thumbnail"
        />
      );
    });
  };

  it("keeps the backing store aligned to the measured client size", async () => {
    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const canvas = render?.container.querySelector("canvas");
    expect(canvas).not.toBeNull();
    expect(canvas?.width).toBe(480);
    expect(canvas?.height).toBe(240);
    expect(canvas?.style.width).toBe("240px");
    expect(canvas?.style.height).toBe("120px");
    expect(canvasMock?.ctx.fillRect).toHaveBeenCalled();
  });

  it("skips drawing when the container size is zero", async () => {
    currentWidth = 0;
    currentHeight = 0;

    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const canvas = render?.container.querySelector("canvas");
    expect(canvas).not.toBeNull();
    expect(canvasMock?.ctx.fillRect).not.toHaveBeenCalled();
    expect(canvasMock?.ctx.clearRect).not.toHaveBeenCalled();
  });

  it("does not redraw again when the observed size stays unchanged", async () => {
    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const initialFillCount = canvasMock?.ctx.fillRect.mock.calls.length ?? 0;

    act(() => {
      resizeTrigger?.();
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(canvasMock?.ctx.fillRect.mock.calls.length).toBe(initialFillCount);
  });

  it("updates the internal size after a real resize", async () => {
    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    currentWidth = 300;
    currentHeight = 150;

    act(() => {
      resizeTrigger?.();
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const canvas = render?.container.querySelector("canvas");
    expect(canvas?.width).toBe(600);
    expect(canvas?.height).toBe(300);
    expect(canvas?.style.width).toBe("300px");
    expect(canvas?.style.height).toBe("150px");
    expect(canvasMock?.ctx.fillRect.mock.calls.length).toBeGreaterThan(0);
  });

  it("keeps cached snapshots separated by size so resize does not reuse a stale bitmap", async () => {
    const payload = makePayload();
    renderCanvas(payload);

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const renderKey = buildRenderKey(payload);
    const firstSizeKey = buildThumbnailSizeKey(240, 120, 2);
    expect(getThumbnailCache(buildThumbnailSnapshotCacheKey("thumbnail", renderKey, firstSizeKey))).toBe(
      "data:image/png;base64,480x240"
    );

    currentWidth = 300;
    currentHeight = 150;

    act(() => {
      resizeTrigger?.();
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const secondSizeKey = buildThumbnailSizeKey(300, 150, 2);
    expect(getThumbnailCache(buildThumbnailSnapshotCacheKey("thumbnail", renderKey, secondSizeKey))).toBe(
      "data:image/png;base64,600x300"
    );
    expect(getThumbnailCache(buildThumbnailSnapshotCacheKey("thumbnail", renderKey, firstSizeKey))).toBe(
      "data:image/png;base64,480x240"
    );
    expect(getThumbnailCache("thumbnail")).toBe("data:image/png;base64,600x300");
  });

  it("keeps drawing while scrolling but defers snapshot refresh until idle", async () => {
    const firstPayload = makePayload();
    renderCanvas(firstPayload);

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const initialFillCount = canvasMock?.ctx.fillRect.mock.calls.length ?? 0;
    const initialSnapshotCount = canvasMock?.toDataURLSpy.mock.calls.length ?? 0;
    const firstImg = render?.container.querySelector("img.thumb-canvas-image");
    const firstSnapshotSrc = firstImg?.getAttribute("src");
    expect(firstSnapshotSrc).toBe("data:image/png;base64,480x240");
    const nextPayload: BarsPayload = {
      bars: [
        [20260318, 100, 102, 98, 101, 10],
        [20260319, 101, 113, 99, 111, 120]
      ],
      ma: { ma7: [], ma20: [], ma60: [] },
      provenance: null
    };

    act(() => {
      window.dispatchEvent(new Event("scroll"));
    });

    currentWidth = 300;
    currentHeight = 150;
    renderCanvas(nextPayload);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(80);
    });

    expect(canvasMock?.ctx.fillRect.mock.calls.length).toBeGreaterThan(initialFillCount);
    expect(canvasMock?.toDataURLSpy.mock.calls.length).toBe(initialSnapshotCount);
    expect(render?.container.querySelector("img.thumb-canvas-image")).toBeNull();
    expect(render?.container.querySelector("canvas")?.style.opacity).not.toBe("0");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60);
      await vi.runAllTimersAsync();
    });

    expect(canvasMock?.ctx.fillRect.mock.calls.length).toBeGreaterThan(initialFillCount);
    expect(render?.container.querySelector("img.thumb-canvas-image")?.getAttribute("src")).not.toBe(firstSnapshotSrc);
  });

  it("aligns the hover crosshair to the candle center inside the plot area", async () => {
    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const thumb = render?.container.querySelector(".thumb-canvas") as HTMLDivElement | null;
    expect(thumb).not.toBeNull();
    vi.spyOn(thumb!, "getBoundingClientRect").mockReturnValue({
      x: 0,
      y: 0,
      width: 240,
      height: 120,
      top: 0,
      right: 240,
      bottom: 120,
      left: 0,
      toJSON: () => ({})
    } as DOMRect);

    act(() => {
      thumb?.dispatchEvent(new MouseEvent("mousemove", { bubbles: true, clientX: 145, clientY: 40 }));
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const crosshair = render?.container.querySelector(".thumb-crosshair") as HTMLDivElement | null;
    expect(crosshair).not.toBeNull();
    expect(crosshair?.style.left).toBe(resolveThumbnailCrosshairLeft(1, 240, 2, true));
  });
});
