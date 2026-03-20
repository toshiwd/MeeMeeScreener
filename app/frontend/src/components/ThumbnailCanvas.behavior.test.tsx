// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ThumbnailCanvas from "./ThumbnailCanvas";
import {
  buildThumbnailSizeKey,
  buildThumbnailSnapshotCacheKey,
  clearThumbnailCache,
  getThumbnailCache
} from "./thumbnailCache";
import type { BarsPayload } from "../store";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

type CanvasContextStub = {
  clearRect: ReturnType<typeof vi.fn>;
  beginPath: ReturnType<typeof vi.fn>;
  moveTo: ReturnType<typeof vi.fn>;
  lineTo: ReturnType<typeof vi.fn>;
  stroke: ReturnType<typeof vi.fn>;
  fillRect: ReturnType<typeof vi.fn>;
  strokeRect: ReturnType<typeof vi.fn>;
  fillText: ReturnType<typeof vi.fn>;
  save: ReturnType<typeof vi.fn>;
  restore: ReturnType<typeof vi.fn>;
  setTransform: ReturnType<typeof vi.fn>;
  lineWidth: number;
  font: string;
  textAlign: CanvasTextAlign;
  textBaseline: CanvasTextBaseline;
  strokeStyle: string;
  fillStyle: string;
};

type ResizeTrigger = (() => void) | null;

const makePayload = (): BarsPayload => ({
  bars: [
    [20260318, 100, 102, 98, 101, 10],
    [20260319, 101, 112, 99, 110, 100]
  ],
  ma: { ma7: [], ma20: [], ma60: [] }
});

const buildRenderKey = (payload: BarsPayload) => {
  const bars = payload.bars ?? [];
  const last = bars[bars.length - 1];
  return `${bars.length}-${bars[0]?.[0]}-${last?.[0]}-${last?.[4]}-${last?.[5]}-0-none-false--60-true-dark`;
};

let currentWidth = 240;
let currentHeight = 120;
let resizeTrigger: ResizeTrigger = null;

const createContextStub = () => {
  const ctx: CanvasContextStub = {
    clearRect: vi.fn(),
    beginPath: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    stroke: vi.fn(),
    fillRect: vi.fn(),
    strokeRect: vi.fn(),
    fillText: vi.fn(),
    save: vi.fn(),
    restore: vi.fn(),
    setTransform: vi.fn(),
    lineWidth: 1,
    font: "",
    textAlign: "left",
    textBaseline: "alphabetic",
    strokeStyle: "#000",
    fillStyle: "#000"
  };
  return ctx;
};

describe("ThumbnailCanvas", () => {
  let root: ReturnType<typeof createRoot> | null = null;
  let container: HTMLDivElement | null = null;
  let ctx: CanvasContextStub;
  let getContextSpy: ReturnType<typeof vi.spyOn>;
  let toDataURLSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
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
    vi.stubGlobal("requestAnimationFrame", ((callback: FrameRequestCallback) =>
      window.setTimeout(() => callback(performance.now()), 0)) as typeof requestAnimationFrame);
    vi.stubGlobal("cancelAnimationFrame", ((handle: number) => window.clearTimeout(handle)) as typeof cancelAnimationFrame);

    ctx = createContextStub();
    getContextSpy = vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(() => ctx as never);
    toDataURLSpy = vi.spyOn(HTMLCanvasElement.prototype, "toDataURL").mockImplementation(function (this: HTMLCanvasElement) {
      return `data:image/png;base64,${this.width}x${this.height}`;
    });

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
    getContextSpy.mockRestore();
    toDataURLSpy.mockRestore();
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
  });

  const renderCanvas = (payload = makePayload()) => {
    act(() => {
      root?.render(
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

    const canvas = container?.querySelector("canvas");
    expect(canvas).not.toBeNull();
    expect(canvas?.width).toBe(480);
    expect(canvas?.height).toBe(240);
    expect(canvas?.style.width).toBe("240px");
    expect(canvas?.style.height).toBe("120px");
    expect(ctx.fillRect).toHaveBeenCalled();
  });

  it("skips drawing when the container size is zero", async () => {
    currentWidth = 0;
    currentHeight = 0;

    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const canvas = container?.querySelector("canvas");
    expect(canvas).not.toBeNull();
    expect(ctx.fillRect).not.toHaveBeenCalled();
    expect(ctx.clearRect).not.toHaveBeenCalled();
  });

  it("does not redraw again when the observed size stays unchanged", async () => {
    renderCanvas();

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const initialFillCount = ctx.fillRect.mock.calls.length;

    act(() => {
      resizeTrigger?.();
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(ctx.fillRect.mock.calls.length).toBe(initialFillCount);
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

    const canvas = container?.querySelector("canvas");
    expect(canvas?.width).toBe(600);
    expect(canvas?.height).toBe(300);
    expect(canvas?.style.width).toBe("300px");
    expect(canvas?.style.height).toBe("150px");
    expect(ctx.fillRect.mock.calls.length).toBeGreaterThan(0);
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

  it("defers redraw while scrolling and redraws after idle", async () => {
    const firstPayload = makePayload();
    renderCanvas(firstPayload);

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    const initialFillCount = ctx.fillRect.mock.calls.length;
    const initialSnapshotCount = toDataURLSpy.mock.calls.length;
    const firstImg = container?.querySelector("img.thumb-canvas-image");
    const firstSnapshotSrc = firstImg?.getAttribute("src");
    expect(firstSnapshotSrc).toBe("data:image/png;base64,480x240");
    const nextPayload: BarsPayload = {
      bars: [
        [20260318, 100, 102, 98, 101, 10],
        [20260319, 101, 113, 99, 111, 120]
      ],
      ma: { ma7: [], ma20: [], ma60: [] }
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

    expect(ctx.fillRect.mock.calls.length).toBe(initialFillCount);
    expect(toDataURLSpy.mock.calls.length).toBe(initialSnapshotCount);
    expect(container?.querySelector("img.thumb-canvas-image")?.getAttribute("src")).toBe(firstSnapshotSrc);
    expect(container?.querySelector("canvas")?.style.opacity).toBe("0");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60);
      await vi.runAllTimersAsync();
    });

    expect(ctx.fillRect.mock.calls.length).toBeGreaterThan(initialFillCount);
    expect(container?.querySelector("img.thumb-canvas-image")?.getAttribute("src")).not.toBe(firstSnapshotSrc);
  });
});
