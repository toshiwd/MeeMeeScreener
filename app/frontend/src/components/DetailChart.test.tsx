// @ts-nocheck
import React from "react";
import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import DetailChart from "./DetailChart";
import { renderClient } from "../test/renderClient";
import { installCanvasMock } from "../test/canvasMock";

const chartMocks: any[] = [];

vi.mock("lightweight-charts", () => {
  const createSeries = (kind: string, options: Record<string, unknown>) => {
    const series = {
      kind,
      options: { ...options },
      data: [] as Array<Record<string, unknown>>,
      setData: vi.fn((next: Array<Record<string, unknown>>) => {
        series.data = next;
      }),
      applyOptions: vi.fn((next: Record<string, unknown>) => {
        series.options = { ...series.options, ...next };
      }),
      priceToCoordinate: vi.fn((price: number) => price),
      coordinateToPrice: vi.fn((coord: number) => coord)
    };
    return series;
  };

  const createTimeScale = (chart: any) => {
    let visibleRange: { from: number; to: number } | null = null;
    const logicalHandlers = new Set<() => void>();
    const timeHandlers = new Set<() => void>();
    const scale = {
      fitContent: vi.fn(() => {
        if (!visibleRange && chart.__defaultRange) {
          visibleRange = chart.__defaultRange;
        }
        logicalHandlers.forEach((handler) => handler());
        timeHandlers.forEach((handler) => handler());
      }),
      setVisibleRange: vi.fn((range: { from: number; to: number }) => {
        visibleRange = range;
        logicalHandlers.forEach((handler) => handler());
        timeHandlers.forEach((handler) => handler());
      }),
      getVisibleRange: vi.fn(() => visibleRange),
      subscribeVisibleLogicalRangeChange: vi.fn((handler: () => void) => {
        logicalHandlers.add(handler);
      }),
      unsubscribeVisibleLogicalRangeChange: vi.fn((handler: () => void) => {
        logicalHandlers.delete(handler);
      }),
      subscribeVisibleTimeRangeChange: vi.fn((handler: () => void) => {
        timeHandlers.add(handler);
      }),
      unsubscribeVisibleTimeRangeChange: vi.fn((handler: () => void) => {
        timeHandlers.delete(handler);
      }),
      timeToCoordinate: vi.fn((time: number) => time),
      coordinateToTime: vi.fn(() => visibleRange?.to ?? null)
    };
    return scale;
  };

  const createPriceScale = () => ({
    applyOptions: vi.fn(),
    subscribeVisibleLogicalRangeChange: vi.fn(),
    unsubscribeVisibleLogicalRangeChange: vi.fn(),
    width: vi.fn(() => 80),
    coordinateToPrice: vi.fn((coord: number) => coord),
    priceToCoordinate: vi.fn((price: number) => price)
  });

  const createChart = vi.fn((_element: HTMLElement, options: Record<string, unknown>) => {
    const chart: any = {
      options: { ...options },
      __defaultRange: null,
      applyOptions: vi.fn((next: Record<string, unknown>) => {
        chart.options = {
          ...chart.options,
          ...next,
          rightPriceScale: {
            ...chart.options.rightPriceScale,
            ...(next.rightPriceScale ?? {})
          }
        };
      }),
      remove: vi.fn(),
      subscribeCrosshairMove: vi.fn((handler: (param: unknown) => void) => {
        chart.crosshairHandler = handler;
      }),
      unsubscribeCrosshairMove: vi.fn(),
      setCrosshairPosition: vi.fn(),
      clearCrosshairPosition: vi.fn(),
      addCandlestickSeries: vi.fn((options: Record<string, unknown>) => {
        chart.candlestickSeries = createSeries("candlestick", options);
        return chart.candlestickSeries;
      }),
      addHistogramSeries: vi.fn((options: Record<string, unknown>) => {
        chart.histogramSeries = createSeries("histogram", options);
        return chart.histogramSeries;
      }),
      addLineSeries: vi.fn((options: Record<string, unknown>) => {
        const series = createSeries("line", options);
        chart.lineSeries.push(series);
        return series;
      }),
      removeSeries: vi.fn(),
      priceScale: vi.fn((id: string) => {
        if (!chart.priceScales[id]) {
          chart.priceScales[id] = createPriceScale();
        }
        return chart.priceScales[id];
      }),
      timeScale: vi.fn(() => chart.timeScaleApi),
      lineSeries: [] as Array<ReturnType<typeof createSeries>>,
      priceScales: {} as Record<string, ReturnType<typeof createPriceScale>>,
      timeScaleApi: null as any
    };
    chart.timeScaleApi = createTimeScale(chart);
    chartMocks.push(chart);
    return chart;
  });

  return {
    __esModule: true,
    CrosshairMode: { Normal: 0 },
    createChart,
  };
});

const installDomMetrics = () => {
  Object.defineProperty(HTMLElement.prototype, "clientWidth", {
    configurable: true,
    get() {
      return 960;
    }
  });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get() {
      return 420;
    }
  });
  Object.defineProperty(HTMLElement.prototype, "getBoundingClientRect", {
    configurable: true,
    value() {
      return {
        x: 0,
        y: 0,
        top: 0,
        left: 0,
        right: 960,
        bottom: 420,
        width: 960,
        height: 420,
        toJSON() {
          return {};
        }
      };
    }
  });
};

describe("DetailChart MeeMee chrome", () => {
  let canvasMock: ReturnType<typeof installCanvasMock> | null = null;
  let rafSpy: ReturnType<typeof vi.spyOn> | null = null;
  let roMock: any = null;

  beforeEach(() => {
    chartMocks.length = 0;
    installDomMetrics();
    canvasMock = installCanvasMock();
    rafSpy = vi.spyOn(window, "requestAnimationFrame").mockImplementation((cb) => {
      cb(0);
      return 1;
    });
    vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => undefined);
    roMock = class ResizeObserverMock {
      callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
      }
      observe(target: Element) {
        this.callback(
          [
            {
              target,
              contentRect: {
                width: 960,
                height: 420,
                x: 0,
                y: 0,
                top: 0,
                left: 0,
                right: 960,
                bottom: 420,
                toJSON() {
                  return {};
                }
              }
            } as ResizeObserverEntry
          ],
          this as unknown as ResizeObserver
        );
      }
      unobserve() {}
      disconnect() {}
    };
    globalThis.ResizeObserver = roMock as unknown as typeof ResizeObserver;
  });

  afterEach(() => {
    canvasMock?.restore();
    rafSpy?.mockRestore();
    vi.restoreAllMocks();
  });

  const baseCandles = [
    { time: Date.UTC(2026, 3, 6) / 1000, open: 100, high: 110, low: 95, close: 108 },
    { time: Date.UTC(2026, 3, 13) / 1000, open: 108, high: 112, low: 104, close: 106 },
    { time: Date.UTC(2026, 3, 20) / 1000, open: 106, high: 114, low: 103, close: 111 }
  ];
  const baseVolume = baseCandles.map((candle, index) => ({ time: candle.time, value: 1000 + index * 100 }));
  const baseMaLines = [
    {
      key: "ma5",
      label: "MA5",
      period: 5,
      color: "#ff0000",
      data: baseCandles.map((candle) => ({ time: candle.time, value: candle.close - 1 })),
      visible: true,
      lineWidth: 1
    },
    {
      key: "ma20",
      label: "MA20",
      period: 20,
      color: "#00aa00",
      data: baseCandles.map((candle) => ({ time: candle.time, value: candle.close - 4 })),
      visible: true,
      lineWidth: 1
    }
  ];

  it("keeps the shared chart default behavior unchanged when MeeMee chrome is off", async () => {
    const render = await renderClient(
      <DetailChart
        candles={baseCandles}
        volume={baseVolume}
        maLines={baseMaLines}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const chart = chartMocks[0];
    expect(chart.options.rightPriceScale.borderVisible).toBe(false);
    expect(chart.lineSeries[0].options.lastValueVisible).toBe(true);
    expect(render.container.querySelector("[data-testid='detail-chart-date-chip']")).toBeNull();
    expect(render.container.querySelector("[data-testid='detail-chart-legend']")).toBeNull();

    render.cleanup();
  });

  it("enables MeeMee detail chrome with hidden MA edge labels and no date chip overlay", async () => {
    const weeklyChipTime = Date.UTC(2026, 3, 6) / 1000;
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: weeklyChipTime, open: 100, high: 110, low: 95, close: 108 }
        ]}
        volume={[{ time: weeklyChipTime, value: 1000 }]}
        maLines={baseMaLines}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        meeMeeDetailChrome={{ timeframe: "weekly" }}
        meeMeeDetailChromeTerminalDates={{
          [weeklyChipTime]: Date.UTC(2026, 3, 9) / 1000
        }}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const chart = chartMocks[0];
    expect(chart.options.rightPriceScale.borderVisible).toBe(true);
    expect(chart.options.rightPriceScale.minimumWidth).toBeGreaterThanOrEqual(72);
    expect(chart.options.timeScale.rightOffset).toBe(0);
    expect(chart.options.timeScale.fixRightEdge).toBe(true);
    expect(chart.options.timeScale.lockVisibleTimeRangeOnResize).toBe(true);
    expect(chart.options.timeScale.rightBarStaysOnScroll).toBe(true);
    expect(chart.lineSeries.every((series: any) => series.options.lastValueVisible === false)).toBe(true);

    const chip = render.container.querySelector("[data-testid='detail-chart-date-chip']");
    const legend = render.container.querySelector("[data-testid='detail-chart-legend']");
    expect(chip).toBeNull();
    expect(legend).not.toBeNull();
    expect(legend?.textContent).toContain("日付");
    expect(legend?.textContent).toContain("終値");
    expect(legend?.textContent).toContain("5MA");

    render.cleanup();
  });

  it("draws ranking buy markers near the candle", async () => {
    const signalCandles = [
      { time: 120, open: 100, high: 112, low: 96, close: 108 },
      { time: 160, open: 108, high: 116, low: 104, close: 114 }
    ];
    const markerTime = signalCandles[1].time;
    const render = await renderClient(
      <DetailChart
        candles={signalCandles}
        volume={signalCandles.map((candle) => ({ time: candle.time, value: 1000 }))}
        maLines={[]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        eventMarkers={[{ time: markerTime, kind: "ranking-up", label: "買い" }]}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(canvasMock?.ctx.fillText).toHaveBeenCalledWith("買い", expect.any(Number), expect.any(Number));
    expect(canvasMock?.ctx.fill).toHaveBeenCalled();
    expect(canvasMock?.ctx.closePath).toHaveBeenCalled();

    render.cleanup();
  });

  it("keeps dense ranking markers as signal labels without rank counts", async () => {
    const signalCandles = [
      { time: 120, open: 100, high: 112, low: 96, close: 108 },
      { time: 160, open: 108, high: 116, low: 104, close: 114 },
      { time: 200, open: 114, high: 120, low: 112, close: 118 }
    ];
    const markerTime = signalCandles[0].time;
    const render = await renderClient(
      <DetailChart
        candles={signalCandles}
        volume={signalCandles.map((candle) => ({ time: candle.time, value: 1000 }))}
        maLines={[]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        eventMarkers={[
          { time: markerTime, kind: "ranking-up", label: "買い" },
          { time: markerTime + 1, kind: "ranking-up", label: "買い" },
          { time: markerTime + 2, kind: "ranking-up", label: "買い" },
        ]}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const labels = canvasMock?.ctx.fillText.mock.calls.map((call) => String(call[0])) ?? [];
    expect(labels).toContain("買い");
    expect(labels.some((label) => label.includes("位") || label.includes("件"))).toBe(false);

    render.cleanup();
  });

  it("suppresses the compact legend when positionOverlay is active", async () => {
    const monthlyChipTime = Date.UTC(2026, 3, 1) / 1000;
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: monthlyChipTime, open: 100, high: 110, low: 95, close: 108 }
        ]}
        volume={[{ time: monthlyChipTime, value: 1000 }]}
        maLines={baseMaLines}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        meeMeeDetailChrome={{ timeframe: "monthly" }}
        meeMeeDetailChromeTerminalDates={{
          [monthlyChipTime]: Date.UTC(2026, 3, 18) / 1000
        }}
        positionOverlay={{
          dailyPositions: [],
          tradeMarkers: [],
          showOverlay: false,
          showPnL: false,
          hoverTime: null
        }}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const chip = render.container.querySelector("[data-testid='detail-chart-date-chip']");
    const legend = render.container.querySelector("[data-testid='detail-chart-legend']");
    expect(chip).toBeNull();
    expect(legend).toBeNull();

    render.cleanup();
  });

  it("keeps the chart pinned to the latest bar while hiding the extra date chip", async () => {
    const chartRef = React.createRef<any>();
    const firstWeekTime = Date.UTC(2026, 3, 6) / 1000;
    const secondWeekTime = Date.UTC(2026, 3, 13) / 1000;
    const render = await renderClient(
      <DetailChart
        ref={chartRef}
        candles={[
          { time: firstWeekTime, open: 100, high: 110, low: 95, close: 108 },
          { time: secondWeekTime, open: 108, high: 112, low: 104, close: 106 }
        ]}
        volume={[
          { time: firstWeekTime, value: 1000 },
          { time: secondWeekTime, value: 1100 }
        ]}
        maLines={[
          {
            key: "ma5",
            label: "MA5",
            period: 5,
            color: "#ff0000",
            data: [
              { time: firstWeekTime, value: 107 },
              { time: secondWeekTime, value: 106 }
            ],
            visible: true,
            lineWidth: 1
          }
        ]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        meeMeeDetailChrome={{ timeframe: "weekly" }}
        meeMeeDetailChromeTerminalDates={{
          [firstWeekTime]: Date.UTC(2026, 3, 9) / 1000,
          [secondWeekTime]: Date.UTC(2026, 3, 17) / 1000
        }}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const chart = chartMocks[0];
    expect(chart.options.timeScale.rightOffset).toBe(0);
    expect(chart.options.timeScale.fixRightEdge).toBe(true);
    expect(chart.options.timeScale.lockVisibleTimeRangeOnResize).toBe(true);
    expect(chart.options.timeScale.rightBarStaysOnScroll).toBe(true);
    expect(render.container.querySelector("[data-testid='detail-chart-date-chip']")).toBeNull();
    expect(render.container.querySelector("[data-testid='detail-chart-legend']")).not.toBeNull();

    await act(async () => {
      chartRef.current?.setVisibleRange({
        from: firstWeekTime,
        to: firstWeekTime
      });
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(render.container.querySelector("[data-testid='detail-chart-date-chip']")).toBeNull();

    render.cleanup();
  });

  it("applies the visible range when data arrives after an empty first render", async () => {
    const range = {
      from: baseCandles[0].time,
      to: baseCandles[baseCandles.length - 1].time
    };
    const render = await renderClient(
      <DetailChart
        candles={[]}
        volume={[]}
        maLines={[]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        visibleRange={range}
        meeMeeDetailChrome={{ timeframe: "weekly" }}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const chart = chartMocks[0];
    expect(chart.timeScaleApi.setVisibleRange).not.toHaveBeenCalled();

    await act(async () => {
      render.root.render(
        <DetailChart
          candles={baseCandles}
          volume={baseVolume}
          maLines={baseMaLines}
          showVolume={false}
          boxes={[]}
          showBoxes={false}
          visibleRange={range}
          meeMeeDetailChrome={{ timeframe: "weekly" }}
        />
      );
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(chart.candlestickSeries.setData).toHaveBeenLastCalledWith(baseCandles);
    expect(chart.timeScaleApi.setVisibleRange).toHaveBeenCalledWith(range);

    render.cleanup();
  });

  it("hides the extra date chip on daily detail chrome", async () => {
    const dailyTime = Date.UTC(2026, 3, 22) / 1000;
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: dailyTime, open: 100, high: 110, low: 95, close: 108 }
        ]}
        volume={[{ time: dailyTime, value: 1000 }]}
        maLines={baseMaLines}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        meeMeeDetailChrome={{ timeframe: "daily" }}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(render.container.querySelector("[data-testid='detail-chart-date-chip']")).toBeNull();
    expect(render.container.querySelector("[data-testid='detail-chart-legend']")).toBeNull();

    render.cleanup();
  });

  it("draws unresolved gap bands inside the plot area", async () => {
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: 1, open: 100, high: 100, low: 95, close: 98 },
          { time: 2, open: 103, high: 104, low: 101, close: 103 },
          { time: 3, open: 102, high: 106, low: 99, close: 100 }
        ]}
        volume={[
          { time: 1, value: 1000 },
          { time: 2, value: 1100 },
          { time: 3, value: 1200 }
        ]}
        maLines={[]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        gapBands={[
          {
            direction: "up",
            topPrice: 104,
            bottomPrice: 100,
            createdAt: 2,
            filledAt: null,
            remainingTopPrice: 101,
            remainingBottomPrice: 100,
            fillRatio: 0.4
          }
        ]}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const gapDrawCalls = canvasMock?.ctx.fillRect.mock.calls.filter((call) => call[0] === 2 && call[2] === 878) ?? [];
    expect(gapDrawCalls.length).toBeGreaterThan(0);

    render.cleanup();
  });

  it("clips box overlays to the loaded candle span", async () => {
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: 10, open: 100, high: 120, low: 90, close: 110 },
          { time: 20, open: 105, high: 122, low: 88, close: 115 },
          { time: 30, open: 112, high: 118, low: 100, close: 113 }
        ]}
        volume={[]}
        maLines={[]}
        showVolume={false}
        boxes={[
          {
            startIndex: 0,
            endIndex: 1,
            startTime: 0,
            endTime: 20,
            lower: 90,
            upper: 120,
            breakout: null
          }
        ]}
        showBoxes
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(canvasMock?.ctx.strokeRect).toHaveBeenCalledWith(5, 90, 20, 30);

    render.cleanup();
  });

  it("skips fully filled gap bands", async () => {
    const render = await renderClient(
      <DetailChart
        candles={[
          { time: 1, open: 100, high: 100, low: 95, close: 98 },
          { time: 2, open: 103, high: 104, low: 101, close: 103 },
          { time: 3, open: 102, high: 106, low: 99, close: 100 }
        ]}
        volume={[
          { time: 1, value: 1000 },
          { time: 2, value: 1100 },
          { time: 3, value: 1200 }
        ]}
        maLines={[]}
        showVolume={false}
        boxes={[]}
        showBoxes={false}
        gapBands={[
          {
            direction: "up",
            topPrice: 104,
            bottomPrice: 100,
            createdAt: 2,
            filledAt: 3,
            remainingTopPrice: 100,
            remainingBottomPrice: 100,
            fillRatio: 1
          }
        ]}
      />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const gapDrawCalls = canvasMock?.ctx.fillRect.mock.calls.filter((call) => call[0] === 2 && call[2] === 878) ?? [];
    expect(gapDrawCalls.length).toBe(0);

    render.cleanup();
  });
});
