import { useEffect, useMemo, useRef, useState } from "react";
import { createChart, CrosshairMode } from "lightweight-charts";
import { useStore } from "../store";

const MA_COLORS = {
  ma3: "#38bdf8",
  ma10: "#a855f7",
  ma20: "#f59e0b",
  ma30: "#22c55e",
  ma60: "#e11d48"
};

export default function Sparkline({ code }: { code: string }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const candleRef = useRef<any>(null);
  const maRefs = useRef<Record<string, any>>({});
  const [chartReady, setChartReady] = useState(false);
  const resizeRafRef = useRef<number | null>(null);
  const monthly = useStore((state) => state.monthlyCache[code] || []);

  const candleData = useMemo(
    () =>
      monthly.map((row) => ({
        time: row[0],
        open: row[1],
        high: row[2],
        low: row[3],
        close: row[4]
      })),
    [monthly]
  );

  const maSeriesData = useMemo(
    () => ({
      ma3: monthly.map((row) => ({ time: row[0], value: row[5] })),
      ma10: monthly.map((row) => ({ time: row[0], value: row[6] })),
      ma20: monthly.map((row) => ({ time: row[0], value: row[7] })),
      ma30: monthly.map((row) => ({ time: row[0], value: row[8] })),
      ma60: monthly.map((row) => ({ time: row[0], value: row[9] }))
    }),
    [monthly]
  );

  useEffect(() => {
    if (!containerRef.current) return;
    const element = containerRef.current;
    let resizeObserver: ResizeObserver | null = null;

    const init = (width: number) => {
      if (chartRef.current || width <= 0) return;
      console.debug("[Sparkline] init", { width, height: 120 });
      const chart = createChart(element, {
        height: 120,
        width,
        layout: {
          background: { color: "transparent" },
          textColor: "#7c8698"
        },
        grid: {
          vertLines: { color: "rgba(255,255,255,0.04)" },
          horzLines: { color: "rgba(255,255,255,0.04)" }
        },
        rightPriceScale: { visible: false },
        leftPriceScale: { visible: false },
        timeScale: { visible: false },
        crosshair: { mode: CrosshairMode.Magnet, vertLine: { visible: false }, horzLine: { visible: false } },
        handleScroll: false,
        handleScale: false
      });

      const series = chart.addCandlestickSeries({
        upColor: "#ef4444",
        downColor: "#22c55e",
        borderVisible: false,
        wickUpColor: "#ef4444",
        wickDownColor: "#22c55e"
      });

      const ma3 = chart.addLineSeries({ color: MA_COLORS.ma3, lineWidth: 1, priceLineVisible: false });
      const ma10 = chart.addLineSeries({ color: MA_COLORS.ma10, lineWidth: 1, priceLineVisible: false });
      const ma20 = chart.addLineSeries({ color: MA_COLORS.ma20, lineWidth: 1, priceLineVisible: false });
      const ma30 = chart.addLineSeries({ color: MA_COLORS.ma30, lineWidth: 1, priceLineVisible: false });
      const ma60 = chart.addLineSeries({ color: MA_COLORS.ma60, lineWidth: 1, priceLineVisible: false });

      chartRef.current = chart;
      candleRef.current = series;
      maRefs.current = { ma3, ma10, ma20, ma30, ma60 };
      setChartReady(true);
    };

    const scheduleResize = (width: number) => {
      if (width <= 0 || !chartRef.current) return;
      if (resizeRafRef.current !== null) return;
      resizeRafRef.current = window.requestAnimationFrame(() => {
        resizeRafRef.current = null;
        const chart = chartRef.current;
        if (!chart) return;
        chart.applyOptions({ width });
        chart.timeScale().fitContent();
      });
    };

    const handleSize = (width: number) => {
      if (width <= 0) return;
      if (!chartRef.current) {
        init(width);
        return;
      }
      scheduleResize(width);
    };

    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const width = Math.floor(entry.contentRect.width);
        handleSize(width);
      }
    });
    resizeObserver.observe(element);

    const initialWidth = Math.floor(element.clientWidth);
    if (initialWidth > 0) {
      handleSize(initialWidth);
    }

    return () => {
      if (resizeRafRef.current !== null) {
        window.cancelAnimationFrame(resizeRafRef.current);
        resizeRafRef.current = null;
      }
      if (resizeObserver) resizeObserver.disconnect();
      if (chartRef.current) {
        chartRef.current.remove();
      }
      chartRef.current = null;
      candleRef.current = null;
      maRefs.current = {};
      setChartReady(false);
    };
  }, []);

  useEffect(() => {
    if (!candleRef.current || !candleData.length) return;
    candleRef.current.setData(candleData);
    Object.entries(maRefs.current).forEach(([key, series]) => {
      const points = maSeriesData[key as keyof typeof maSeriesData];
      series.setData(points.filter((point) => point.value !== null));
    });
    if (chartRef.current) {
      window.requestAnimationFrame(() => chartRef.current?.timeScale().fitContent());
    }
  }, [candleData, maSeriesData, chartReady]);

  return <div ref={containerRef} className="sparkline" />;
}
