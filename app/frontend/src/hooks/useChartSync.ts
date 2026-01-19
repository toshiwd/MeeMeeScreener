import { useRef, useState, useEffect, MutableRefObject } from "react";
import { DetailChartHandle } from "../components/DetailChart";

type Timeframe = "daily" | "weekly" | "monthly";

interface UseChartSyncOptions {
    enabled: boolean;
    cursorEnabled: boolean;
    onLoadMoreDaily?: () => void;
    onLoadMoreMonthly?: () => void;
    hasMoreDaily?: boolean;
    loadingDaily?: boolean;
    hasMoreMonthly?: boolean;
    loadingMonthly?: boolean;
    dailyCandles?: { time: number }[];
    monthlyCandles?: { time: number }[];
}

export function useChartSync(
    dailyChartRef: MutableRefObject<DetailChartHandle | null>,
    monthlyChartRef: MutableRefObject<DetailChartHandle | null>,
    weeklyChartRef?: MutableRefObject<DetailChartHandle | null>,
    options?: UseChartSyncOptions
) {
    const [hoverTime, setHoverTime] = useState<number | null>(null);

    const hoverRafRef = useRef<number | null>(null);
    const hoverTimePendingRef = useRef<number | null>(null);
    const hoverTimeRef = useRef<number | null>(null);

    const syncRangesRef = useRef(options?.enabled ?? true);
    const syncRafRef = useRef<number | null>(null);
    const pendingRangeRef = useRef<{ from: number; to: number } | null>(null);

    // Update ref when option changes
    useEffect(() => {
        syncRangesRef.current = options?.enabled ?? true;
    }, [options?.enabled]);

    // Clean up RAFs
    useEffect(() => {
        return () => {
            if (hoverRafRef.current !== null) {
                window.cancelAnimationFrame(hoverRafRef.current);
            }
            if (syncRafRef.current !== null) {
                window.cancelAnimationFrame(syncRafRef.current);
            }
        };
    }, []);

    const scheduleHoverTime = (time: number | null) => {
        hoverTimePendingRef.current = time;
        if (hoverRafRef.current !== null) return;
        hoverRafRef.current = window.requestAnimationFrame(() => {
            hoverRafRef.current = null;
            const next = hoverTimePendingRef.current ?? null;
            if (hoverTimeRef.current === next) return;
            hoverTimeRef.current = next;
            setHoverTime(next);
        });
    };

    const syncRangeToSecondary = (range: { from: number; to: number }) => {
        if (!syncRangesRef.current) return;

        // Check if we need to load more data
        const dailyMin = options?.dailyCandles?.[0]?.time;
        const monthlyMin = options?.monthlyCandles?.[0]?.time;

        // Logic: If visible range start is before the earliest loaded data, trigger load more
        // Note: This logic assumes 'range' is coming from the chart.
        // However, the original code had checks against weeklyCandles for Daily range changes?
        // Let's replicate the logic but generically.

        // Original:
        // if (weeklyMin && range.from < weeklyMin && hasMoreDaily && !loadingDaily) loadMoreDaily();
        // if (monthlyMin && range.from < monthlyMin && hasMoreMonthly && !loadingMonthly) loadMoreMonthly();

        // Note: The original logic in DetailView seemed to trigger loadMoreDaily when WEEKLY range hit the edge?
        // Wait, let's re-read DetailView.tsx:1716:
        // const syncRangeToSecondary = (range: { from: number; to: number }) => {
        //   ...
        //   const weeklyMin = weeklyCandles[0]?.time;
        //   if (weeklyMin && range.from < weeklyMin && hasMoreDaily && !loadingDaily) loadMoreDaily();
        // (This seems to imply 'range' is from Daily chart, and we compare with 'weeklyMin'? Or maybe reverse?) 
        // Actually, DetailChart calls 'onVisibleRangeChange' for 'daily' chart.
        // So 'range' is the Daily chart's visible range.
        // Why compare with 'weeklyMin'? Maybe a copy-paste quirk or logic I don't fully grasp. 
        // Usually we want to load more DAILY data if we scroll DAILY chart to the left.
        // But 'dailyChart' handles its own data loading internally in some implementations? 
        // No, DetailView line 2759 has a "Load more daily" button.
        // The auto-load logic seems to be: if we scroll 'daily' chart past 'weekly' known data range? 
        // Ah, wait. Daily and Weekly candles usually cover similar timeframes but Weekly has fewer bars.
        // Let's stick to the behavior: if we scroll left, and we are near the edge of loaded data, trigger loadMore.

        if (options?.dailyCandles && options.dailyCandles.length > 0) {
            const firstDaily = options.dailyCandles[0].time;
            if (range.from < firstDaily && options.hasMoreDaily && !options.loadingDaily) {
                options.onLoadMoreDaily?.();
            }
        }

        // Also sync to other charts
        weeklyChartRef?.current?.setVisibleRange(range);
        monthlyChartRef.current?.setVisibleRange(range);
    };

    const handleDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
        if (!range) return;
        pendingRangeRef.current = range;
        if (syncRafRef.current !== null) return;
        syncRafRef.current = window.requestAnimationFrame(() => {
            syncRafRef.current = null;
            const pending = pendingRangeRef.current;
            if (!pending) return;
            syncRangeToSecondary(pending);
        });
    };

    const handleDailyCrosshair = (time: number | null) => {
        if (!options?.cursorEnabled) return;
        if (options.enabled === false) return;

        weeklyChartRef?.current?.setCrosshair(time, null);
        monthlyChartRef.current?.setCrosshair(time, null);
        scheduleHoverTime(time);
    };

    const handleMonthlyCrosshair = (time: number | null) => {
        if (!options?.cursorEnabled) return;
        if (options.enabled === false) return;

        dailyChartRef.current?.setCrosshair(time, null);
        weeklyChartRef?.current?.setCrosshair(time, null);
        scheduleHoverTime(time);
    };

    const handleWeeklyCrosshair = (time: number | null) => {
        if (!options?.cursorEnabled) return;
        if (options.enabled === false) return;

        dailyChartRef.current?.setCrosshair(time, null);
        monthlyChartRef.current?.setCrosshair(time, null);
        scheduleHoverTime(time);
    };

    return {
        hoverTime,
        setHoverTime, // exposed if needed to reset
        handleDailyVisibleRangeChange,
        handleDailyCrosshair,
        handleMonthlyCrosshair,
        handleWeeklyCrosshair
    };
}
