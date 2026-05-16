
import { useCallback, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { OffscreenDetailView } from "../components/OffscreenDetailView";
import { api } from "../api";

import { useStore } from "../store";
import { saveBlobToFile } from "../utils/windowScreenshot";

const WAIT_FRAMES = 8;
const SCREENSHOT_CAPTURE_SETTLE_MS = 350;
const DAILY_SCREENSHOT_BARS = 120;
const WEEKLY_SCREENSHOT_BARS = 52;
const MONTHLY_SCREENSHOT_BARS = 36;
const SCREENSHOT_TARGET_MISSING_MESSAGE = "スクショ対象がありません。";
const SCREENSHOT_DATA_LOAD_FAILED_MESSAGE = "チャートデータを取得できませんでした。";
const SCREENSHOT_CAPTURE_FAILED_MESSAGE = "スクショの画像生成に失敗しました。";
const SCREENSHOT_SAVE_FAILED_MESSAGE = "スクショの保存に失敗しました。";
const SCREENSHOT_SAVE_TIMEOUT_MESSAGE = "スクショの保存に時間がかかりすぎています。";
const SCREENSHOT_GENERIC_FAILED_MESSAGE = "スクショ作成中に問題が発生しました。";

// Helper to wait for frames
const waitForFrames = (count: number) => {
    return new Promise<void>((resolve) => {
        let i = 0;
        const tick = () => {
            i++;
            if (i >= count) {
                // Wait a bit more for canvas draw
                setTimeout(resolve, 100);
            } else {
                requestAnimationFrame(tick);
            }
        };
        requestAnimationFrame(tick);
    });
};

export const useConsultScreenshot = () => {
    const [isProcessing, setIsProcessing] = useState(false);
    const [progress, setProgress] = useState<{ current: number; total: number } | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);
    const rootRef = useRef<any>(null);

    const { boxesCache, maSettings, settings } = useStore();
    const showBoxes = settings.showBoxes;

    const generateScreenshots = useCallback(async (codes: string[]) => {
        if (codes.length === 0) return { success: false, error: SCREENSHOT_TARGET_MISSING_MESSAGE };

        setIsProcessing(true);
        setProgress({ current: 0, total: codes.length });

        // Create hidden container if not exists
        if (!containerRef.current) {
            const div = document.createElement("div");
            div.style.position = "absolute";
            div.style.left = "-9999px";
            div.style.top = "0";
            div.style.width = "1400px"; // High resolution width
            div.style.height = "900px";
            div.style.visibility = "visible"; // Must be visible for charts to render
            div.style.zIndex = "-9999";
            div.style.background = "#0f172a";
            document.body.appendChild(div);
            containerRef.current = div;
            rootRef.current = createRoot(div);
        }

        let savedCount = 0;
        let lastError = null;
        let lastSavedDir = null;

        try {
            for (let i = 0; i < codes.length; i++) {
                const code = codes[i];
                try {
                    setProgress({ current: i + 1, total: codes.length });

                    // 1. Fetch Data
                    // Use cache if available? We can use the store or verify freshness.
                    // Reusing DetailView's loader logic logic manually here.
                    // Assuming we have a loader that returns { daily, weekly, monthly, ... }
                    // For now, let's assume we fetch fresh or use a helper.

                    // We need to fetch Ticker Name too.
                    let tickerName = code;
                    try {
                        const metaRes = await api.get(`/stocks/${code}`);
                        tickerName = metaRes.data?.name ?? code;
                    } catch {
                        // Fallback to the ticker code when metadata lookup fails.
                    }

                    // Fetch daily/weekly/monthly in one round-trip.
                    const barsRes = await api.post("/batch_bars_v3", {
                        codes: [code],
                        timeframes: ["daily", "weekly", "monthly"],
                        limit: 160,
                        timeframeLimits: {
                            daily: 140,
                            weekly: 80,
                            monthly: 72,
                        },
                        includeProvisional: true,
                        includeBoxes: true,
                    });

                    // Extract payload from response
                    const barsItem = barsRes.data?.items?.[code];
                    const dailyPayload = barsItem?.daily;
                    const weeklyPayload = barsItem?.weekly;
                    const monthlyPayload = barsItem?.monthly;

                    if (!dailyPayload || !weeklyPayload || !monthlyPayload) {
                        throw new Error(SCREENSHOT_DATA_LOAD_FAILED_MESSAGE);
                    }

                    // Helper to parse candles from bars array
                    const parseBars = (data: number[][]) => data.map(d => ({
                        time: d[0],
                        open: d[1],
                        high: d[2],
                        low: d[3],
                        close: d[4],
                    }));
                    const parseVolume = (data: number[][]) => data.map(d => ({
                        time: d[0],
                        value: d[5],
                    }));

                    const dailyCandles = parseBars(dailyPayload.bars).slice(-DAILY_SCREENSHOT_BARS);
                    const dailyVolume = parseVolume(dailyPayload.bars).slice(-DAILY_SCREENSHOT_BARS);
                    const weeklyCandles = parseBars(weeklyPayload.bars).slice(-WEEKLY_SCREENSHOT_BARS);
                    const weeklyVolume = parseVolume(weeklyPayload.bars).slice(-WEEKLY_SCREENSHOT_BARS);
                    const monthlyCandles = parseBars(monthlyPayload.bars).slice(-MONTHLY_SCREENSHOT_BARS);
                    const monthlyVolume = parseVolume(monthlyPayload.bars).slice(-MONTHLY_SCREENSHOT_BARS);
                    const monthlyBoxes = Array.isArray(monthlyPayload.boxes)
                        ? monthlyPayload.boxes
                        : boxesCache.monthly[code] || boxesCache.daily[code] || [];

                    // Quick MA function (Simple Moving Average)
                    const calculateSMA = (data: { time: number, close: number }[], period: number) => {
                        const result = [];
                        for (let j = 0; j < data.length; j++) {
                            if (j < period - 1) continue;
                            let sum = 0;
                            for (let k = 0; k < period; k++) sum += data[j - k].close;
                            result.push({ time: data[j].time, value: sum / period });
                        }
                        return result;
                    };

                    const buildMaLines = (candles: any[], settings: any[]) => {
                        return settings
                            .filter(s => s.visible)
                            .map((s) => ({
                                key: `ma-${s.period}`,
                                period: s.period,
                                color: s.color,
                                lineWidth: s.lineWidth,
                                visible: true,
                                data: calculateSMA(candles, s.period)
                            }));
                    };

                    const dailyMaLines = buildMaLines(dailyCandles, maSettings.daily);
                    const weeklyMaLines = buildMaLines(weeklyCandles, maSettings.weekly);
                    const monthlyMaLines = buildMaLines(monthlyCandles, maSettings.monthly);

                    // Positions (Fetch or empty)
                    // We can fetch if needed, let's assume empty for speed/safety unless demanded.
                    const dailyPositions: any[] = [];
                    const tradeMarkers: any[] = [];

                    // Render
                    rootRef.current.render(
                        <OffscreenDetailView
                            code={code}
                            tickerName={tickerName}
                            dailyCandles={dailyCandles}
                            weeklyCandles={weeklyCandles}
                            monthlyCandles={monthlyCandles}
                            dailyVolume={dailyVolume}
                            weeklyVolume={weeklyVolume}
                            monthlyVolume={monthlyVolume}
                            dailyMaLines={dailyMaLines}
                            weeklyMaLines={weeklyMaLines}
                            monthlyMaLines={monthlyMaLines}
                            boxes={monthlyBoxes}
                            showBoxes={showBoxes}
                            dailyPositions={dailyPositions}
                            tradeMarkers={tradeMarkers}
                        />
                    );

                    // Wait for Render
                    await waitForFrames(WAIT_FRAMES);

                    // Capture
                    // `captureWindowBlob` uses `document.getElementById('root')` by default.
                    // We need to modify `windowScreenshot.ts` to accept an element, OR use `html2canvas` directly here.
                    // Let's import `html2canvas` directly here for custom element.
                    const html2canvas = (await import("html2canvas")).default;

                    // Capture with reduced scale for stability
                    let canvas;
                    try {
                        await new Promise(r => setTimeout(r, SCREENSHOT_CAPTURE_SETTLE_MS));

                        canvas = await html2canvas(containerRef.current, {
                            useCORS: true,
                            allowTaint: true, // Crucial for some canvas elements
                            scale: 1.5,
                            logging: false,
                            backgroundColor: "#0f172a",
                            width: 1400,
                            height: 900
                        });
                    } catch (err) {
                        console.error("html2canvas failed:", err);
                        lastError = SCREENSHOT_CAPTURE_FAILED_MESSAGE;
                        continue;
                    }

                    const blob = await new Promise<Blob | null>(res => canvas.toBlob(res, "image/png"));

                    if (blob) {
                        const now = new Date();
                        const stamp = now.toISOString().replace(/[-:T.]/g, "").slice(0, 15); // YYYYMMDDHHMMSS
                        const safeCode = code.replace(/[^a-zA-Z0-9]/g, "_");
                        const filename = `Consult_${safeCode}_${stamp}.png`;

                        try {
                            // Add timeout to prevent infinite hang
                            const savePromise = saveBlobToFile(blob, filename);
                            const timeoutPromise = new Promise<never>((_, reject) =>
                                setTimeout(() => reject(new Error(SCREENSHOT_SAVE_TIMEOUT_MESSAGE)), 15000)
                            );

                            const res = await Promise.race([savePromise, timeoutPromise]);

                            if (res.success) {
                                savedCount++;
                                if (res.savedDir) lastSavedDir = res.savedDir;
                            } else {
                                console.error("Save failed:", res.error);
                                lastError = res.error || SCREENSHOT_SAVE_FAILED_MESSAGE;
                            }
                        } catch (e: any) {
                            console.error("Save exception:", e);
                            lastError = e?.message || SCREENSHOT_SAVE_FAILED_MESSAGE;
                        }
                    }
                } catch (e: any) {
                    console.error(`Error processing code ${code}:`, e);
                    if (!lastError) lastError = `${code} のスクショ作成に失敗しました。`;
                }
            }

        } catch (e: any) {
            console.error("Global screenshot error:", e);
            lastError = e?.message || SCREENSHOT_GENERIC_FAILED_MESSAGE;
        } finally {
            setIsProcessing(false);
            setProgress(null);
            if (rootRef.current) {
                // setTimeout(() => rootRef.current.unmount(), 1000);
            }
        }

        return { success: savedCount > 0, count: savedCount, savedDir: lastSavedDir, error: lastError };

    }, [boxesCache, maSettings, showBoxes]);

    return { generateScreenshots, isProcessing, progress };
};
