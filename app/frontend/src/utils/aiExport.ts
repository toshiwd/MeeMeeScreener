/**
 * AI相談用銘柄情報出力ユーティリティ
 * 銘柄詳細/練習モードから呼び出し、Markdown形式で出力
 */

import type { Box, MaSetting } from "../store";

type BarData = {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume?: number | null;
};

type SignalData = {
    label: string;
    kind: "warning" | "achieved";
};

type PositionSnapshot = {
    brokerKey?: string;
    brokerLabel?: string;
    longLots: number;
    shortLots: number;
    avgLongPrice?: number;
    avgShortPrice?: number;
    realizedPnL?: number;
};

type CandleAnalysis = {
    date: string;
    shape: string;
    signal: string;
};

type AIExportInput = {
    code: string;
    name?: string | null;
    // View settings
    visibleTimeframe: "daily" | "weekly" | "monthly";
    rangeMonths: number | null;
    // Data by timeframe
    dailyBars: BarData[];
    weeklyBars: BarData[];
    monthlyBars: BarData[];
    // MA settings
    maSettings: {
        daily: MaSetting[];
        weekly: MaSetting[];
        monthly: MaSetting[];
    };
    // Signals/Badges
    signals: SignalData[];
    // UI state
    showBoxes: boolean;
    showPositions: boolean;
    boxes: Box[];
    // Daily memo map (date => memo)
    dailyMemos?: Record<string, string>;
    // Current positions summary
    currentPositions?: PositionSnapshot[];
    // Current values (optional)
    currentPrice?: number | null;
};

type AIExportResult = {
    markdown: string;
    json: object;
};

type MAPayload = {
    daily: Record<string, number | null>;
    weekly: Record<string, number | null>;
    monthly: Record<string, number | null>;
};

const N_A = "N/A";
const VOLUME_UNIT = "shares";

const formatDate = (time: number): string => {
    if (time >= 10000000 && time < 100000000) {
        const year = Math.floor(time / 10000);
        const month = Math.floor((time % 10000) / 100);
        const day = time % 100;
        return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
    if (time >= 100000 && time < 1000000) {
        const year = Math.floor(time / 100);
        const month = time % 100;
        return `${year}-${String(month).padStart(2, "0")}-01`;
    }
    const date = time > 1000000000000 ? new Date(time) : time > 1000000000 ? new Date(time * 1000) : null;
    if (!date || Number.isNaN(date.getTime())) return N_A;
    return date.toISOString().slice(0, 10);
};

const formatNumber = (value: number | null | undefined, digits = 0): string => {
    if (value == null || !Number.isFinite(value)) return N_A;
    return value.toFixed(digits);
};

const formatIsoWithOffset = (date: Date): string => {
    const pad = (value: number) => String(value).padStart(2, "0");
    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    const seconds = pad(date.getSeconds());
    const offsetMinutes = -date.getTimezoneOffset();
    const sign = offsetMinutes >= 0 ? "+" : "-";
    const absOffset = Math.abs(offsetMinutes);
    const offsetHours = pad(Math.floor(absOffset / 60));
    const offsetMins = pad(absOffset % 60);
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}${sign}${offsetHours}:${offsetMins}`;
};

const logVolumeStats = (stage: string, bars: BarData[]) => {
    const total = bars.length;
    let nulls = 0;
    let zeros = 0;
    let min: number | null = null;
    let max: number | null = null;
    bars.forEach((bar) => {
        const value = bar.volume;
        if (value == null || !Number.isFinite(value)) {
            nulls += 1;
            return;
        }
        if (value === 0) zeros += 1;
        min = min == null ? value : Math.min(min, value);
        max = max == null ? value : Math.max(max, value);
    });
    console.debug("[ai_export] volume_stats", {
        stage,
        total,
        nulls,
        zeros,
        min,
        max,
    });
};

const computeMA = (bars: BarData[], period: number): number | null => {
    if (bars.length < period) return null;
    let sum = 0;
    for (let i = bars.length - period; i < bars.length; i++) {
        sum += bars[i].close;
    }
    return sum / period;
};

const computeCandleStats = (bar: BarData) => {
    const open = bar.open;
    const close = bar.close;
    const high = bar.high;
    const low = bar.low;
    const body = Math.abs(close - open);
    const range = Math.max(high - low, 1e-9);
    const upper = high - Math.max(open, close);
    const lower = Math.min(open, close) - low;
    const body_r = body / range;
    const upper_r = upper / range;
    const lower_r = lower / range;
    return {
        open,
        close,
        high,
        low,
        body,
        range,
        upper,
        lower,
        body_r,
        upper_r,
        lower_r,
    };
};

const buildCandleShapes = (bars: BarData[]): CandleAnalysis[] => {
    return bars.map((bar) => {
        const stats = computeCandleStats(bar);
        const shapes: string[] = [];
        const isBull = stats.close > stats.open;
        const isBear = stats.close < stats.open;
        const isDoji = stats.body_r <= 0.05;

        if (isBull) shapes.push("bull");
        if (isBear) shapes.push("bear");
        if (!isBull && !isBear) shapes.push("doji");

        if (isBull && stats.body_r >= 0.6) shapes.push("big_bull");
        if (isBear && stats.body_r >= 0.6) shapes.push("big_bear");
        if (isBull && stats.body_r <= 0.2) shapes.push("small_bull");
        if (isBear && stats.body_r <= 0.2) shapes.push("small_bear");
        if (isBull && stats.upper_r <= 0.05 && stats.lower_r <= 0.05) {
            shapes.push("marubozu_bull");
        }
        if (isBear && stats.upper_r <= 0.05 && stats.lower_r <= 0.05) {
            shapes.push("marubozu_bear");
        }
        if (isDoji) shapes.push("doji");
        if (stats.body_r > 0.05 && stats.body_r <= 0.2) shapes.push("spinning_top");
        if (isDoji && stats.lower_r >= 0.5 && stats.upper_r <= 0.1) {
            shapes.push("dragonfly_doji");
        }
        if (isDoji && stats.upper_r >= 0.5 && stats.lower_r <= 0.1) {
            shapes.push("gravestone_doji");
        }
        if (stats.upper_r >= 0.5) shapes.push("long_upper_wick");
        if (stats.lower_r >= 0.5) shapes.push("long_lower_wick");
        if (stats.lower_r >= 0.5 && stats.body_r <= 0.3 && stats.upper_r <= 0.15) {
            shapes.push("hammer");
        }
        if (stats.upper_r >= 0.5 && stats.body_r <= 0.3 && stats.lower_r <= 0.15) {
            shapes.push("inverted_hammer");
        }

        return {
            date: formatDate(bar.time),
            shape: shapes.length ? shapes.join("|") : "none",
            signal: "none",
        };
    });
};

const buildCandleSignals = (bars: BarData[], shapes: CandleAnalysis[]): CandleAnalysis[] => {
    const statsList = bars.map((bar) => computeCandleStats(bar));
    const analysis = shapes.map((entry) => ({ ...entry, signal: "none" }));

    const epsFor = (v: number) => Math.max(v * 0.001, 0.1);

    for (let i = 1; i < bars.length; i += 1) {
        const s1 = statsList[i - 1];
        const s2 = statsList[i];
        const o1 = s1.open;
        const c1 = s1.close;
        const o2 = s2.open;
        const c2 = s2.close;
        const signals: string[] = [];

        const prevBull = c1 > o1;
        const prevBear = c1 < o1;
        const currBull = c2 > o2;
        const currBear = c2 < o2;

        if (currBull && prevBear && o2 <= c1 && c2 >= o1) signals.push("bullish_engulfing");
        if (currBear && prevBull && o2 >= c1 && c2 <= o1) signals.push("bearish_engulfing");

        if (Math.max(o2, c2) <= Math.max(o1, c1) && Math.min(o2, c2) >= Math.min(o1, c1)) {
            signals.push("harami");
            if (s2.body_r <= 0.05) signals.push("harami_doji");
        }

        const epsHigh = epsFor(Math.max(s1.high, s2.high));
        const epsLow = epsFor(Math.max(s1.low, s2.low));
        if (Math.abs(s2.high - s1.high) <= epsHigh) signals.push("tweezer_top");
        if (Math.abs(s2.low - s1.low) <= epsLow) signals.push("tweezer_bottom");

        if (prevBear && s1.body_r >= 0.6 && o2 < c1 && c2 >= (o1 + c1) / 2 && c2 < o1) {
            signals.push("piercing_line");
        }
        if (prevBull && s1.body_r >= 0.6 && o2 > c1 && c2 <= (o1 + c1) / 2 && c2 > o1) {
            signals.push("dark_cloud_cover");
        }

        if (signals.length) analysis[i].signal = signals.join("|");
    }

    for (let i = 2; i < bars.length; i += 1) {
        const s0 = statsList[i - 2];
        const s1 = statsList[i - 1];
        const s2 = statsList[i];
        const signals: string[] = [];

        const bull1 = s0.close > s0.open;
        const bull2 = s1.close > s1.open;
        const bull3 = s2.close > s2.open;
        const bear1 = s0.close < s0.open;
        const bear2 = s1.close < s1.open;
        const bear3 = s2.close < s2.open;

        if (
            bull1 && bull2 && bull3 &&
            s0.lower < s1.lower && s1.lower < s2.lower &&
            s0.close < s1.close && s1.close < s2.close &&
            s0.upper_r <= 0.3 && s1.upper_r <= 0.3 && s2.upper_r <= 0.3
        ) {
            signals.push("three_white_soldiers");
        }
        if (
            bear1 && bear2 && bear3 &&
            s0.high > s1.high && s1.high > s2.high &&
            s0.close > s1.close && s1.close > s2.close &&
            s0.lower_r <= 0.3 && s1.lower_r <= 0.3 && s2.lower_r <= 0.3
        ) {
            signals.push("three_black_crows");
        }

        if (
            bear1 && s0.body_r >= 0.6 &&
            s1.body_r <= 0.2 &&
            bull3 && s2.body_r >= 0.6 &&
            s2.close >= (s0.open + s0.close) / 2
        ) {
            signals.push("morning_star");
        }
        if (
            bull1 && s0.body_r >= 0.6 &&
            s1.body_r <= 0.2 &&
            bear3 && s2.body_r >= 0.6 &&
            s2.close <= (s0.open + s0.close) / 2
        ) {
            signals.push("evening_star");
        }

        if (signals.length) {
            analysis[i].signal = analysis[i].signal === "none"
                ? signals.join("|")
                : `${analysis[i].signal}|${signals.join("|")}`;
        }
    }

    for (let i = 4; i < bars.length; i += 1) {
        const s0 = statsList[i - 4];
        const s1 = statsList[i - 3];
        const s2 = statsList[i - 2];
        const s3 = statsList[i - 1];
        const s4 = statsList[i];
        const signals: string[] = [];

        const strongBull = s0.close > s0.open && s0.body_r >= 0.6;
        const strongBear = s0.close < s0.open && s0.body_r >= 0.6;
        const midSmall = [s1, s2, s3].every((s) => s.body_r <= 0.3);
        const midInside = [s1, s2, s3].every((s) => s.high <= s0.high && s.low >= s0.low);
        const lastStrongBull = s4.close > s4.open && s4.body_r >= 0.6 && s4.close > s0.close;
        const lastStrongBear = s4.close < s4.open && s4.body_r >= 0.6 && s4.close < s0.close;

        if (strongBull && midSmall && midInside && lastStrongBull) {
            signals.push("rising_three_methods");
        }
        if (strongBear && midSmall && midInside && lastStrongBear) {
            signals.push("falling_three_methods");
        }

        if (signals.length) {
            analysis[i].signal = analysis[i].signal === "none"
                ? signals.join("|")
                : `${analysis[i].signal}|${signals.join("|")}`;
        }
    }

    return analysis;
};

const buildOHLCVCsv = (
    bars: BarData[],
    limit: number,
    memoMap?: Record<string, string>
): string => {
    const sliced = bars.slice(-limit);
    const shapeSeed = buildCandleShapes(sliced);
    const analyzed = buildCandleSignals(sliced, shapeSeed);
    const includeMemo = Boolean(memoMap);
    const lines = [
        includeMemo
            ? "date,open,high,low,close,volume,shape,signal,memo"
            : "date,open,high,low,close,volume,shape,signal"
    ];
    sliced.forEach((bar) => {
        const date = formatDate(bar.time);
        const analysis = analyzed.find((entry) => entry.date === date);
        const shape = analysis?.shape ?? "none";
        const signal = analysis?.signal ?? "none";
        const memo = memoMap?.[date] ?? "";
        const volume = bar.volume == null || !Number.isFinite(bar.volume) ? "" : String(bar.volume);
        lines.push(
            includeMemo
                ? `${date},${bar.open},${bar.high},${bar.low},${bar.close},${volume},${shape},${signal},${memo}`
                : `${date},${bar.open},${bar.high},${bar.low},${bar.close},${volume},${shape},${signal}`
        );
    });
    return lines.join("\n");
};

const getActiveMAList = (settings: MaSetting[]): string => {
    const active = settings.filter((ma) => ma.visible);
    if (!active.length) return "なし";
    return active.map((ma) => `MA${ma.period}`).join(", ");
};

const computeMAValues = (bars: BarData[], settings: MaSetting[]): string => {
    if (!bars.length) return N_A;
    const active = settings.filter((ma) => ma.visible);
    if (!active.length) return N_A;
    const parts: string[] = [];
    active.forEach((ma) => {
        const value = computeMA(bars, ma.period);
        parts.push(`MA${ma.period}=${value != null ? formatNumber(value, 2) : N_A}`);
    });
    return parts.join(", ");
};

const buildPositionsByBroker = (positions: PositionSnapshot[] | undefined) => {
    const empty = {
        buy: 0,
        sell: 0,
        avgLong: null as number | null,
        avgShort: null as number | null,
        pnl: null as number | null,
    };
    const result: Record<string, typeof empty> = {
        rakuten: { ...empty },
        sbi: { ...empty },
    };
    (positions ?? []).forEach((pos) => {
        const key = (pos.brokerKey ?? "unknown").toLowerCase();
        result[key] = {
            buy: Number.isFinite(Number(pos.longLots)) ? Number(pos.longLots) : 0,
            sell: Number.isFinite(Number(pos.shortLots)) ? Number(pos.shortLots) : 0,
            avgLong: Number.isFinite(Number(pos.avgLongPrice)) ? Number(pos.avgLongPrice) : null,
            avgShort: Number.isFinite(Number(pos.avgShortPrice)) ? Number(pos.avgShortPrice) : null,
            pnl: Number.isFinite(Number(pos.realizedPnL)) ? Number(pos.realizedPnL) : null,
        };
    });
    const total = Object.values(result).reduce(
        (acc, item) => {
            acc.buy += item.buy;
            acc.sell += item.sell;
            return acc;
        },
        { buy: 0, sell: 0 }
    );
    return {
        ...result,
        total: {
            buy: total.buy,
            sell: total.sell,
            avgLong: null,
            avgShort: null,
            pnl: null,
        },
    };
};

const toFiniteNumber = (value: unknown): number | null => {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
};

const computeMAAt = (bars: BarData[], period: number, endExclusive: number): number | null => {
    if (period <= 0 || endExclusive < period) return null;
    let sum = 0;
    for (let i = endExclusive - period; i < endExclusive; i += 1) {
        sum += bars[i].close;
    }
    return sum / period;
};

const computeMASlopePct = (bars: BarData[], period: number, lookbackBars: number): number | null => {
    const end = bars.length;
    const current = computeMAAt(bars, period, end);
    const prev = computeMAAt(bars, period, end - lookbackBars);
    if (current == null || prev == null || !Number.isFinite(prev) || Math.abs(prev) < 1e-9) return null;
    return (current - prev) / Math.abs(prev);
};

const lastFiniteVolume = (bars: BarData[]): number | null => {
    for (let i = bars.length - 1; i >= 0; i -= 1) {
        const volume = toFiniteNumber(bars[i].volume);
        if (volume != null) return volume;
    }
    return null;
};

const buildPatternSnapshot = (
    input: AIExportInput,
    maPayload: MAPayload,
    currentPrice: number | null,
    lastDaily: BarData | null
) => {
    const bars = input.dailyBars;
    const lastClose = toFiniteNumber(currentPrice ?? lastDaily?.close ?? null);
    const ma7 = toFiniteNumber(maPayload.daily["7"]);
    const ma20 = toFiniteNumber(maPayload.daily["20"]);
    const ma60 = toFiniteNumber(maPayload.daily["60"]);
    const ma20Slope5d = computeMASlopePct(bars, 20, 5);
    const ma60Slope5d = computeMASlopePct(bars, 60, 5);

    let maAlignment = "unknown";
    if (ma7 != null && ma20 != null && ma60 != null) {
        if (ma7 >= ma20 && ma20 >= ma60) maAlignment = "ma7>=ma20>=ma60";
        else if (ma7 <= ma20 && ma20 <= ma60) maAlignment = "ma7<=ma20<=ma60";
        else maAlignment = "mixed";
    }

    let trendRegime = "sideways";
    if (maAlignment === "ma7>=ma20>=ma60") trendRegime = "uptrend";
    else if (maAlignment === "ma7<=ma20<=ma60") trendRegime = "downtrend";

    const extensionFromMA20Pct =
        lastClose != null && ma20 != null && Math.abs(ma20) > 1e-9
            ? (lastClose - ma20) / Math.abs(ma20)
            : null;
    const extensionFromMA60Pct =
        lastClose != null && ma60 != null && Math.abs(ma60) > 1e-9
            ? (lastClose - ma60) / Math.abs(ma60)
            : null;

    const recent60 = bars.slice(-60);
    const recentHighCandidates = recent60
        .map((bar) => toFiniteNumber(bar.high))
        .filter((v): v is number => v != null);
    const recentHigh60 = recentHighCandidates.length > 0 ? Math.max(...recentHighCandidates) : null;
    const pullbackFromHighPct =
        lastClose != null && recentHigh60 != null && recentHigh60 > 0
            ? (lastClose - recentHigh60) / recentHigh60
            : null;

    const vol20 = bars
        .slice(-20)
        .map((bar) => toFiniteNumber(bar.volume))
        .filter((v): v is number => v != null);
    const avgVolume20 = vol20.length > 0 ? vol20.reduce((sum, v) => sum + v, 0) / vol20.length : null;
    const latestVolume = lastFiniteVolume(bars);
    const volumeRatio20 =
        latestVolume != null && avgVolume20 != null && avgVolume20 > 0
            ? latestVolume / avgVolume20
            : null;

    const lastBox = input.boxes.length > 0 ? input.boxes[input.boxes.length - 1] : null;
    const boxPosition = (() => {
        if (!lastBox || lastClose == null) return "none";
        if (lastClose > lastBox.upper) return "above";
        if (lastClose < lastBox.lower) return "below";
        return "inside";
    })();
    const barsSinceBoxEnd = (() => {
        if (!lastBox) return null;
        const idx = bars.findIndex((bar) => bar.time >= lastBox.endTime);
        if (idx < 0) return null;
        return Math.max(0, bars.length - 1 - idx);
    })();

    const tags: string[] = [];
    if (trendRegime === "uptrend") tags.push("trend_up");
    if (trendRegime === "downtrend") tags.push("trend_down");
    if (
        trendRegime === "uptrend" &&
        pullbackFromHighPct != null &&
        pullbackFromHighPct <= -0.02 &&
        pullbackFromHighPct >= -0.12
    ) {
        tags.push("pullback_in_uptrend");
    }
    if (boxPosition === "above" && volumeRatio20 != null && volumeRatio20 >= 1.2) {
        tags.push("breakout_with_volume");
    }
    if (ma20Slope5d != null && ma60Slope5d != null && ma20Slope5d > 0 && ma60Slope5d > 0) {
        tags.push("ma_slopes_positive");
    }
    if (extensionFromMA20Pct != null && extensionFromMA20Pct >= 0.12) {
        tags.push("overextended_risk");
    }
    if (tags.length === 0) {
        tags.push("no_clear_edge");
    }

    const entryHypothesis =
        tags.includes("pullback_in_uptrend")
            ? "Uptrend pullback: enter when price reclaims short MA with stable volume."
            : tags.includes("breakout_with_volume")
                ? "Breakout continuation: enter on hold above range high with volume support."
                : "No immediate edge: wait for either pullback support or a clean breakout/retest.";
    const invalidationRule =
        ma20 != null
            ? `Daily close below MA20 (${formatNumber(ma20, 2)}) without quick reclaim.`
            : "Breakout fails and price returns inside prior range.";
    const takeProfitRule =
        recentHigh60 != null
            ? `Scale near prior swing high (${formatNumber(recentHigh60, 2)}) and trail the remainder.`
            : "Scale out at +1R and trail under recent swing lows.";

    return {
        as_of_date: lastDaily ? formatDate(lastDaily.time) : null,
        trend: {
            regime: trendRegime,
            ma_alignment: maAlignment,
            ma20_slope_5d_pct: ma20Slope5d,
            ma60_slope_5d_pct: ma60Slope5d,
            price_vs_ma20_pct: extensionFromMA20Pct,
            price_vs_ma60_pct: extensionFromMA60Pct,
        },
        range_breakout: {
            has_box: Boolean(lastBox),
            box_low: lastBox ? lastBox.lower : null,
            box_high: lastBox ? lastBox.upper : null,
            breakout_direction: lastBox?.breakout ?? "none",
            price_position: boxPosition,
            bars_since_box_end: barsSinceBoxEnd,
        },
        volume: {
            latest: latestVolume,
            avg20: avgVolume20,
            ratio20: volumeRatio20,
        },
        pullback: {
            recent_high_60d: recentHigh60,
            pullback_from_high_pct: pullbackFromHighPct,
        },
        pattern_tags: tags,
        reasoning_template: {
            entry_hypothesis: entryHypothesis,
            invalidation: invalidationRule,
            take_profit_or_scale_out: takeProfitRule,
        },
    };
};

export const buildAIExport = (input: AIExportInput): AIExportResult => {
    const now = new Date();
    const exportedAt = formatIsoWithOffset(now);
    logVolumeStats("export", input.dailyBars);

    const lastDaily = input.dailyBars.length ? input.dailyBars[input.dailyBars.length - 1] : null;
    const currentPrice = input.currentPrice ?? lastDaily?.close ?? null;
    const dailyMemos = input.dailyMemos ?? {};

    const dailyBase = buildCandleShapes(input.dailyBars);
    const weeklyBase = buildCandleShapes(input.weeklyBars);
    const monthlyBase = buildCandleShapes(input.monthlyBars);
    const dailyAnalysis = buildCandleSignals(input.dailyBars, dailyBase);
    const weeklyAnalysis = buildCandleSignals(input.weeklyBars, weeklyBase);
    const monthlyAnalysis = buildCandleSignals(input.monthlyBars, monthlyBase);
    const maPayload: MAPayload = {
        daily: Object.fromEntries(
            input.maSettings.daily
                .filter((m) => m.visible)
                .map((m) => [m.period, computeMA(input.dailyBars, m.period)])
        ) as Record<string, number | null>,
        weekly: Object.fromEntries(
            input.maSettings.weekly
                .filter((m) => m.visible)
                .map((m) => [m.period, computeMA(input.weeklyBars, m.period)])
        ) as Record<string, number | null>,
        monthly: Object.fromEntries(
            input.maSettings.monthly
                .filter((m) => m.visible)
                .map((m) => [m.period, computeMA(input.monthlyBars, m.period)])
        ) as Record<string, number | null>,
    };
    const boxesPayload = input.boxes.slice(-5).map((box) => ({
        start: formatDate(box.startTime),
        end: formatDate(box.endTime),
        low: box.lower,
        high: box.upper,
        direction: box.breakout,
    }));
    const positionsByBroker = buildPositionsByBroker(input.currentPositions);
    const patternSnapshot = buildPatternSnapshot(input, maPayload, currentPrice, lastDaily);
    const patternSnapshotJson = JSON.stringify(patternSnapshot, null, 2);

    // Build signals text
    const signalsText =
        input.signals.length > 0
            ? input.signals.map((s) => `${s.label}(${s.kind})`).join(", ")
            : "なし";

    // Build boxes text  
    const boxesText = input.showBoxes && input.boxes.length
        ? input.boxes
            .slice(-3)
            .map((b) => `${formatDate(b.startTime)}〜${formatDate(b.endTime)}: ${formatNumber(b.lower, 0)}〜${formatNumber(b.upper, 0)} (${b.breakout || "未ブレイク"})`)
            .join("\n    ")
        : "表示OFF or なし";

    // Build markdown
    const headerJson = JSON.stringify(
        {
            schemaVersion: "ai_export_v2",
            exportedAt,
            ticker: input.code,
            name: input.name ?? null,
            lastDate: lastDaily ? formatDate(lastDaily.time) : null,
            lastClose: lastDaily ? lastDaily.close : null,
            ma: maPayload,
            boxes: boxesPayload,
            positions: positionsByBroker,
            volumeUnit: VOLUME_UNIT,
        },
        null,
        2
    );
    const markdown = `\`\`\`json
${headerJson}
\`\`\`

# AI相談用 銘柄情報エクスポート

## 基本情報
- 銘柄コード: ${input.code}
- 銘柄名: ${input.name ?? N_A}
- エクスポート日時: ${exportedAt}
- schemaVersion: ai_export_v2
- volumeUnit: ${VOLUME_UNIT}

## 表示設定
- 表示足: ${input.visibleTimeframe === "daily" ? "日足" : input.visibleTimeframe === "weekly" ? "週足" : "月足"}
- 表示期間: ${input.rangeMonths ? `${input.rangeMonths}ヶ月` : "全期間"}
- Boxes表示: ${input.showBoxes ? "ON" : "OFF"}
- Positions表示: ${input.showPositions ? "ON" : "OFF"}

## 現在値・移動平均
- 現在値(終値): ${formatNumber(currentPrice, 2)}
- 最終日付: ${lastDaily ? formatDate(lastDaily.time) : N_A}

### 日足MA設定
- 有効MA: ${getActiveMAList(input.maSettings.daily)}
- MA値: ${computeMAValues(input.dailyBars, input.maSettings.daily)}

### 週足MA設定
- 有効MA: ${getActiveMAList(input.maSettings.weekly)}
- MA値: ${computeMAValues(input.weeklyBars, input.maSettings.weekly)}

### 月足MA設定
- 有効MA: ${getActiveMAList(input.maSettings.monthly)}
- MA値: ${computeMAValues(input.monthlyBars, input.maSettings.monthly)}

## シグナル/バッジ
${signalsText}

## Box情報
${boxesText}

## Positions
${(() => {
  const positions = input.currentPositions ?? [];
  if (!positions.length) return "N/A";
  const lines = positions.map((pos) => {
    const label = pos.brokerLabel ?? pos.brokerKey ?? "TOTAL";
    const longLots = formatNumber(pos.longLots ?? 0, 0);
    const shortLots = formatNumber(pos.shortLots ?? 0, 0);
    const avgLong = pos.avgLongPrice != null ? formatNumber(pos.avgLongPrice, 2) : N_A;
    const avgShort = pos.avgShortPrice != null ? formatNumber(pos.avgShortPrice, 2) : N_A;
    const pnl = pos.realizedPnL != null ? formatNumber(pos.realizedPnL, 0) : N_A;
    return `- ${label}: buy=${longLots} / sell=${shortLots} / avgLong=${avgLong} / avgShort=${avgShort} / pnl=${pnl}`;
  });
  return lines.join("\n");
})()}

## Positions Total
${(() => {
  const positions = input.currentPositions ?? [];
  const totals = positions.reduce(
    (acc, pos) => {
      const buy = Number(pos.longLots ?? 0);
      const sell = Number(pos.shortLots ?? 0);
      acc.buy += Number.isFinite(buy) ? buy : 0;
      acc.sell += Number.isFinite(sell) ? sell : 0;
      return acc;
    },
    { buy: 0, sell: 0 }
  );
  return `sell=${totals.sell} / buy=${totals.buy} / text=${totals.sell}-${totals.buy}`;
})()}

## Memos (Daily)
${(() => {
  const entries = Object.entries(dailyMemos);
  if (!entries.length) return "N/A";
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  const tail = entries.slice(-30);
  return tail.map(([date, memo]) => `- ${date}: ${memo || ""}`).join("\n");
})()}

## Pattern Snapshot (AI Friendly)
\`\`\`json
${patternSnapshotJson}
\`\`\`

## OHLCV データ

### 日足 (直近120本)
\`\`\`csv
${buildOHLCVCsv(input.dailyBars, 120, dailyMemos)}
\`\`\`

### 週足 (直近60本)
\`\`\`csv
${buildOHLCVCsv(input.weeklyBars, 60)}
\`\`\`

### 月足 (直近36本)
\`\`\`csv
${buildOHLCVCsv(input.monthlyBars, 36)}
\`\`\`
`;

    // Build JSON
    const json = {
        schemaVersion: "ai_export_v2",
        code: input.code,
        name: input.name ?? null,
        exportedAt,
        settings: {
            visibleTimeframe: input.visibleTimeframe,
            rangeMonths: input.rangeMonths,
            showBoxes: input.showBoxes,
            showPositions: input.showPositions,
        },
        currentPrice,
        lastDate: lastDaily ? formatDate(lastDaily.time) : null,
        ma: maPayload,
        signals: input.signals,
        boxes: boxesPayload,
        memos: dailyMemos,
        positions: input.currentPositions ?? [],
        positions_by_broker: positionsByBroker,
        volumeUnit: VOLUME_UNIT,
        pattern_snapshot: patternSnapshot,
        positions_total: (() => {
            const totals = (input.currentPositions ?? []).reduce(
                (acc, pos) => {
                    const buy = Number(pos.longLots ?? 0);
                    const sell = Number(pos.shortLots ?? 0);
                    acc.buy += Number.isFinite(buy) ? buy : 0;
                    acc.sell += Number.isFinite(sell) ? sell : 0;
                    return acc;
                },
                { buy: 0, sell: 0 }
            );
            return {
                sell: totals.sell,
                buy: totals.buy,
                text: `${totals.sell}-${totals.buy}`,
            };
        })(),
        candle_shapes: {
            daily: dailyAnalysis.map((entry) => ({ date: entry.date, shape: entry.shape })),
            weekly: weeklyAnalysis.map((entry) => ({ date: entry.date, shape: entry.shape })),
            monthly: monthlyAnalysis.map((entry) => ({ date: entry.date, shape: entry.shape })),
        },
        candle_signals: {
            daily: dailyAnalysis.map((entry) => ({ date: entry.date, signal: entry.signal })),
            weekly: weeklyAnalysis.map((entry) => ({ date: entry.date, signal: entry.signal })),
            monthly: monthlyAnalysis.map((entry) => ({ date: entry.date, signal: entry.signal })),
        },
        ohlcv: {
            daily: input.dailyBars.slice(-120).map((b) => {
                const date = formatDate(b.time);
                const analysis = dailyAnalysis.find((entry) => entry.date === date);
                const volume = b.volume == null || !Number.isFinite(b.volume) ? null : b.volume;
                return {
                    date,
                    o: b.open,
                    h: b.high,
                    l: b.low,
                    c: b.close,
                    v: volume,
                    shape: analysis?.shape ?? "none",
                    signal: analysis?.signal ?? "none",
                    memo: dailyMemos[date] ?? ""
                };
            }),
            weekly: input.weeklyBars.slice(-60).map((b) => {
                const date = formatDate(b.time);
                const analysis = weeklyAnalysis.find((entry) => entry.date === date);
                const volume = b.volume == null || !Number.isFinite(b.volume) ? null : b.volume;
                return {
                    date,
                    o: b.open,
                    h: b.high,
                    l: b.low,
                    c: b.close,
                    v: volume,
                    shape: analysis?.shape ?? "none",
                    signal: analysis?.signal ?? "none"
                };
            }),
            monthly: input.monthlyBars.slice(-36).map((b) => {
                const date = formatDate(b.time);
                const analysis = monthlyAnalysis.find((entry) => entry.date === date);
                const volume = b.volume == null || !Number.isFinite(b.volume) ? null : b.volume;
                return {
                    date,
                    o: b.open,
                    h: b.high,
                    l: b.low,
                    c: b.close,
                    v: volume,
                    shape: analysis?.shape ?? "none",
                    signal: analysis?.signal ?? "none"
                };
            }),
        },
    };


    return { markdown, json };
};

export const copyToClipboard = async (text: string): Promise<boolean> => {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch {
        // Fallback
        const textarea = document.createElement("textarea");
        textarea.value = text;
        textarea.style.position = "fixed";
        textarea.style.opacity = "0";
        document.body.appendChild(textarea);
        textarea.select();
        const success = document.execCommand("copy");
        document.body.removeChild(textarea);
        return success;
    }
};

export const saveAsFile = async (
    content: string,
    filename: string,
    mimeType: string
): Promise<boolean> => {
    try {
        if ("showSaveFilePicker" in window) {
            const lower = filename.toLowerCase();
            const extension = lower.endsWith(".json")
                ? ".json"
                : lower.endsWith(".csv")
                    ? ".csv"
                    : lower.endsWith(".ebk")
                        ? ".ebk"
                    : lower.endsWith(".txt")
                        ? ".txt"
                        : ".md";
            const description =
                extension === ".json"
                    ? "JSON File"
                    : extension === ".csv"
                        ? "CSV File"
                        : extension === ".ebk"
                            ? "EBK File"
                        : extension === ".txt"
                            ? "Text File"
                            : "Markdown File";
            const handle = await (window as unknown as { showSaveFilePicker: (options: object) => Promise<FileSystemFileHandle> }).showSaveFilePicker({
                suggestedName: filename,
                types: [
                    {
                        description,
                        accept: { [mimeType]: [extension] },
                    },
                ],
            });
            const writable = await handle.createWritable();
            await writable.write(content);
            await writable.close();
            return true;
        }
    } catch (error) {
        if ((error as Error).name === "AbortError") {
            return false; // User cancelled
        }
    }

    // Fallback: download
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    return true;
};
