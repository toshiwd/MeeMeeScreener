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
    volume?: number;
};

type SignalData = {
    label: string;
    kind: "warning" | "achieved";
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
    // Current values (optional)
    currentPrice?: number | null;
};

type AIExportResult = {
    markdown: string;
    json: object;
};

const N_A = "N/A";

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
    return value.toLocaleString("ja-JP", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
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

const buildOHLCVCsv = (bars: BarData[], limit: number): string => {
    const sliced = bars.slice(-limit);
    const lines = ["date,open,high,low,close,volume"];
    sliced.forEach((bar) => {
        lines.push(
            `${formatDate(bar.time)},${bar.open},${bar.high},${bar.low},${bar.close},${bar.volume ?? 0}`
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

export const buildAIExport = (input: AIExportInput): AIExportResult => {
    const now = new Date();
    const timestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")} ${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;

    const lastDaily = input.dailyBars.length ? input.dailyBars[input.dailyBars.length - 1] : null;
    const currentPrice = input.currentPrice ?? lastDaily?.close ?? null;

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
    const markdown = `# AI相談用 銘柄情報エクスポート

## 基本情報
- 銘柄コード: ${input.code}
- 銘柄名: ${input.name ?? N_A}
- エクスポート日時: ${timestamp}

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

## OHLCV データ

### 日足 (直近120本)
\`\`\`csv
${buildOHLCVCsv(input.dailyBars, 120)}
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
        code: input.code,
        name: input.name ?? null,
        exportedAt: now.toISOString(),
        settings: {
            visibleTimeframe: input.visibleTimeframe,
            rangeMonths: input.rangeMonths,
            showBoxes: input.showBoxes,
            showPositions: input.showPositions,
        },
        currentPrice,
        lastDate: lastDaily ? formatDate(lastDaily.time) : null,
        maSettings: {
            daily: input.maSettings.daily.filter((m) => m.visible).map((m) => ({ period: m.period, value: computeMA(input.dailyBars, m.period) })),
            weekly: input.maSettings.weekly.filter((m) => m.visible).map((m) => ({ period: m.period, value: computeMA(input.weeklyBars, m.period) })),
            monthly: input.maSettings.monthly.filter((m) => m.visible).map((m) => ({ period: m.period, value: computeMA(input.monthlyBars, m.period) })),
        },
        signals: input.signals,
        boxes: input.boxes.slice(-5),
        ohlcv: {
            daily: input.dailyBars.slice(-120).map((b) => ({ date: formatDate(b.time), o: b.open, h: b.high, l: b.low, c: b.close, v: b.volume ?? 0 })),
            weekly: input.weeklyBars.slice(-60).map((b) => ({ date: formatDate(b.time), o: b.open, h: b.high, l: b.low, c: b.close, v: b.volume ?? 0 })),
            monthly: input.monthlyBars.slice(-36).map((b) => ({ date: formatDate(b.time), o: b.open, h: b.high, l: b.low, c: b.close, v: b.volume ?? 0 })),
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
