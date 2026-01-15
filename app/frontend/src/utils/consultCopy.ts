/**
 * Utility for copying consultation data to clipboard
 */

interface ConsultCopyData {
    symbol: string;
    name: string;
    date: string;
    ohlc: {
        open: number;
        high: number;
        low: number;
        close: number;
    };
    volume?: number;
    position?: {
        sell: number;
        buy: number;
    };
    ma?: {
        ma7?: { value: number; trend: string };
        ma20?: { value: number; trend: string };
        ma60?: { value: number; trend: string };
    };
    signals?: string[];
    memo: string;
}

export function buildConsultCopyText(data: ConsultCopyData): string {
    const lines: string[] = [];

    // 銘柄
    lines.push(`【銘柄】${data.symbol} ${data.name}`);

    // 日付
    lines.push(`【日付】${data.date}（日足カーソル）`);

    // 建玉
    if (data.position) {
        lines.push(`【建玉】売-買=${data.position.sell}-${data.position.buy}`);
    } else {
        lines.push(`【建玉】—`);
    }

    // MA
    if (data.ma) {
        const maStatus: string[] = [];
        if (data.ma.ma7) {
            maStatus.push(`7:${data.ma.ma7.trend}`);
        }
        if (data.ma.ma20) {
            maStatus.push(`20:${data.ma.ma20.trend}`);
        }
        if (data.ma.ma60) {
            maStatus.push(`60:${data.ma.ma60.trend}`);
        }
        lines.push(`【MA】${maStatus.length > 0 ? maStatus.join(" ") : "—"}`);
    } else {
        lines.push(`【MA】—`);
    }

    // シグナル
    if (data.signals && data.signals.length > 0) {
        lines.push(`【シグナル】${data.signals.join(", ")}`);
    } else {
        lines.push(`【シグナル】—`);
    }

    // OHLC
    lines.push(`【OHLC】始値:${data.ohlc.open} 高値:${data.ohlc.high} 安値:${data.ohlc.low} 終値:${data.ohlc.close}`);

    // 出来高
    if (data.volume != null) {
        lines.push(`【出来高】${data.volume.toLocaleString()}`);
    }

    // メモ (必ず含める)
    const memoText = data.memo.trim() || "（コメントなし）";
    lines.push(`【メモ】${memoText}`);

    return lines.join("\n");
}

export async function copyToClipboard(text: string): Promise<boolean> {
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
        } else {
            // Fallback for older browsers
            const textArea = document.createElement("textarea");
            textArea.value = text;
            textArea.style.position = "fixed";
            textArea.style.left = "-999999px";
            textArea.style.top = "-999999px";
            document.body.appendChild(textArea);
            textArea.focus();
            textArea.select();
            const successful = document.execCommand("copy");
            document.body.removeChild(textArea);
            return successful;
        }
    } catch (error) {
        console.error("Failed to copy to clipboard:", error);
        return false;
    }
}
