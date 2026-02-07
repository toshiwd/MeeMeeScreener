import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import { IconPointer, IconPointerOff } from "@tabler/icons-react";
import "./DailyMemoPanel.css";

interface DailyMemoPanelProps {
    code: string;
    selectedDate: string | null;
    selectedBarData: {
        time: number;
        open: number;
        high: number;
        low: number;
        close: number;
        volume?: number;
    } | null;
    maValues?: {
        ma7?: number;
        ma20?: number;
        ma60?: number;
        ma100?: number;
        ma200?: number;
    };
    maTrends?: {
        ma7?: string;
        ma20?: string;
        ma60?: string;
        ma100?: string;
        ma200?: string;
    };
    position?: {
        buy: number;
        sell: number;
    };
    prevDayData?: {
        close: number;
        change: number;
        changePercent: number;
    };
    cursorMode: boolean;
    onToggleCursorMode: () => void;
    onPrevDay: () => void;
    onNextDay: () => void;
    onCopyForConsult: () => void;
}

interface MemoData {
    memo: string;
    updated_at: string | null;
}

export default function DailyMemoPanel({
    code,
    selectedDate,
    selectedBarData,
    maValues,
    maTrends,
    position,
    prevDayData,
    cursorMode,
    onToggleCursorMode,
    onPrevDay,
    onNextDay,
    onCopyForConsult,
}: DailyMemoPanelProps) {
    const [memo, setMemo] = useState("");
    const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
    const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);
    const [errorMessage, setErrorMessage] = useState<string | null>(null);

    const saveTimeoutRef = useRef<number | null>(null);
    const lastSavedKeyRef = useRef<string | null>(null);

    // Load memo when date changes
    useEffect(() => {
        if (!code || !selectedDate) {
            setMemo("");
            setLastSavedAt(null);
            return;
        }

        const loadMemo = async () => {
            try {
                const response = await api.get("/memo", {
                    params: { symbol: code, date: selectedDate, timeframe: "D" },
                });
                const data = response.data as MemoData;
                setMemo(data.memo || "");
                setLastSavedAt(data.updated_at);
                setSaveStatus("idle");
                lastSavedKeyRef.current = `${code}-${selectedDate}`;
            } catch (error) {
                console.error("Failed to load memo:", error);
                setMemo("");
                setLastSavedAt(null);
            }
        };

        loadMemo();
    }, [code, selectedDate]);

    // Save memo with debounce
    const saveMemo = async (memoText: string, forceKey?: string) => {
        const saveKey = forceKey || `${code}-${selectedDate}`;

        // Prevent saving if key has changed (user moved to different date)
        if (saveKey !== `${code}-${selectedDate}`) {
            if (import.meta.env.MODE === "development") {
                console.log("Save cancelled: key mismatch", saveKey, `${code}-${selectedDate}`);
            }
            return;
        }

        if (!code || !selectedDate) return;

        setSaveStatus("saving");
        setErrorMessage(null);

        try {
            const response = await api.put("/memo", {
                symbol: code,
                date: selectedDate,
                timeframe: "D",
                memo: memoText.trim(),
            });

            const data = response.data as { ok: boolean; updated_at?: string; deleted?: boolean };

            if (data.ok) {
                setSaveStatus("saved");
                setLastSavedAt(data.updated_at || null);
                lastSavedKeyRef.current = saveKey;

                // Clear saved status after 3 seconds
                setTimeout(() => {
                    if (saveStatus === "saved") setSaveStatus("idle");
                }, 3000);
            }
        } catch (error: any) {
            console.error("Failed to save memo:", error);
            setSaveStatus("error");
            setErrorMessage(error.response?.data?.error || "保存に失敗しました");
        }
    };

    // Auto-save with debounce
    const handleMemoChange = (value: string) => {
        // Enforce 100 character limit
        if (value.length > 100) {
            return;
        }

        setMemo(value);
        setSaveStatus("idle");

        // Clear existing timeout
        if (saveTimeoutRef.current) {
            clearTimeout(saveTimeoutRef.current);
        }

        // Set new timeout for auto-save (800ms debounce)
        saveTimeoutRef.current = setTimeout(() => {
            saveMemo(value);
        }, 800);
    };

    // Save before date change
    const handleDateChange = (direction: "prev" | "next") => {
        // Save current memo if there are unsaved changes
        if (saveTimeoutRef.current) {
            clearTimeout(saveTimeoutRef.current);
            saveMemo(memo, `${code}-${selectedDate}`);
        }

        // Navigate
        if (direction === "prev") {
            onPrevDay();
        } else {
            onNextDay();
        }
    };

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            if (saveTimeoutRef.current) {
                clearTimeout(saveTimeoutRef.current);
                // Note: We can't await here, but the save will still execute
                saveMemo(memo, `${code}-${selectedDate}`);
            }
        };
    }, []);

    const formatDate = (dateStr: string | null) => {
        if (!dateStr) return "—";
        const date = new Date(dateStr);
        const days = ["日", "月", "火", "水", "木", "金", "土"];
        const dayOfWeek = days[date.getDay()];
        return `${dateStr} (${dayOfWeek})`;
    };

    const formatTime = (timestamp: string | null) => {
        if (!timestamp) return "";
        const date = new Date(timestamp);
        return date.toLocaleTimeString("ja-JP", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    };

    const formatNumber = (value: number | null | undefined, decimals = 0) => {
        if (value == null) return "—";
        return value.toLocaleString("ja-JP", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    };

    const remainingChars = 100 - memo.length;

    return (
        <div className="daily-memo-panel">
            <div className="memo-panel-header">
                <h3>日足カーソル</h3>
                <button
                    type="button"
                    className={`cursor-mode-toggle ${cursorMode ? "active" : ""}`}
                    onClick={onToggleCursorMode}
                    title="カーソルモード切替 (C)"
                >
                    {cursorMode ? (
                        <>
                            <IconPointer size={16} />
                            <span>カーソルON</span>
                        </>
                    ) : (
                        <>
                            <IconPointerOff size={16} />
                            <span>カーソルOFF</span>
                        </>
                    )}
                </button>
            </div>

            {selectedDate && selectedBarData ? (
                <>
                    <div className="memo-panel-info">
                        <div className="info-header">
                            <div className="info-date">{formatDate(selectedDate)}</div>
                            <div className="info-nav">
                                <button
                                    type="button"
                                    className="nav-btn"
                                    onClick={() => handleDateChange("prev")}
                                    title="前日 (←)"
                                >
                                    ←
                                </button>
                                <button
                                    type="button"
                                    className="nav-btn"
                                    onClick={() => handleDateChange("next")}
                                    title="翌日 (→)"
                                >
                                    →
                                </button>
                            </div>
                        </div>

                        {prevDayData && (
                            <div className="prev-day-info">
                                前日比 {prevDayData.change >= 0 ? '+' : ''}{formatNumber(prevDayData.change)}
                                ({prevDayData.changePercent >= 0 ? '+' : ''}{prevDayData.changePercent.toFixed(2)}%)
                            </div>
                        )}

                        <div className="info-section">
                            <div className="info-grid-2col">
                                <div>O</div><div>{formatNumber(selectedBarData.open)}</div>
                                <div>H</div><div>{formatNumber(selectedBarData.high)}</div>
                                <div>L</div><div>{formatNumber(selectedBarData.low)}</div>
                                <div>C</div><div>{formatNumber(selectedBarData.close)}</div>
                            </div>
                        </div>

                        {selectedBarData.volume != null && (
                            <div className="info-section">
                                <div className="info-label">出来高</div>
                                <div className="info-value">{formatNumber(selectedBarData.volume)}</div>
                            </div>
                        )}

                        {maValues && (
                            <div className="info-section">
                                <div className="info-label">MA</div>
                                <div className="ma-list">
                                    {maValues.ma7 != null && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#ef4444' }}>MA1</span>
                                            <span className="ma-value">{formatNumber(maValues.ma7)}</span>
                                        </div>
                                    )}
                                    {maValues.ma20 != null && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#22c55e' }}>MA2</span>
                                            <span className="ma-value">{formatNumber(maValues.ma20)}</span>
                                        </div>
                                    )}
                                    {maValues.ma60 != null && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#3b82f6' }}>MA3</span>
                                            <span className="ma-value">{formatNumber(maValues.ma60)}</span>
                                        </div>
                                    )}
                                    {maValues.ma100 != null && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#a855f7' }}>MA4</span>
                                            <span className="ma-value">{formatNumber(maValues.ma100)}</span>
                                        </div>
                                    )}
                                    {maValues.ma200 != null && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#f97316' }}>MA5</span>
                                            <span className="ma-value">{formatNumber(maValues.ma200)}</span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}

                        {maTrends && (
                            <div className="info-section">
                                <div className="info-label">本数</div>
                                <div className="ma-list">
                                    {maTrends.ma7 && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#ef4444' }}>MA1</span>
                                            <span className="ma-value">{maTrends.ma7}</span>
                                        </div>
                                    )}
                                    {maTrends.ma20 && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#22c55e' }}>MA2</span>
                                            <span className="ma-value">{maTrends.ma20}</span>
                                        </div>
                                    )}
                                    {maTrends.ma60 && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#3b82f6' }}>MA3</span>
                                            <span className="ma-value">{maTrends.ma60}</span>
                                        </div>
                                    )}
                                    {maTrends.ma100 && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#a855f7' }}>MA4</span>
                                            <span className="ma-value">{maTrends.ma100}</span>
                                        </div>
                                    )}
                                    {maTrends.ma200 && (
                                        <div className="ma-item">
                                            <span className="ma-name" style={{ color: '#f97316' }}>MA5</span>
                                            <span className="ma-value">{maTrends.ma200}</span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}

                        {position && (
                            <div className="info-section">
                                <div className="info-label">建玉</div>
                                <div className="position-info">
                                    {position.buy > 0 || position.sell > 0 ? (
                                        <div className="position-value">{position.sell} / {position.buy}</div>
                                    ) : (
                                        <div className="position-empty">N/A</div>
                                    )}
                                </div>
                            </div>
                        )}

                        <div className="info-actions">
                            <button
                                type="button"
                                className="consult-copy-btn"
                                onClick={onCopyForConsult}
                                title="相談用にコピー"
                            >
                                📋 相談用にコピー
                            </button>
                        </div>
                    </div>

                    <div className="memo-panel-input">
                        <div className="memo-header">
                            <label htmlFor="daily-memo">メモ (100字以内)</label>
                            <div className="memo-status">
                                {saveStatus === "saving" && <span className="status-saving">保存中...</span>}
                                {saveStatus === "saved" && lastSavedAt && (
                                    <span className="status-saved">保存済み {formatTime(lastSavedAt)}</span>
                                )}
                                {saveStatus === "error" && (
                                    <span className="status-error">保存失敗 (再試行)</span>
                                )}
                                <span className={`char-count ${remainingChars < 10 ? "warning" : ""}`}>
                                    残り {remainingChars}
                                </span>
                            </div>
                        </div>
                        <textarea
                            id="daily-memo"
                            className="memo-textarea"
                            value={memo}
                            onChange={(e) => handleMemoChange(e.target.value)}
                            placeholder="この日の気づきやメモを入力..."
                            maxLength={100}
                            rows={3}
                        />
                        {errorMessage && <div className="memo-error">{errorMessage}</div>}
                    </div>
                </>
            ) : (
                <div className="memo-panel-empty">
                    <p>カーソルモードをONにして、日足チャートをクリックしてください</p>
                </div>
            )}
        </div>
    );
}
