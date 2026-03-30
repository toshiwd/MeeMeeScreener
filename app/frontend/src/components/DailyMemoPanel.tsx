import { useEffect, useRef, useState } from "react";
import { api } from "../api";
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
  title?: string;
  cursorMode?: boolean;
  onToggleCursorMode?: () => void;
  onPrevDay: () => void;
  onNextDay: () => void;
  onCopyForConsult: () => void;
}

interface MemoData {
  memo: string;
  updated_at: string | null;
}

const MEMO_MAX_LENGTH = 100;
const SAVE_DEBOUNCE_MS = 800;
const SAVED_BADGE_MS = 3000;

export default function DailyMemoPanel({
  code,
  selectedDate,
  selectedBarData,
  maValues,
  position,
  prevDayData,
  title = "日足情報",
  cursorMode = true,
  onPrevDay,
  onNextDay,
  onCopyForConsult,
}: DailyMemoPanelProps) {
  const [memo, setMemo] = useState("");
  const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const saveTimeoutRef = useRef<number | null>(null);
  const savedStatusTimeoutRef = useRef<number | null>(null);
  const loadRequestSeqRef = useRef(0);
  const latestMemoRef = useRef("");
  const latestCodeRef = useRef(code);
  const latestSelectedDateRef = useRef<string | null>(selectedDate);
  const saveMemoRef = useRef<((memoText: string, forceKey?: string) => Promise<void>) | null>(null);

  latestMemoRef.current = memo;
  latestCodeRef.current = code;
  latestSelectedDateRef.current = selectedDate;

  const clearSaveTimeout = () => {
    if (saveTimeoutRef.current != null) {
      window.clearTimeout(saveTimeoutRef.current);
      saveTimeoutRef.current = null;
    }
  };

  const clearSavedStatusTimeout = () => {
    if (savedStatusTimeoutRef.current != null) {
      window.clearTimeout(savedStatusTimeoutRef.current);
      savedStatusTimeoutRef.current = null;
    }
  };

  useEffect(() => {
    loadRequestSeqRef.current += 1;
    const requestSeq = loadRequestSeqRef.current;
    clearSaveTimeout();
    clearSavedStatusTimeout();

    if (!code || !selectedDate) {
      setMemo("");
      setLastSavedAt(null);
      setSaveStatus("idle");
      setErrorMessage(null);
      return;
    }

    const controller = new AbortController();
    setMemo("");
    setLastSavedAt(null);
    setSaveStatus("idle");
    setErrorMessage(null);

    void api
      .get("/memo", {
        params: { symbol: code, date: selectedDate, timeframe: "D" },
        signal: controller.signal,
      })
      .then((response) => {
        if (requestSeq !== loadRequestSeqRef.current) return;
        const data = response.data as MemoData;
        setMemo(data.memo || "");
        setLastSavedAt(data.updated_at);
        setSaveStatus("idle");
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || requestSeq !== loadRequestSeqRef.current) return;
        console.error("Failed to load memo:", error);
        setMemo("");
        setLastSavedAt(null);
      });

    return () => {
      controller.abort();
    };
  }, [code, selectedDate]);

  const saveMemo = async (memoText: string, forceKey?: string) => {
    const saveKey = forceKey || `${code}-${selectedDate}`;
    if (saveKey !== `${code}-${selectedDate}`) return;
    if (!code || !selectedDate) return;

    clearSavedStatusTimeout();
    setSaveStatus("saving");
    setErrorMessage(null);

    try {
      const response = await api.put("/memo", {
        symbol: code,
        date: selectedDate,
        timeframe: "D",
        memo: memoText.trim(),
      });
      const data = response.data as { ok: boolean; updated_at?: string };
      if (!data.ok) return;
      setSaveStatus("saved");
      setLastSavedAt(data.updated_at || null);
      savedStatusTimeoutRef.current = window.setTimeout(() => {
        setSaveStatus((current) => (current === "saved" ? "idle" : current));
        savedStatusTimeoutRef.current = null;
      }, SAVED_BADGE_MS);
    } catch (error: any) {
      console.error("Failed to save memo:", error);
      setSaveStatus("error");
      setErrorMessage(error.response?.data?.error || "メモの保存に失敗しました");
    }
  };
  saveMemoRef.current = saveMemo;

  const handleMemoChange = (value: string) => {
    if (value.length > MEMO_MAX_LENGTH) return;

    setMemo(value);
    setSaveStatus("idle");
    clearSaveTimeout();
    clearSavedStatusTimeout();

    saveTimeoutRef.current = window.setTimeout(() => {
      void saveMemo(value);
      saveTimeoutRef.current = null;
    }, SAVE_DEBOUNCE_MS);
  };

  const handleDateChange = (direction: "prev" | "next") => {
    const shouldFlushPendingSave = saveTimeoutRef.current != null;
    clearSaveTimeout();
    clearSavedStatusTimeout();
    if (shouldFlushPendingSave) {
      void saveMemo(memo, `${code}-${selectedDate}`);
    }

    if (direction === "prev") {
      onPrevDay();
      return;
    }
    onNextDay();
  };

  useEffect(() => {
    return () => {
      const shouldFlushPendingSave = saveTimeoutRef.current != null;
      clearSaveTimeout();
      clearSavedStatusTimeout();
      if (shouldFlushPendingSave) {
        void saveMemoRef.current?.(
          latestMemoRef.current,
          `${latestCodeRef.current}-${latestSelectedDateRef.current}`
        );
      }
    };
  }, []);

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return "--";
    const date = new Date(dateStr);
    const days = ["日", "月", "火", "水", "木", "金", "土"];
    return `${dateStr} (${days[date.getDay()]})`;
  };

  const formatTime = (timestamp: string | null) => {
    if (!timestamp) return "";
    const date = new Date(timestamp);
    return date.toLocaleTimeString("ja-JP", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const formatNumber = (value: number | null | undefined, decimals = 0) => {
    if (value == null) return "--";
    return value.toLocaleString("ja-JP", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  };

  if (!selectedDate || !selectedBarData) {
    return null;
  }

  const remainingChars = MEMO_MAX_LENGTH - memo.length;

  return (
    <div className="daily-memo-panel">
      <div className="memo-panel-header">
        <h3>{title}</h3>
      </div>

      {!cursorMode ? (
        <div className="memo-panel-empty">カーソルをONにすると日足情報を表示します</div>
      ) : (
        <div className="memo-panel-info">
          <div className="info-header">
            <div className="info-date">{formatDate(selectedDate)}</div>
            <div className="info-nav">
              <button type="button" className="nav-btn" onClick={() => handleDateChange("prev")}>
                前日
              </button>
              <button type="button" className="nav-btn" onClick={() => handleDateChange("next")}>
                次日
              </button>
            </div>
          </div>
          <div className="prev-day-info">
            <span className="info-label">前日比</span>
            <div
              className={`price-change ${
                prevDayData && prevDayData.change > 0
                  ? "positive"
                  : prevDayData && prevDayData.change < 0
                    ? "negative"
                    : ""
              }`}
            >
              {prevDayData
                ? `${prevDayData.change > 0 ? "+" : ""}${formatNumber(prevDayData.change, 0)} (${formatNumber(prevDayData.changePercent, 1)}%)`
                : "--"}
            </div>
          </div>
          <div className="ohlc-grid">
            <div className="ohlc-item">
              <span>O</span>
              <span>{formatNumber(selectedBarData.open, 0)}</span>
            </div>
            <div className="ohlc-item">
              <span>H</span>
              <span>{formatNumber(selectedBarData.high, 0)}</span>
            </div>
            <div className="ohlc-item">
              <span>L</span>
              <span>{formatNumber(selectedBarData.low, 0)}</span>
            </div>
            <div className="ohlc-item">
              <span>C</span>
              <span>{formatNumber(selectedBarData.close, 0)}</span>
            </div>
            <div className="ohlc-item">
              <span>出来高</span>
              <span>{formatNumber(selectedBarData.volume, 0)}</span>
            </div>
          </div>
          <div className="memo-meta-row">
            {maValues && (
              <div className="memo-ma-list">
                {maValues.ma7 != null && <div className="memo-ma-item">7MA {formatNumber(maValues.ma7, 0)}</div>}
                {maValues.ma20 != null && <div className="memo-ma-item">20MA {formatNumber(maValues.ma20, 0)}</div>}
                {maValues.ma60 != null && <div className="memo-ma-item">60MA {formatNumber(maValues.ma60, 0)}</div>}
                {maValues.ma100 != null && <div className="memo-ma-item">100MA {formatNumber(maValues.ma100, 0)}</div>}
                {maValues.ma200 != null && <div className="memo-ma-item">200MA {formatNumber(maValues.ma200, 0)}</div>}
              </div>
            )}
            {position && (
              <div className="memo-position-row">
                <span>建玉</span>
                <span>買い {formatNumber(position.buy, 0)}</span>
                <span>売り {formatNumber(position.sell, 0)}</span>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="memo-section">
        <div className="memo-section-header">
          <span>日付メモ (100字以内)</span>
          {lastSavedAt && <span className="memo-saved-at">更新 {formatTime(lastSavedAt)}</span>}
        </div>
        <textarea
          className="memo-textarea"
          value={memo}
          onChange={(event) => handleMemoChange(event.target.value)}
          maxLength={MEMO_MAX_LENGTH}
          placeholder="メモを入力..."
        />
        <div className="memo-footer">
          <span className={`memo-status memo-status-${saveStatus}`}>
            {errorMessage ||
              (saveStatus === "saving"
                ? "保存中..."
                : saveStatus === "saved"
                  ? "保存済み"
                  : "")}
          </span>
          <span className="memo-remaining">{remainingChars}文字</span>
        </div>
      </div>

      <div className="memo-section">
        <button type="button" className="nav-btn" onClick={onCopyForConsult}>
          相談用にコピー
        </button>
      </div>
    </div>
  );
}
