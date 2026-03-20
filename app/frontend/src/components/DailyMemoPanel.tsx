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
  position,
  prevDayData,
  title = "日足情報",
  onPrevDay,
  onNextDay,
  onCopyForConsult,
}: DailyMemoPanelProps) {
  const [memo, setMemo] = useState("");
  const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const saveTimeoutRef = useRef<number | null>(null);
  const latestMemoRef = useRef("");
  const latestCodeRef = useRef(code);
  const latestSelectedDateRef = useRef<string | null>(selectedDate);
  const saveMemoRef = useRef<((memoText: string, forceKey?: string) => Promise<void>) | null>(null);

  latestMemoRef.current = memo;
  latestCodeRef.current = code;
  latestSelectedDateRef.current = selectedDate;

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
      } catch (error) {
        console.error("Failed to load memo:", error);
        setMemo("");
        setLastSavedAt(null);
      }
    };

    loadMemo();
  }, [code, selectedDate]);

  const saveMemo = async (memoText: string, forceKey?: string) => {
    const saveKey = forceKey || `${code}-${selectedDate}`;
    if (saveKey !== `${code}-${selectedDate}`) return;
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
        setTimeout(() => {
          setSaveStatus((current) => (current === "saved" ? "idle" : current));
        }, 3000);
      }
    } catch (error: any) {
      console.error("Failed to save memo:", error);
      setSaveStatus("error");
      setErrorMessage(error.response?.data?.error || "メモの保存に失敗しました");
    }
  };
  saveMemoRef.current = saveMemo;

  const handleMemoChange = (value: string) => {
    if (value.length > 100) return;

    setMemo(value);
    setSaveStatus("idle");

    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
    }

    saveTimeoutRef.current = setTimeout(() => {
      void saveMemo(value);
    }, 800);
  };

  const handleDateChange = (direction: "prev" | "next") => {
    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
      void saveMemo(memo, `${code}-${selectedDate}`);
    }

    if (direction === "prev") {
      onPrevDay();
    } else {
      onNextDay();
    }
  };

  useEffect(() => {
    return () => {
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
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
    return value.toLocaleString("ja-JP", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  };

  if (!selectedDate || !selectedBarData) {
    return null;
  }

  const remainingChars = 100 - memo.length;

  return (
    <div className="daily-memo-panel">
      <div className="memo-panel-header">
        <h3>{title}</h3>
      </div>

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
        <div className="memo-price-row">
          <span
            className={`price-change ${
              prevDayData && prevDayData.change > 0 ? "positive" : prevDayData && prevDayData.change < 0 ? "negative" : ""
            }`}
          >
            {prevDayData && prevDayData.change !== undefined
              ? `${prevDayData.change > 0 ? "+" : ""}${formatNumber(prevDayData.change, 0)} (${formatNumber(prevDayData.changePercent, 1)}%)`
              : "--"}
          </span>
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

      <div className="memo-section">
        <div className="memo-section-header">
          <span>日付メモ</span>
          {lastSavedAt && <span className="memo-saved-at">更新 {formatTime(lastSavedAt)}</span>}
        </div>
        <textarea
          className="memo-textarea"
          value={memo}
          onChange={(e) => handleMemoChange(e.target.value)}
          maxLength={100}
          placeholder="メモを入力..."
        />
        <div className="memo-footer">
          <span className={`memo-status memo-status-${saveStatus}`}>
            {errorMessage || (saveStatus === "saving" ? "保存中..." : saveStatus === "saved" ? "保存済み" : "")}
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
