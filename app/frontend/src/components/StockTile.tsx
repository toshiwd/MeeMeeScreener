// @ts-nocheck
import { memo, type MouseEvent } from "react";
import type { ReactNode } from "react";
import { IconHeart, IconHeartFilled } from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import { Ticker, useStore } from "../store";
import { formatEventBadgeDate, parseEventDateMs } from "../utils/events";
import ThumbnailCanvas from "./ThumbnailCanvas";
import { buildThumbnailCacheKey, getThumbnailCache } from "./thumbnailCache";

type StockTileProps = {
  ticker: Ticker;
  timeframe: "monthly" | "weekly" | "daily";
  maxBars?: number;
  active?: boolean;
  kept?: boolean;
  theme?: "dark" | "light";
  asofLabel?: string | null;
  asofTooltip?: string | null;
  onActivate?: (code: string) => void;
  onOpenDetail: (code: string) => void;
  onToggleKeep?: (code: string) => void;
  onExclude?: (code: string) => void;
  annotation?: ReactNode;
};

const StockTile = memo(function StockTile({
  ticker,
  timeframe,
  maxBars,
  active = false,
  kept = false,
  theme,
  asofLabel,
  asofTooltip,
  onActivate,
  onOpenDetail,
  onToggleKeep,
  onExclude,
  annotation
}: StockTileProps) {
  const DAY_MS = 24 * 60 * 60 * 1000;
  const barsPayload = useStore((state) => {
    const map = state.barsCache?.[timeframe] ?? {};
    return map[ticker.code];
  });
  const { ready: backendReady } = useBackendReadyState();
  const isFavorite = useStore((state) => state.favorites.includes(ticker.code));
  const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
  const boxes = useStore((state) => {
    const map = state.boxesCache?.[timeframe] ?? {};
    return map[ticker.code] ?? [];
  });
  const barsStatus = useStore((state) => {
    const map = state.barsStatus?.[timeframe] ?? {};
    return map[ticker.code] ?? "idle";
  });
  const maSettings = useStore((state) => {
    const map = state.maSettings;
    if (!map) return [];
    return timeframe === "daily"
      ? map.daily ?? []
      : timeframe === "weekly"
      ? map.weekly ?? []
      : map.monthly ?? [];
  });
  const showBoxes = useStore((state) => state.settings.showBoxes);
  const cacheKey = buildThumbnailCacheKey(ticker.code, timeframe, showBoxes, maSettings, theme ?? "dark");
  const cachedThumb = getThumbnailCache(cacheKey);
  const earningsLabel = formatEventBadgeDate(ticker.eventEarningsDate);
  const rightsLabel = formatEventBadgeDate(ticker.eventRightsDate);
  const bars = barsPayload?.bars ?? [];
  const latestBar = bars.length ? bars[bars.length - 1] : null;
  const prevBar = bars.length > 1 ? bars[bars.length - 2] : null;
  const latestBarTime = Number.isFinite(latestBar?.[0]) ? Number(latestBar[0]) : null;
  const latestBarMs = (() => {
    if (latestBarTime == null) return null;
    const value = Math.trunc(latestBarTime);
    if (value >= 10000000 && value < 100000000) {
      const year = Math.floor(value / 10000);
      const month = Math.floor((value % 10000) / 100) - 1;
      const day = value % 100;
      return Date.UTC(year, month, day);
    }
    if (value >= 1000000000000) return value;
    if (value >= 1000000000) return value * 1000;
    return null;
  })();
  const formatBarDate = (value: number | null | undefined) => {
    if (!Number.isFinite(value)) return "--";
    const raw = Math.trunc(value);
    if (raw >= 10000000 && raw < 100000000) {
      const year = Math.floor(raw / 10000);
      const month = String(Math.floor((raw % 10000) / 100)).padStart(2, "0");
      const day = String(raw % 100).padStart(2, "0");
      return `${String(year % 100).padStart(2, "0")}/${month}/${day}`;
    }
    if (raw >= 1000000000000) {
      const date = new Date(raw);
      if (Number.isNaN(date.getTime())) return "--";
      return `${String(date.getUTCFullYear() % 100).padStart(2, "0")}/${String(
        date.getUTCMonth() + 1
      ).padStart(2, "0")}/${String(date.getUTCDate()).padStart(2, "0")}`;
    }
    if (raw >= 1000000000) {
      const date = new Date(raw * 1000);
      if (Number.isNaN(date.getTime())) return "--";
      return `${String(date.getUTCFullYear() % 100).padStart(2, "0")}/${String(
        date.getUTCMonth() + 1
      ).padStart(2, "0")}/${String(date.getUTCDate()).padStart(2, "0")}`;
    }
    return "--";
  };
  const formatChangeRate = (value: number | null | undefined) => {
    if (typeof value !== "number" || Number.isNaN(value)) return "--";
    const sign = value > 0 ? "+" : "";
    return `${sign}${(value * 100).toFixed(1)}%`;
  };
  const latestClose =
    Number.isFinite(latestBar?.[4]) ? Number(latestBar[4]) : ticker.lastClose ?? null;
  const prevClose = Number.isFinite(prevBar?.[4]) ? Number(prevBar[4]) : null;
  const dayChange =
    latestClose != null && prevClose != null && prevClose !== 0
      ? (latestClose - prevClose) / prevClose
      : ticker.chg1D ?? null;
  const latestDateLabel = formatBarDate(latestBarTime);
  const latestCloseLabel = latestClose != null ? latestClose.toLocaleString("ja-JP") : "--";
  const dayChangeLabel = formatChangeRate(dayChange);
  const isNearEvent = (eventDate: string | null | undefined) => {
    const eventMs = parseEventDateMs(eventDate);
    if (eventMs == null || latestBarMs == null) return false;
    return Math.abs(eventMs - latestBarMs) <= 31 * DAY_MS;
  };
  const showRightsBadge = isNearEvent(ticker.eventRightsDate);
  const showEarningsBadge = isNearEvent(ticker.eventEarningsDate);
  const changeTone =
    typeof dayChange === "number" && !Number.isNaN(dayChange)
      ? dayChange >= 0
        ? "up"
        : "down"
      : "flat";

  const handleActivate = () => onActivate?.(ticker.code);
  const handleOpenDetail = () => onOpenDetail(ticker.code);
  const handleToggleKeep = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    onToggleKeep?.(ticker.code);
  };
  const handleExclude = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    onExclude?.(ticker.code);
  };
  const handleToggleFavorite = async (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    if (!backendReady) return;
    const next = !isFavorite;
    setFavoriteLocal(ticker.code, next);
    try {
      if (next) {
        await api.post(`/favorites/${encodeURIComponent(ticker.code)}`);
      } else {
        await api.delete(`/favorites/${encodeURIComponent(ticker.code)}`);
      }
    } catch {
      setFavoriteLocal(ticker.code, isFavorite);
    }
  };
  const handleOpenClick = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    handleOpenDetail();
  };

  return (
    <div
      className={`tile ${active ? "is-selected" : ""}`}
      role="button"
      tabIndex={0}
      onClick={() => {
        handleActivate();
        handleOpenDetail();
      }}
      onKeyDown={(event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          handleOpenDetail();
        }
      }}
    >
      <div className="tile-header">
        <div className="tile-id">
          <span className="tile-code">{ticker.code}</span>
          <span className="tile-name">{ticker.name}</span>
        </div>
        <div className="tile-actions">
          <button
            type="button"
            className={`favorite-toggle ${isFavorite ? "active" : ""}`}
            onClick={handleToggleFavorite}
            aria-label={isFavorite ? "お気に入り解除" : "お気に入り追加"}
          >
            {isFavorite ? <IconHeartFilled size={16} /> : <IconHeart size={16} />}
          </button>
          <button
            type="button"
            className={`candidate-toggle ${kept ? "active" : ""}`}
            onClick={handleToggleKeep}
            aria-label={kept ? "候補から外す" : "候補に追加"}
          >
            {kept ? "✓" : "+"}
          </button>
          <button
            type="button"
            className="tile-action danger"
            onClick={handleExclude}
            aria-label="除外"
          >
            x
          </button>
          <button
            type="button"
            className="tile-action"
            onClick={handleOpenClick}
            aria-label="詳細を開く"
          >
            &gt;
          </button>
        </div>
      </div>
      <div className="tile-meta tile-meta-grid">
        <span className="tile-meta-item">日付 {latestDateLabel}</span>
        <span className="tile-meta-item">終値 {latestCloseLabel}</span>
        <span className={`tile-meta-item tile-meta-change ${changeTone}`}>前日比 {dayChangeLabel}</span>
        {asofLabel && (
          <span className="asof-badge provisional" data-tooltip={asofTooltip ?? ""}>
            暫定 {asofLabel}
          </span>
        )}
        {(showRightsBadge || showEarningsBadge) && (
          <span className="event-badges">
            {showRightsBadge && rightsLabel && <span className="event-badge event-rights">権利 {rightsLabel}</span>}
            {showEarningsBadge && earningsLabel && <span className="event-badge event-earnings">決算 {earningsLabel}</span>}
          </span>
        )}
        {ticker.dataStatus === "missing" && <span className="badge status-missing">データ欠損</span>}
      </div>
      {annotation ? <div className="tile-annotation-row">{annotation}</div> : null}
      <div className="tile-chart">
        {barsPayload && barsPayload.bars?.length ? (
          <ThumbnailCanvas
            payload={barsPayload}
            boxes={boxes}
            showBoxes={showBoxes}
            maSettings={maSettings}
            cacheKey={cacheKey}
            maxBars={maxBars}
            showAxes
            theme={theme}
          />
        ) : cachedThumb ? (
          <div className="thumb-canvas">
            <img className="thumb-canvas-image" src={cachedThumb} alt="" />
          </div>
        ) : (
          <div className={`tile-loading ${barsStatus !== "error" && barsStatus !== "empty" ? "skeleton skeleton-chart" : ""}`}>
            {barsStatus === "error"
              ? "読み込み失敗"
              : barsStatus === "empty"
              ? "データなし"
              : null}
          </div>
        )}
      </div>
    </div>
  );
});

export default StockTile;
