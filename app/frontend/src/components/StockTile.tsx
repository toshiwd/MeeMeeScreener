import { memo, type MouseEvent } from "react";
import { useEffect } from "react";
import { IconHeart, IconHeartFilled } from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import { Ticker, useStore } from "../store";
import type { SignalChip } from "../utils/signals";
import { formatEventBadgeDate } from "../utils/events";
import ThumbnailCanvas from "./ThumbnailCanvas";
import { buildThumbnailCacheKey, getThumbnailCache } from "./thumbnailCache";

type StockTileProps = {
  ticker: Ticker;
  timeframe: "monthly" | "weekly" | "daily";
  maxBars?: number;
  signals?: SignalChip[];
  active?: boolean;
  kept?: boolean;
  theme?: "dark" | "light";
  asofLabel?: string | null;
  asofTooltip?: string | null;
  onActivate?: (code: string) => void;
  onOpenDetail: (code: string) => void;
  onToggleKeep?: (code: string) => void;
  onExclude?: (code: string) => void;
};

const StockTile = memo(function StockTile({
  ticker,
  timeframe,
  maxBars,
  signals,
  active = false,
  kept = false,
  theme,
  asofLabel,
  asofTooltip,
  onActivate,
  onOpenDetail,
  onToggleKeep,
  onExclude
}: StockTileProps) {
  const barsPayload = useStore((state) => {
    const map = state.barsCache?.[timeframe] ?? {};
    return map[ticker.code];
  });
  const { ready: backendReady } = useBackendReadyState();
  const favorites = useStore((state) => state.favorites);
  const favoritesLoaded = useStore((state) => state.favoritesLoaded);
  const loadFavorites = useStore((state) => state.loadFavorites);
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
  const isFavorite = favorites.includes(ticker.code);
  const formatScore = (value: number | null | undefined) =>
    Number.isFinite(value)
      ? String(Math.min(10, Math.max(0, Math.round(value! * 10))))
      : "--";
  const formatN = (value: number | null | undefined) =>
    typeof value === "number" ? String(value) : "--";

  useEffect(() => {
    if (!backendReady || favoritesLoaded) return;
    loadFavorites();
  }, [backendReady, favoritesLoaded, loadFavorites]);

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
          {asofLabel && (
            <span className="asof-badge" title={asofTooltip ?? undefined}>
              asof: {asofLabel}
            </span>
          )}
          {(rightsLabel || earningsLabel) && (
            <span className="event-badges">
              {rightsLabel && <span className="event-badge event-rights">権利 {rightsLabel}</span>}
              {earningsLabel && <span className="event-badge event-earnings">決算 {earningsLabel}</span>}
            </span>
          )}
          {ticker.dataStatus === "missing" && (
            <span className="badge status-missing">データ欠損</span>
          )}
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
            className={`tile-action ${kept ? "active" : ""}`}
            onClick={handleToggleKeep}
            aria-label={kept ? "候補から外す" : "候補に追加"}
          >
            +
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
      {signals?.length ? (
        <div className="tile-signal-row">
          <div className="signal-chips">
            {signals.slice(0, 4).map((signal) => (
              <span
                key={signal.label}
                className={`signal-chip ${signal.kind === "warning" ? "warning" : "achieved"}`}
              >
                {signal.label}
              </span>
            ))}
          </div>
        </div>
      ) : null}
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
          <div className="tile-loading">
            {barsStatus === "error"
              ? "読み込み失敗"
              : barsStatus === "empty"
              ? "データなし"
              : "読み込み中..."}
          </div>
        )}
      </div>
    </div>
  );
});

export default StockTile;
