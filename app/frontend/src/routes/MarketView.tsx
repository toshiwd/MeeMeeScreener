import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import TopNav from "../components/TopNav";
import { useStore } from "../store";
import { api } from "../api";
import MarketHeatmapPanel from "../features/market/MarketHeatmapPanel";
import {
  buildSectorMemberIndex,
  buildWatchlistSectorIndex,
  enrichMarketItems,
  type MarketMetricKey,
  type MarketPeriodKey,
  type MarketSectorViewItem,
  type MarketTimelineFrame,
  type MarketTimelineItem
} from "../features/market/marketHelpers";
import { formatMarketFlow, formatMarketRate } from "../features/market/marketHelpers";
import {
  buildPersistedMarketViewState,
  getMarketTimelineFrameDateKey,
  MARKET_VIEW_STATE_KEY,
  MARKET_VIEW_STATE_VERSION,
  resolveInitialMarketCursor,
  type StoredMarketViewState
} from "./marketViewState";
const TIMELINE_LIMIT = 180;

const PERIOD_OPTIONS: { key: MarketPeriodKey; label: string }[] = [
  { key: "1d", label: "1日" },
  { key: "1w", label: "1週" },
  { key: "1m", label: "1ヶ月" }
];

const METRIC_OPTIONS: { key: MarketMetricKey; label: string }[] = [
  { key: "rate", label: "騰落率" },
  { key: "flow", label: "資金フロー" },
  { key: "both", label: "両方" }
];

const readStoredState = (): Partial<StoredMarketViewState> | null => {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(MARKET_VIEW_STATE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as StoredMarketViewState;
    if (parsed.stateVersion !== MARKET_VIEW_STATE_VERSION) return null;
    return parsed;
  } catch {
    return null;
  }
};

const formatSelectedSummary = (item: MarketSectorViewItem | null) => {
  if (!item) return "未選択";
  return `${item.label} ${item.sector33_code}`;
};

export default function MarketView() {
  const location = useLocation();
  const navigate = useNavigate();
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const tickers = useStore((state) => state.tickers);
  const keepList = useStore((state) => state.keepList);

  const stored = useMemo(() => readStoredState(), []);
  const [period, setPeriod] = useState<MarketPeriodKey>(stored?.period ?? "1d");
  const [metric, setMetric] = useState<MarketMetricKey>(stored?.metric ?? "rate");
  const [selectedSector, setSelectedSector] = useState<string | null>(stored?.selectedSector ?? null);
  const [cursorIndex, setCursorIndex] = useState(stored?.cursorIndex ?? 0);
  const [cursorUserInteracted, setCursorUserInteracted] = useState(false);
  const [frames, setFrames] = useState<MarketTimelineFrame[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const cursorInitializedPeriodRef = useRef<MarketPeriodKey | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const payload = buildPersistedMarketViewState({
        period,
        metric,
        selectedSector,
        cursorIndex,
        cursorDate: activeFrameDateKey,
        cursorUserInteracted,
        previous: readStoredState()
      });
      window.sessionStorage.setItem(MARKET_VIEW_STATE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }, [period, metric, cursorIndex, cursorUserInteracted, selectedSector, activeFrameDateKey]);

  useEffect(() => {
    if (!ensureListLoaded) return;
    void ensureListLoaded();
  }, [ensureListLoaded]);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await api.get("/market/heatmap/timeline", {
          params: { period, limit: TIMELINE_LIMIT }
        });
        if (canceled) return;
        const rawFrames = Array.isArray(res.data?.frames) ? (res.data.frames as MarketTimelineFrame[]) : [];
        const hasItems = rawFrames.some((frame) => Array.isArray(frame.items) && frame.items.length > 0);
        if (rawFrames.length > 0 && hasItems) {
          setFrames(rawFrames);
          if (cursorInitializedPeriodRef.current !== period) {
            const resolved = resolveInitialMarketCursor(rawFrames, readStoredState());
            setCursorIndex(resolved.index);
            cursorInitializedPeriodRef.current = period;
          }
          return;
        }
        const fallback = await api.get("/market/heatmap", { params: { period } });
        if (canceled) return;
        const items = Array.isArray(fallback.data?.items) ? (fallback.data.items as MarketTimelineItem[]) : [];
        const fallbackFrame = {
          asof: Math.floor(Date.now() / 1000),
          label: new Date().toISOString().slice(0, 10),
          items
        };
        setFrames([fallbackFrame]);
        if (cursorInitializedPeriodRef.current !== period) {
          const resolved = resolveInitialMarketCursor([fallbackFrame], readStoredState());
          setCursorIndex(resolved.index);
          cursorInitializedPeriodRef.current = period;
        }
      } catch (loadError) {
        if (canceled) return;
        const message = loadError instanceof Error && loadError.message.trim()
          ? loadError.message.trim()
          : "市場データの読み込みに失敗しました。";
        setError(message);
        setFrames([]);
      } finally {
        if (!canceled) setLoading(false);
      }
    };
    void load();
    return () => {
      canceled = true;
    };
  }, [period]);

  const sectorMemberIndex = useMemo(() => buildSectorMemberIndex(tickers), [tickers]);
  const watchlistSectorIndex = useMemo(
    () => buildWatchlistSectorIndex(keepList, tickers),
    [keepList, tickers]
  );

  const activeFrame = useMemo(() => {
    if (!frames.length) return null;
    const safeIndex = Math.min(Math.max(cursorIndex, 0), frames.length - 1);
    return frames[safeIndex] ?? null;
  }, [frames, cursorIndex]);
  const activeFrameDateKey = useMemo(
    () => (activeFrame ? getMarketTimelineFrameDateKey(activeFrame) : null),
    [activeFrame]
  );

  const allActiveItems = useMemo(() => {
    if (!activeFrame) return [];
    return enrichMarketItems(activeFrame.items ?? [], sectorMemberIndex, watchlistSectorIndex);
  }, [activeFrame, sectorMemberIndex, watchlistSectorIndex]);

  useEffect(() => {
    if (selectedSector && !allActiveItems.some((item) => item.sector33_code === selectedSector)) {
      setSelectedSector(null);
    }
  }, [selectedSector, allActiveItems]);

  const selectedSectorItem = useMemo(
    () => allActiveItems.find((item) => item.sector33_code === selectedSector) ?? null,
    [allActiveItems, selectedSector]
  );

  const selectedSectorMembers = useMemo(() => {
    if (!selectedSector) return [];
    return sectorMemberIndex.get(selectedSector) ?? [];
  }, [selectedSector, sectorMemberIndex]);

  const selectedSectorFallbackItems = useMemo(() => {
    if (!selectedSectorItem) return [];
    return selectedSectorItem.watchlistTickers.length > 0
      ? selectedSectorItem.watchlistTickers
      : selectedSectorItem.representatives.length > 0
        ? selectedSectorItem.representatives
        : selectedSectorMembers.slice(0, 2);
  }, [selectedSectorItem, selectedSectorMembers]);

  const timelineMax = Math.max(frames.length - 1, 0);
  const timelineLabel = activeFrame?.label ?? "";
  const selectedSummary = formatSelectedSummary(selectedSectorItem);

  const selectSector = useCallback((item: MarketSectorViewItem) => {
    setSelectedSector(item.sector33_code);
  }, []);

  const handlePeriodChange = useCallback((nextPeriod: MarketPeriodKey) => {
    cursorInitializedPeriodRef.current = null;
    setCursorUserInteracted(false);
    setPeriod(nextPeriod);
  }, []);

  const handleDetailOpen = useCallback(
    (code: string) => {
      try {
        sessionStorage.setItem("detailListBack", location.pathname);
        sessionStorage.setItem(
          "detailListCodes",
          JSON.stringify(selectedSectorFallbackItems.map((item) => item.code))
        );
      } catch {
        // ignore storage failures
      }
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname, selectedSectorFallbackItems]
  );

  const panelItems = selectedSectorFallbackItems;

  return (
    <div className="app-shell market-view">
      <div className="dynamic-header market-header">
        <div className="dynamic-header-row header-row-top">
          <div className="header-row-left">
            <TopNav />
          </div>
        </div>
        <div className="dynamic-header-row market-control-row">
          <div className="market-control-group">
            <div className="market-control-label">期間</div>
            <div className="segmented segmented-compact">
              {PERIOD_OPTIONS.map((option) => (
                <button
                  key={option.key}
                  type="button"
                  className={period === option.key ? "active" : ""}
                  onClick={() => handlePeriodChange(option.key)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>

          <div className="market-control-group">
            <div className="market-control-label">指標</div>
            <div className="segmented segmented-compact">
              {METRIC_OPTIONS.map((option) => (
                <button
                  key={option.key}
                  type="button"
                  className={metric === option.key ? "active" : ""}
                  onClick={() => setMetric(option.key)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>

          <div className="market-timeline">
            <span className="market-timeline-label">{timelineLabel || "最新"}</span>
            <input
              className="heatmap-timeline-range"
              type="range"
              min={0}
              max={timelineMax}
              value={Math.min(Math.max(cursorIndex, 0), timelineMax)}
              onChange={(event) => {
                setCursorUserInteracted(true);
                setCursorIndex(Number(event.target.value));
              }}
              disabled={timelineMax <= 0}
            />
            <span className="market-timeline-meta">
              {frames.length ? `${Math.min(Math.max(cursorIndex, 0), timelineMax) + 1}/${frames.length}` : "0/0"}
            </span>
          </div>
        </div>
      </div>

      <main className="market-main market-layout">
        <section className="market-main-panel">
          <MarketHeatmapPanel
            loading={loading}
            error={error}
            items={allActiveItems}
            metric={metric}
            selectedSector={selectedSector}
            onSectorSelect={selectSector}
            onSectorHover={() => {
              // tooltip handles the hover preview.
            }}
          />
        </section>

        <aside className="market-side-panel">
          <div className="market-side-panel-header">
            <div>
              <div className="market-side-title">セクター詳細</div>
              <div className="market-side-subtitle">{selectedSummary}</div>
            </div>
          </div>
          {selectedSectorItem ? (
            <>
              <div className="market-side-summary">
                <div className="market-side-summary-row">
                  <span>業種名</span>
                  <strong>{selectedSectorItem.label}</strong>
                </div>
                <div className="market-side-summary-row">
                  <span>指標</span>
                  <strong>
                    {metric === "flow"
                      ? formatMarketFlow(selectedSectorItem.flow)
                      : metric === "both"
                        ? `${formatMarketRate(selectedSectorItem.rate)} / ${formatMarketFlow(selectedSectorItem.flow)}`
                        : formatMarketRate(selectedSectorItem.rate)}
                  </strong>
                </div>
                <div className="market-side-summary-row">
                  <span>監視銘柄</span>
                  <strong>{selectedSectorItem.watchlistCount}件</strong>
                </div>
                <div className="market-side-summary-row">
                  <span>代表銘柄</span>
                  <strong>
                    {selectedSectorItem.representatives.length
                      ? selectedSectorItem.representatives
                          .map((entry) => `${entry.code} ${entry.name}`)
                          .join(" / ")
                      : "--"}
                  </strong>
                </div>
              </div>

              {selectedSectorItem.watchlistCount > 0 ? (
                <div className="market-side-list">
                  {panelItems.map((item) => (
                    <button
                      key={item.code}
                      type="button"
                      className="market-side-row"
                      onClick={() => handleDetailOpen(item.code)}
                    >
                      <span className="market-side-row-code">{item.code}</span>
                      <span className="market-side-row-name">{item.name}</span>
                    </button>
                  ))}
                </div>
              ) : (
                <>
                  <div className="market-side-empty">このセクターに監視銘柄はありません。</div>
                  {panelItems.length > 0 ? (
                    <div className="market-side-list">
                      {panelItems.map((item) => (
                        <button
                          key={item.code}
                          type="button"
                          className="market-side-row"
                          onClick={() => handleDetailOpen(item.code)}
                        >
                          <span className="market-side-row-code">{item.code}</span>
                          <span className="market-side-row-name">{item.name}</span>
                        </button>
                      ))}
                    </div>
                  ) : null}
                </>
              )}
            </>
          ) : (
            <div className="market-side-empty">
              左のセクターをクリックすると詳細を表示します。
            </div>
          )}
        </aside>
      </main>

      <div className="market-summary-band">
        {!loading && error && !allActiveItems.length && (
          <div className="market-global-note">{error}</div>
        )}
      </div>
    </div>
  );
}
