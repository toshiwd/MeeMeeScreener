import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import TopNav from "../components/TopNav";
import { useBackendReadyState } from "../backendReady";
import { useStore } from "../store";
import { api } from "../api";
import MarketHeatmapPanel from "../features/market/MarketHeatmapPanel";
import {
  buildSectorMemberIndex,
  buildWatchlistSectorIndex,
  enrichMarketItems,
  resolveMarketTickerRate,
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
import {
  clearMarketTimelineCache,
  describeMarketTimelineSource,
  readMarketTimelineCache,
  writeMarketTimelineCache,
  type MarketTimelineCacheSource,
} from "./marketTimelineCache";
import { openDetailWithPrefetch } from "./detail/openDetailWithPrefetch";
import { recordPerfEvent } from "../perfDiagnostics";
const TIMELINE_LIMIT = 180;
const THEME_ROTATION_LIMIT = 12;
const THEME_CANDIDATE_LIMIT = 8;

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

type ThemeLeader = {
  code: string;
  name: string;
  changePct: number;
  volumeRatio?: number | null;
  isEarnings?: boolean | null;
};

type ThemeRotationItem = {
  themeId: string;
  name: string;
  avgChangePct: number;
  advancerRatio: number;
  avgVolumeRatio: number;
  strength: number;
  strengthDelta: number;
  status: "NEW" | "ACCEL" | "HOT" | "FADE" | "COLD" | "WATCH";
  count: number;
  earningsCount: number;
  leaders: ThemeLeader[];
};

type ThemeRotationPayload = {
  label?: string | null;
  themes?: ThemeRotationItem[];
  excludeEarnings?: boolean | null;
};

type ThemeCandidateMode = "entry_ease" | "value_range" | "continuation";

type ThemeCandidateItem = {
  rank: number;
  code: string;
  name: string;
  theme: string;
  score: number;
  mode: ThemeCandidateMode;
  label?: string | null;
  close: number;
  changePct: number;
  volumeRatio: number;
  extended20: number;
  themeCount: number;
  themeAdvancerRatio: number;
  candlestickState?: { states?: string[]; closePosition?: number } | null;
  visualCheck?: {
    required?: boolean;
    setup?: string;
    supports?: string[];
    resistances?: string[];
    warnings?: string[];
    supportPrice?: number | null;
    resistancePrice?: number | null;
    detailRouteRequired?: boolean;
  } | null;
  reasonCodes?: string[];
  penaltyCodes?: string[];
  detailRoute?: string;
};

type ThemeCandidatePayload = {
  label?: string | null;
  mode?: ThemeCandidateMode;
  items?: ThemeCandidateItem[];
  excludeEarnings?: boolean | null;
};

const THEME_STATUS_LABEL: Record<ThemeRotationItem["status"], string> = {
  NEW: "NEW",
  ACCEL: "加速",
  HOT: "強い",
  FADE: "失速",
  COLD: "弱い",
  WATCH: "監視"
};

const formatSigned = (value: number, digits = 1) => {
  if (!Number.isFinite(value)) return "--";
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
};

const formatRatio = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  return `${Math.round(value * 100)}%`;
};

const CANDIDATE_MODE_LABEL: Record<ThemeCandidateMode, string> = {
  entry_ease: "買いやすさ",
  value_range: "値幅狙い",
  continuation: "継続強度"
};

const CANDIDATE_CODE_LABEL: Record<string, string> = {
  bearish_large_body: "大陰線",
  upper_wick_rejection: "上ヒゲ",
  weak_close: "引け弱い",
  extended: "過熱",
  volume_expansion: "出来高増",
  breakout_60d: "60日高値",
  theme_breadth: "テーマ幅",
  thin_theme: "薄いテーマ",
  ma_alignment: "MA良好",
  bullish_close: "引け強い",
  wide_range: "値幅余地",
  below_ma60: "60MA下"
};

const codeLabel = (code: string) => CANDIDATE_CODE_LABEL[code] ?? code;

const VISUAL_SETUP_LABEL: Record<string, string> = {
  needs_detail_chart_check: "要チャート確認",
  avoid_until_repair: "形状修復待ち",
  momentum_only: "勢い限定",
  breakout_watch: "上抜け確認",
  constructive: "形状良好"
};

const visualSetupLabel = (setup?: string | null) => setup ? VISUAL_SETUP_LABEL[setup] ?? setup : "要チャート確認";

function ThemeCandidateLane({
  loading,
  error,
  payload,
  mode,
  onModeChange,
  onOpenDetail
}: {
  loading: boolean;
  error: string | null;
  payload: ThemeCandidatePayload | null;
  mode: ThemeCandidateMode;
  onModeChange: (next: ThemeCandidateMode) => void;
  onOpenDetail: (code: string) => void;
}) {
  const items = payload?.items ?? [];
  return (
    <section className="market-candidate-panel">
      <div className="market-candidate-head">
        <div>
          <div className="market-theme-title">固定スコア候補</div>
          <div className="market-theme-subtitle">
            {payload?.label ? `${payload.label} / ` : ""}
            テーマの強さと日足形状を固定ルールで採点
          </div>
        </div>
        <div className="segmented segmented-compact market-candidate-mode">
          {(Object.keys(CANDIDATE_MODE_LABEL) as ThemeCandidateMode[]).map((key) => (
            <button
              key={key}
              type="button"
              className={mode === key ? "active" : ""}
              onClick={() => onModeChange(key)}
            >
              {CANDIDATE_MODE_LABEL[key]}
            </button>
          ))}
        </div>
      </div>
      {loading && !items.length ? (
        <div className="market-theme-empty">固定スコア候補を読み込み中...</div>
      ) : error && !items.length ? (
        <div className="market-theme-empty">{error}</div>
      ) : items.length ? (
        <div className="market-candidate-list">
          {items.map((item) => {
            const penalties = item.penaltyCodes ?? [];
            const reasons = item.reasonCodes ?? [];
            const visualWarnings = item.visualCheck?.warnings ?? [];
            return (
              <button
                key={`${item.mode}:${item.code}`}
                type="button"
                className="market-candidate-row"
                onClick={() => onOpenDetail(item.code)}
              >
                <span className="market-candidate-rank">{item.rank}</span>
                <span className="market-candidate-main">
                  <span className="market-candidate-title">
                    <strong>{item.code}</strong>
                    <span>{item.name}</span>
                  </span>
                  <span className="market-candidate-theme">{item.theme}</span>
                  <span className={`market-candidate-visual is-${visualWarnings.length ? "warning" : "ok"}`}>
                    {visualSetupLabel(item.visualCheck?.setup)}
                  </span>
                  <span className="market-candidate-chips">
                    {reasons.slice(0, 3).map((code) => (
                      <span key={code} className="market-candidate-chip">{codeLabel(code)}</span>
                    ))}
                    {penalties.slice(0, 3).map((code) => (
                      <span key={code} className="market-candidate-chip is-penalty">{codeLabel(code)}</span>
                    ))}
                    <span className="market-candidate-chip is-visual">詳細確認</span>
                  </span>
                </span>
                <span className="market-candidate-score">
                  <strong>{Number.isFinite(item.score) ? item.score.toFixed(1) : "--"}</strong>
                  <span>{formatSigned(item.changePct)}%</span>
                  <span>出来高 {formatSigned(item.volumeRatio, 2)}x</span>
                </span>
              </button>
            );
          })}
        </div>
      ) : (
        <div className="market-theme-empty">固定スコア候補がありません。</div>
      )}
    </section>
  );
}

function ThemeRotationLane({
  loading,
  error,
  payload,
  excludeEarnings,
  onExcludeEarningsChange
}: {
  loading: boolean;
  error: string | null;
  payload: ThemeRotationPayload | null;
  excludeEarnings: boolean;
  onExcludeEarningsChange: (next: boolean) => void;
}) {
  const themes = payload?.themes ?? [];
  return (
    <section className="market-theme-panel">
      <div className="market-theme-head">
        <div>
          <div className="market-theme-title">テーマローテーション</div>
          <div className="market-theme-subtitle">
            {payload?.label ? `${payload.label} / ` : ""}
            騰落・上昇銘柄比率・出来高で資金の向きを集約
          </div>
        </div>
        <label className="market-theme-toggle">
          <input
            type="checkbox"
            checked={excludeEarnings}
            onChange={(event) => onExcludeEarningsChange(event.target.checked)}
          />
          <span>決算近辺を除外</span>
        </label>
      </div>
      {loading && !themes.length ? (
        <div className="market-theme-empty">テーマ集計を読み込み中...</div>
      ) : error && !themes.length ? (
        <div className="market-theme-empty">{error}</div>
      ) : themes.length ? (
        <div className="market-theme-scroll">
          {themes.slice(0, 10).map((theme) => {
            const tone =
              theme.status === "FADE" || theme.status === "COLD"
                ? "negative"
                : theme.status === "WATCH"
                  ? "neutral"
                  : "positive";
            const leader = theme.leaders?.[0] ?? null;
            return (
              <div key={theme.themeId} className={`market-theme-card is-${tone}`}>
                <div className="market-theme-card-top">
                  <span className="market-theme-name">{theme.name}</span>
                  <span className={`market-theme-status is-${theme.status.toLowerCase()}`}>
                    {THEME_STATUS_LABEL[theme.status]}
                  </span>
                </div>
                <div className="market-theme-score-row">
                  <strong>{formatSigned(theme.avgChangePct)}%</strong>
                  <span>{formatRatio(theme.advancerRatio)}</span>
                  <span>出来高 {formatSigned(theme.avgVolumeRatio, 2)}x</span>
                </div>
                <div className="market-theme-meta-row">
                  <span>{theme.count}銘柄</span>
                  <span>{theme.earningsCount ? `決算近辺 ${theme.earningsCount}` : "非決算中心"}</span>
                  <span>{formatSigned(theme.strengthDelta, 2)}</span>
                </div>
                <div className="market-theme-leader">
                  {leader ? `${leader.code} ${leader.name} ${formatSigned(leader.changePct)}%` : "リーダーなし"}
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="market-theme-empty">テーマ集計対象がありません。</div>
      )}
    </section>
  );
}

export default function MarketView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
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
  const [timelineSource, setTimelineSource] = useState<MarketTimelineCacheSource | null>(null);
  const [themeRotation, setThemeRotation] = useState<ThemeRotationPayload | null>(null);
  const [themeCandidates, setThemeCandidates] = useState<ThemeCandidatePayload | null>(null);
  const [themeExcludeEarnings, setThemeExcludeEarnings] = useState(false);
  const [themeCandidateMode, setThemeCandidateMode] = useState<ThemeCandidateMode>("entry_ease");
  const [themeLoading, setThemeLoading] = useState(false);
  const [candidateLoading, setCandidateLoading] = useState(false);
  const [themeError, setThemeError] = useState<string | null>(null);
  const [candidateError, setCandidateError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const cursorInitializedPeriodRef = useRef<MarketPeriodKey | null>(null);
  const loadingStartedAtRef = useRef<number | null>(null);

  useEffect(() => {
    recordPerfEvent("market_view_mount", {
      route: location.pathname,
    });
  }, [location.pathname]);

  useEffect(() => {
    if (!ensureListLoaded) return;
    void ensureListLoaded();
  }, [ensureListLoaded]);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      const storage = typeof window === "undefined" ? null : window.localStorage;
      const cachedTimeline = storage ? readMarketTimelineCache(storage, period, TIMELINE_LIMIT) : null;
      const cachedFrames = cachedTimeline?.frames ?? null;
      setTimelineSource(cachedTimeline?.source ?? null);
      if (cachedFrames?.length) {
        setFrames(cachedFrames);
        recordPerfEvent("market_timeline_cache_hydrated", {
          period,
          frameCount: cachedFrames.length,
          source: cachedTimeline?.source ?? "timeline",
        });
        if (cursorInitializedPeriodRef.current !== period) {
          const resolved = resolveInitialMarketCursor(cachedFrames, readStoredState());
          setCursorIndex(resolved.index);
          cursorInitializedPeriodRef.current = period;
        }
      }
      setLoading(!cachedFrames?.length);
      loadingStartedAtRef.current = performance.now();
      setError(null);
      recordPerfEvent("market_timeline_fetch_start", {
        period,
        cachedFrameCount: cachedFrames?.length ?? 0,
      });
      try {
        const res = await api.get("/market/heatmap/timeline", {
          params: { period, limit: TIMELINE_LIMIT }
        });
        if (canceled) return;
        const rawFrames = Array.isArray(res.data?.frames) ? (res.data.frames as MarketTimelineFrame[]) : [];
        const hasItems = rawFrames.some((frame) => Array.isArray(frame.items) && frame.items.length > 0);
        if (rawFrames.length > 0 && hasItems) {
          setFrames(rawFrames);
          setTimelineSource("timeline");
          if (storage) {
            writeMarketTimelineCache(storage, period, "timeline", rawFrames, TIMELINE_LIMIT);
            clearMarketTimelineCache(storage, period, "snapshot_fallback", TIMELINE_LIMIT);
          }
          recordPerfEvent("market_timeline_fetch_end", {
            period,
            frameCount: rawFrames.length,
          });
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
        setTimelineSource("snapshot_fallback");
        if (storage) {
          writeMarketTimelineCache(storage, period, "snapshot_fallback", [fallbackFrame], TIMELINE_LIMIT);
        }
        recordPerfEvent("market_timeline_fallback_used", {
          period,
          itemCount: items.length,
        });
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
        recordPerfEvent("market_timeline_fetch_failed", {
          period,
          message,
          retainedFrameCount: cachedFrames?.length ?? 0,
        });
        if (!cachedFrames?.length) {
          setError(message);
          setFrames([]);
          setTimelineSource(null);
        }
      } finally {
        if (!canceled) {
          setLoading(false);
          if (loadingStartedAtRef.current != null) {
            recordPerfEvent("market_loading_state_complete", {
              period,
              visibleMs: Number((performance.now() - loadingStartedAtRef.current).toFixed(1)),
            });
          }
          loadingStartedAtRef.current = null;
        }
      }
    };
    void load();
    return () => {
      canceled = true;
    };
  }, [period]);

  useEffect(() => {
    if (!backendReady) return;
    let canceled = false;
    const wait = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));
    const loadThemes = async () => {
      setThemeLoading(true);
      setThemeError(null);
      let lastError: unknown = null;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          const res = await api.get("/market/theme-rotation", {
            params: {
              limit: THEME_ROTATION_LIMIT,
              exclude_earnings: themeExcludeEarnings
            }
          });
          if (canceled) return;
          setThemeRotation((res.data ?? null) as ThemeRotationPayload | null);
          recordPerfEvent("market_theme_rotation_fetch_end", {
            count: Array.isArray(res.data?.themes) ? res.data.themes.length : 0,
            excludeEarnings: themeExcludeEarnings,
            attempt: attempt + 1
          });
          lastError = null;
          break;
        } catch (loadError) {
          lastError = loadError;
          if (canceled) return;
          if (attempt < 2) {
            await wait(1200 * (attempt + 1));
            continue;
          }
        }
      }
      if (lastError) {
        if (canceled) return;
        const message = lastError instanceof Error && lastError.message.trim()
          ? lastError.message.trim()
          : "テーマ集計の取得に失敗しました。";
        setThemeError(message);
        setThemeRotation(null);
        recordPerfEvent("market_theme_rotation_fetch_failed", {
          message,
          excludeEarnings: themeExcludeEarnings
        });
      }
      if (!canceled) {
        setThemeLoading(false);
      }
    };
    void loadThemes();
    return () => {
      canceled = true;
    };
  }, [backendReady, themeExcludeEarnings]);

  useEffect(() => {
    if (!backendReady) return;
    let canceled = false;
    const wait = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));
    const loadCandidates = async () => {
      setCandidateLoading(true);
      setCandidateError(null);
      let lastError: unknown = null;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          const res = await api.get("/market/theme-candidates", {
            params: {
              mode: themeCandidateMode,
              limit: THEME_CANDIDATE_LIMIT,
              exclude_earnings: themeExcludeEarnings
            }
          });
          if (canceled) return;
          setThemeCandidates((res.data ?? null) as ThemeCandidatePayload | null);
          recordPerfEvent("market_theme_candidates_fetch_end", {
            count: Array.isArray(res.data?.items) ? res.data.items.length : 0,
            excludeEarnings: themeExcludeEarnings,
            mode: themeCandidateMode,
            attempt: attempt + 1
          });
          lastError = null;
          break;
        } catch (loadError) {
          lastError = loadError;
          if (canceled) return;
          if (attempt < 2) {
            await wait(1200 * (attempt + 1));
            continue;
          }
        }
      }
      if (lastError) {
        if (canceled) return;
        const message = lastError instanceof Error && lastError.message.trim()
          ? lastError.message.trim()
          : "固定スコア候補の取得に失敗しました。";
        setCandidateError(message);
        setThemeCandidates(null);
        recordPerfEvent("market_theme_candidates_fetch_failed", {
          message,
          excludeEarnings: themeExcludeEarnings,
          mode: themeCandidateMode
        });
      }
      if (!canceled) {
        setCandidateLoading(false);
      }
    };
    void loadCandidates();
    return () => {
      canceled = true;
    };
  }, [backendReady, themeCandidateMode, themeExcludeEarnings]);

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
    return selectedSectorMembers.length > 0
      ? selectedSectorMembers
      : selectedSectorItem.representatives.length > 0
        ? selectedSectorItem.representatives
        : selectedSectorItem.watchlistTickers;
  }, [selectedSectorItem, selectedSectorMembers]);

  const timelineMax = Math.max(frames.length - 1, 0);
  const timelineLabel = activeFrame?.label ?? "";
  const selectedSummary = formatSelectedSummary(selectedSectorItem);
  const timelineSourceNote = describeMarketTimelineSource(timelineSource);

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
      recordPerfEvent("market_open_detail", {
        code,
        listCount: selectedSectorFallbackItems.length,
      });
      void openDetailWithPrefetch({
        navigate,
        code,
        listCodes: selectedSectorFallbackItems.map((item) => item.code),
        backPath: location.pathname,
        backendReady
      });
    },
    [backendReady, navigate, location.pathname, selectedSectorFallbackItems]
  );

  const panelItems = selectedSectorFallbackItems;
  const renderPanelItem = useCallback(
    (item: (typeof panelItems)[number]) => {
      const rate = resolveMarketTickerRate(item, period);
      const rateTone = rate == null ? "neutral" : rate > 0 ? "positive" : rate < 0 ? "negative" : "neutral";
      return (
        <button
          key={item.code}
          type="button"
          className="market-side-row"
          onClick={() => handleDetailOpen(item.code)}
        >
          <span className="market-side-row-main">
            <span className="market-side-row-code">{item.code}</span>
            <span className="market-side-row-name">{item.name}</span>
          </span>
          <span className={`market-side-row-rate is-${rateTone}`}>
            {rate == null ? "--" : formatMarketRate(rate)}
          </span>
        </button>
      );
    },
    [handleDetailOpen, period]
  );

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
          <div className="market-main-stack">
            <ThemeRotationLane
              loading={themeLoading}
              error={themeError}
              payload={themeRotation}
              excludeEarnings={themeExcludeEarnings}
              onExcludeEarningsChange={setThemeExcludeEarnings}
            />
            <ThemeCandidateLane
              loading={candidateLoading}
              error={candidateError}
              payload={themeCandidates}
              mode={themeCandidateMode}
              onModeChange={setThemeCandidateMode}
              onOpenDetail={handleDetailOpen}
            />
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
          </div>
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
                  <span>一覧銘柄</span>
                  <strong>{selectedSectorItem.watchlistCount}件</strong>
                </div>
                <div className="market-side-summary-row">
                  <span>セクター内銘柄</span>
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
                  {panelItems.map(renderPanelItem)}
                </div>
              ) : (
                <>
                  <div className="market-side-empty">このセクターに一覧銘柄はありません。</div>
                  {panelItems.length > 0 ? (
                    <div className="market-side-list">
                      {panelItems.map(renderPanelItem)}
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
        {timelineSourceNote && (
          <div className="market-global-note">{timelineSourceNote}</div>
        )}
        {!loading && error && !allActiveItems.length && (
          <div className="market-global-note">{error}</div>
        )}
      </div>
    </div>
  );
}
