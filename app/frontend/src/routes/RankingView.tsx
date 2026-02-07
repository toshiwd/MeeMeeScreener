﻿import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { IconHeart, IconHeartFilled } from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import ChartListCard from "../components/ChartListCard";
import Toast from "../components/Toast";
import UnifiedListHeader from "../components/UnifiedListHeader";
import { MaSetting, useStore } from "../store";
import { formatEventBadgeDate } from "../utils/events";
import { computeSignalMetrics } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { useConsultScreenshot } from "../hooks/useConsultScreenshot";
import { downloadChartScreenshots } from "../utils/chartScreenshot";

type RankItem = {
  code: string;
  name?: string;
  changePct?: number | null;
  changeAbs?: number | null;
  asOf?: string | null;
  close?: number | null;
  prevClose?: number | null;
  liquidity20d?: number | null;
  series?: number[][];
  is_favorite?: boolean;
  mlPUp?: number | null;
  mlRetPred20?: number | null;
  mlEv20?: number | null;
  mlEv20Net?: number | null;
  hybridScore?: number | null;
  modelVersion?: string | null;
};

type RankTimeframe = "D" | "W" | "M";
type RankWhich = "latest" | "prev";

const RANK_VIEW_STATE_KEY = "rankingViewState";

const RANK_MA_SETTINGS: MaSetting[] = [
  { key: "ma1", label: "MA1", period: 7, visible: true, color: "#ef4444", lineWidth: 1 },
  { key: "ma2", label: "MA2", period: 20, visible: true, color: "#22c55e", lineWidth: 1 },
  { key: "ma3", label: "MA3", period: 60, visible: true, color: "#3b82f6", lineWidth: 1 },
  { key: "ma4", label: "MA4", period: 100, visible: true, color: "#a855f7", lineWidth: 1 },
  { key: "ma5", label: "MA5", period: 200, visible: true, color: "#f59e0b", lineWidth: 1 }
];
const SCREENSHOT_LIMIT = 10;

export default function RankingView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
  const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const boxesCache = useStore((state) => state.boxesCache);
  const maSettings = useStore((state) => state.maSettings);
  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);
  const favorites = useStore((state) => state.favorites);

  const [dir, setDir] = useState<"up" | "down">("up");
  // const [rankTimeframe, setRankTimeframe] = useState<RankTimeframe>("D"); // REMOVED
  const [rankWhich, setRankWhich] = useState<RankWhich>("latest");
  const [items, setItems] = useState<RankItem[]>([]);
  const [search, setSearch] = useState("");
  const [filterSignalsOnly, setFilterSignalsOnly] = useState(false);
  const [filterDataOnly, setFilterDataOnly] = useState(false);
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [selectedCodes, setSelectedCodes] = useState<string[]>([]);
  const [consultVisible, setConsultVisible] = useState(false);
  const [consultExpanded, setConsultExpanded] = useState(false);
  const [consultTab, setConsultTab] = useState<"selection" | "position">("selection");
  const [consultText, setConsultText] = useState("");
  const [consultSort, setConsultSort] = useState<ConsultationSort>("score");
  const [consultBusy, setConsultBusy] = useState(false);
  const [screenshotBusy, setScreenshotBusy] = useState(false);
  const [consultMeta, setConsultMeta] = useState<{ omitted: number }>({ omitted: 0 });
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const rankTileLogRef = useRef<Map<string, string>>(new Map());
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";
  const [useFallback, setUseFallback] = useState(false);

  // Use the screenshot hook
  const { generateScreenshots } = useConsultScreenshot();

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(RANK_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as {
        listTimeframe?: "daily" | "weekly" | "monthly";
        dir?: "up" | "down";
        rankWhich?: RankWhich;
      };
      if (parsed.listTimeframe) {
        setListTimeframe(parsed.listTimeframe);
      }
      if (parsed.dir === "up" || parsed.dir === "down") {
        setDir(parsed.dir);
      }
      if (parsed.rankWhich === "latest" || parsed.rankWhich === "prev") {
        setRankWhich(parsed.rankWhich);
      }
    } catch {
      // ignore storage failures
    }
  }, [setListTimeframe]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const payload = {
        listTimeframe,
        dir,
        rankWhich
      };
      window.sessionStorage.setItem(RANK_VIEW_STATE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }, [listTimeframe, dir, rankWhich]);

  const listStyles = useMemo(
    () =>
    ({
      "--list-cols": listColumns,
      "--list-rows": listRows
    } as CSSProperties),
    [listColumns, listRows]
  );
  const listMaSettings =
    listTimeframe === "daily"
      ? maSettings.daily
      : listTimeframe === "weekly"
        ? maSettings.weekly
        : maSettings.monthly;

  const resolvedMaSettings = listMaSettings ?? RANK_MA_SETTINGS;

  // Map generic timeframe to single char for API and labels
  const tfChar = useMemo(() => {
    switch (listTimeframe) {
      case "weekly": return "W";
      case "monthly": return "M";
      default: return "D";
    }
  }, [listTimeframe]);

  /*
  const timeframeButtons = useMemo(
    () => [
      { key: "D" as RankTimeframe, label: "日足" },
      { key: "W" as RankTimeframe, label: "週足" },
      { key: "M" as RankTimeframe, label: "月足" }
    ],
    []
  );
  */

  const whichLabelMap = useMemo(
    () => ({
      D: { latest: "当日", prev: "前日" },
      W: { latest: "今週", prev: "前週" },
      M: { latest: "今月", prev: "前月" }
    }),
    []
  );
  // Decoupling: rankTimeframe and listTimeframe are now independent.
  // The user can view Weekly Ranking while looking at Daily Charts, for example.

  const sortOptions = useMemo(
    () => [
      { value: "up", label: "上昇Top50" },
      { value: "down", label: "下落Top50" }
    ],
    []
  );

  const filterItems = useMemo(
    () => [
      {
        key: "signals",
        label: "\u30b7\u30b0\u30ca\u30eb\u3042\u308a",
        checked: filterSignalsOnly,
        onToggle: () => setFilterSignalsOnly((prev) => !prev)
      },
      {
        key: "data",
        label: "\u30c7\u30fc\u30bf\u53d6\u5f97\u6e08\u307f",
        checked: filterDataOnly,
        onToggle: () => setFilterDataOnly((prev) => !prev)
      }
    ],
    [filterSignalsOnly, filterDataOnly]
  );

  const fallbackItems = useMemo(() => {
    const normalizeBars = (bars: number[][]) => {
      if (bars.length < 2) return bars;
      return Number(bars[0]?.[0]) > Number(bars[bars.length - 1]?.[0]) ? [...bars].reverse() : bars;
    };
    const resolveChange = (bars: number[][]) => {
      const normalized = normalizeBars(bars);
      if (normalized.length < 3 && rankWhich === "prev") return { changePct: null, changeAbs: null };
      if (normalized.length < 2) return { changePct: null, changeAbs: null };
      const tIndex = rankWhich === "latest" ? normalized.length - 1 : normalized.length - 2;
      const prevIndex = rankWhich === "latest" ? normalized.length - 2 : normalized.length - 3;
      const close = Number(normalized[tIndex]?.[4]);
      const prevClose = Number(normalized[prevIndex]?.[4]);
      if (!Number.isFinite(close) || !Number.isFinite(prevClose) || prevClose === 0) {
        return { changePct: null, changeAbs: null };
      }
      const changeAbs = close - prevClose;
      return { changePct: changeAbs / prevClose, changeAbs };
    };
    const list = tickers.map((ticker) => {
      const payload = barsCache[listTimeframe]?.[ticker.code] ?? null;
      const series = payload?.bars ?? [];
      const change = resolveChange(series);
      return {
        code: ticker.code,
        name: ticker.name ?? ticker.code,
        changePct: change.changePct,
        changeAbs: change.changeAbs,
        is_favorite: favorites.includes(ticker.code)
      };
    });
    return list;
  }, [tickers, favorites, barsCache, listTimeframe, rankWhich]);

  const searchResults = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return items;
    return items.filter((item) => {
      const codeMatch = item.code.toLowerCase().includes(term);
      const nameMatch = (item.name ?? "").toLowerCase().includes(term);
      return codeMatch || nameMatch;
    });
  }, [items, search]);

  const signalMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>["signals"]>();
    searchResults.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code] ?? null;
      const series = payload && payload.bars?.length ? payload.bars : item.series ?? [];
      if (!series.length) return;
      const signals = computeSignalMetrics(series, 4).signals;
      if (signals.length) {
        map.set(item.code, signals);
      }
    });
    return map;
  }, [searchResults, barsCache, listTimeframe]);

  const filteredItems = useMemo(() => {
    if (!filterSignalsOnly && !filterDataOnly) return searchResults;
    return searchResults.filter((item) => {
      const payload = barsCache[listTimeframe]?.[item.code] ?? null;
      const series = payload && payload.bars?.length ? payload.bars : item.series ?? [];
      const hasData = series.length > 0;
      if (filterDataOnly && !hasData) return false;
      if (filterSignalsOnly && !signalMap.has(item.code)) return false;
      return true;
    });
  }, [searchResults, filterSignalsOnly, filterDataOnly, barsCache, listTimeframe, signalMap]);
  const sortedItems = useMemo(() => {
    if (!useFallback) {
      return filteredItems;
    }
    const list = [...filteredItems];
    const getLiquidity = (item: RankItem) =>
      Number.isFinite(item.liquidity20d ?? NaN) ? (item.liquidity20d as number) : -1;
    list.sort((a, b) => {
      const aChange = Number.isFinite(a.changePct ?? NaN) ? (a.changePct as number) : null;
      const bChange = Number.isFinite(b.changePct ?? NaN) ? (b.changePct as number) : null;
      const aMissing = aChange == null;
      const bMissing = bChange == null;
      if (aMissing && bMissing) return a.code.localeCompare(b.code, "ja");
      if (aMissing) return 1;
      if (bMissing) return -1;
      if (aChange !== bChange) {
        return dir === "up" ? bChange - aChange : aChange - bChange;
      }
      const aLiq = getLiquidity(a);
      const bLiq = getLiquidity(b);
      if (aLiq !== bLiq) return bLiq - aLiq;
      return a.code.localeCompare(b.code, "ja");
    });
    return list;
  }, [filteredItems, dir, useFallback]);
  const listCodes = useMemo(() => sortedItems.map((item) => item.code), [sortedItems]);
  const densityKey = `${listColumns}x${listRows}`;

  useEffect(() => {
    if (!backendReady) return;
    if (tickers.length) return;
    loadList().catch(() => { });
  }, [backendReady, loadList, tickers.length]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);

  useEffect(() => {
    if (!backendReady) return;
    setLoading(true);
    setErrorMessage(null);
    setUseFallback(false);
    api
      .get("/rankings", { params: { tf: tfChar, which: rankWhich, dir, mode: "hybrid", limit: 50 } })
      .then((res) => {
        const payload = res.data as { items?: RankItem[]; errors?: string[] };
        const list = Array.isArray(payload.items) ? payload.items : [];
        if (import.meta.env.MODE === "development") {
          try {
            const hasAsOf = list.some((item) => Boolean(item.asOf));
            console.log(
              JSON.stringify({
                tag: "rank_api_response",
                tf: tfChar,
                which: rankWhich,
                dir,
                mode: "hybrid",
                items: list.length,
                has_asOf: hasAsOf,
                sample: list.slice(0, 3).map((item) => ({ code: item.code, asOf: item.asOf })),
              })
            );
          } catch {
            // ignore debug log errors
          }
        }
        setItems(list);
        setUseFallback(false);
        if (payload.errors?.length) {
          setErrorMessage(payload.errors[0]);
        }
      })
      .catch(() => {
        setItems(fallbackItems);
        setUseFallback(true);
        setErrorMessage("ランキングの取得に失敗しました。簡易データを表示しています。");
      })
      .finally(() => setLoading(false));
  }, [backendReady, dir, tfChar, rankWhich, fallbackItems]);

  useEffect(() => {
    if (!backendReady) return;
    if (!sortedItems.length) return;
    ensureBarsForVisible(
      listTimeframe,
      sortedItems.map((item) => item.code),
      "ranking"
    );
  }, [backendReady, ensureBarsForVisible, sortedItems, listTimeframe]);

  useEffect(() => {
    if (!sortedItems.length) return;
    const logs: Array<Record<string, unknown>> = [];
    sortedItems.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code] ?? null;
      const series = payload?.bars ?? item.series ?? [];
      if (!series.length) return;
      const dtMin = series[0]?.[0] ?? null;
      const dtMax = series[series.length - 1]?.[0] ?? null;
      const label = formatAsOf(item.asOf);
      const signature = `${dtMin}|${dtMax}|${series.length}|${label}|${item.asOf ?? ""}`;
      const prevSignature = rankTileLogRef.current.get(item.code);
      if (prevSignature === signature) return;
      rankTileLogRef.current.set(item.code, signature);
      const recentDt = series.slice(-3).map((bar) => bar?.[0] ?? null);
      logs.push({
        tag: "rank_tile_data",
        code: item.code,
        timeframe: listTimeframe,
        dt_min: dtMin,
        dt_max: dtMax,
        len: series.length,
        right_dt: dtMax,
        label,
        anchor_date: item.asOf ?? null,
        recent_dt: recentDt,
      });
    });
    if (import.meta.env.MODE === "development") {
      logs.forEach((entry) => console.log(JSON.stringify(entry)));
    }
  }, [sortedItems, barsCache, listTimeframe]);

  useEffect(() => {
    if (!useFallback) return;
    setItems(fallbackItems);
  }, [fallbackItems, useFallback]);

  useEffect(() => {
    if (!items.length) {
      setSelectedCodes([]);
      return;
    }
    setSelectedCodes((prev) => prev.filter((code) => items.some((item) => item.code === code)));
  }, [items]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && consultVisible) {
        setConsultVisible(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [consultVisible]);

  const selectedSet = useMemo(() => new Set(selectedCodes), [selectedCodes]);

  const toggleSelect = useCallback((code: string) => {
    setSelectedCodes((prev) => {
      if (prev.includes(code)) return prev.filter((item) => item !== code);
      return [...prev, code];
    });
  }, []);

  const handleOpenDetail = useCallback(
    (code: string) => {
      try {
        sessionStorage.setItem("detailListBack", location.pathname);
        sessionStorage.setItem("detailListCodes", JSON.stringify(listCodes));
      } catch {
        // ignore storage failures
      }
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname, listCodes]
  );

  const handleToggleFavorite = useCallback(
    async (code: string, isFavorite: boolean) => {
      setItems((current) =>
        current.map((item) =>
          item.code === code ? { ...item, is_favorite: !isFavorite } : item
        )
      );
      setFavoriteLocal(code, !isFavorite);
      try {
        if (isFavorite) {
          await api.delete(`/favorites/${encodeURIComponent(code)}`);
        } else {
          await api.post(`/favorites/${encodeURIComponent(code)}`);
        }
      } catch {
        setItems((current) =>
          current.map((item) =>
            item.code === code ? { ...item, is_favorite: isFavorite } : item
          )
        );
        setFavoriteLocal(code, isFavorite);
        setToastMessage("お気に入りの更新に失敗しました。");
      }
    },
    [setFavoriteLocal]
  );

  const buildConsultation = useCallback(async () => {
    if (!selectedCodes.length) return;
    setConsultBusy(true);
    try {
      try {
        await ensureBarsForVisible(consultTimeframe, selectedCodes, "consult-pack");
      } catch {
        // Use available cache even if fetch fails.
      }
      const itemsForPack = selectedCodes.map((code) => {
        const rankItem = items.find((item) => item.code === code);
        const payload = barsCache[consultTimeframe]?.[code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        return {
          code,
          name: rankItem?.name ?? null,
          market: null,
          sector: null,
          bars: payload?.bars ?? null,
          boxes,
          boxState: null,
          hasBox: null,
          buyState: null,
          buyStateScore: null,
          buyStateReason: null,
          buyStateDetails: null
        };
      });
      const result = buildConsultationPack(
        {
          createdAt: new Date(),
          timeframe: consultTimeframe,
          barsCount: consultBarsCount
        },
        itemsForPack,
        consultSort
      );
      setConsultText(result.text);
      setConsultMeta({ omitted: result.omittedCount });
      setConsultVisible(true);
      setConsultExpanded(true);
      setConsultTab("selection");
    } finally {
      setConsultBusy(false);
    }
  }, [
    selectedCodes,
    ensureBarsForVisible,
    consultTimeframe,
    items,
    barsCache,
    boxesCache,
    consultSort
  ]);

  const handleCreateScreenshots = useCallback(async () => {
    if (selectedCodes.length === 0) {
      setToastMessage("スクショ対象がありません。");
      return;
    }

    // Check setting for Consult mode (Use new method)
    // The user requirement says "Replace" so we just use the new one.

    setToastMessage("スクショ生成を開始します...");

    const result = await generateScreenshots(selectedCodes);

    if (result.success) {
      setToastMessage(`${result.count}件のスクショを保存しました`);
      if (result.success && window.pywebview?.api?.open_screenshot_dir) {
        setToastAction({
          label: "フォルダを開く",
          onClick: async () => {
            await window.pywebview!.api.open_screenshot_dir();
          }
        });
      }
    } else {
      setToastMessage(`保存失敗: ${result.error || "不明なエラー"}`);
    }
  }, [selectedCodes, generateScreenshots]);

  const handleCopyConsult = useCallback(async () => {
    if (!consultText) {
      setToastMessage("相談パックがまだありません。");
      return;
    }
    try {
      await navigator.clipboard.writeText(consultText);
      setToastMessage("相談パックをコピーしました。");
    } catch {
      setToastMessage("コピーに失敗しました。");
    }
  }, [consultText]);

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = selectedCodes.slice(0, limit);
    const extra = Math.max(0, selectedCodes.length - visible.length);
    return { visible, extra };
  }, [selectedCodes]);

  const showSkeleton = backendReady && loading && items.length === 0;
  const emptyLabel =
    !loading && backendReady && sortedItems.length === 0 && !errorMessage
      ? search.trim() || filterSignalsOnly || filterDataOnly
        ? "該当する銘柄がありません。"
        : "ランキングがありません。"
      : null;
  const isSingleDensity = listColumns === 1 && listRows === 1;
  const formatPct = (value?: number | null) => {
    if (!Number.isFinite(value ?? NaN)) return "--";
    return `${((value ?? 0) * 100).toFixed(2)}%`;
  };
  const formatDownProb = (upProb?: number | null) => {
    if (!Number.isFinite(upProb ?? NaN)) return "--";
    const downProb = 1 - (upProb ?? 0);
    const clipped = Math.min(1, Math.max(0, downProb));
    return `${(clipped * 100).toFixed(2)}%`;
  };
  const formatAsOf = (value?: string | null) => value ?? "--";

  return (
    <div className="app-shell list-view">
      <UnifiedListHeader
        timeframe={listTimeframe}
        onTimeframeChange={setListTimeframe}
        rangeBars={listRangeBars}
        onRangeChange={setListRangeBars}
        search={search}
        onSearchChange={setSearch}
        sortValue={dir}
        sortOptions={sortOptions}
        onSortChange={(value) => setDir(value as "up" | "down")}
        columns={listColumns}
        rows={listRows}
        onColumnsChange={setListColumns}
        onRowsChange={setListRows}
        filterItems={filterItems}
        helpLabel="相談"
        onHelpClick={() => {
          setConsultVisible(true);
          setConsultExpanded(false);
          setConsultTab("selection");
        }}
      />
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "8px",
          alignItems: "center",
          padding: "6px 16px",
          borderBottom: "1px solid var(--theme-border)",
          background: "var(--theme-bg-secondary)"
        }}
      >
        {/* Timeframe buttons removed: Using Global Header Timeframe */}
        <div className="segmented segmented-compact">
          {(["latest", "prev"] as RankWhich[]).map((key) => (
            <button
              key={key}
              type="button"
              className={rankWhich === key ? "active" : ""}
              onClick={() => setRankWhich(key)}
            >
              {whichLabelMap[tfChar][key]}
            </button>
          ))}
        </div>
        <div className="segmented segmented-compact">
          {(["up", "down"] as const).map((key) => (
            <button
              key={key}
              type="button"
              className={dir === key ? "active" : ""}
              onClick={() => setDir(key)}
            >
              {key === "up" ? "上昇" : "下落"}
            </button>
          ))}
        </div>
        <span className="rank-score-badge">表示: エントリー優先</span>
      </div>
      <div
        className={`rank-shell list-shell${isSingleDensity ? " is-single" : ""} ${consultPaddingClass}`}
        style={listStyles}
      >
        {showSkeleton && (
          <div className="rank-skeleton">
            {Array.from({ length: 4 }).map((_, index) => (
              <div className="tile skeleton-card" key={`rank - skeleton - ${index}`}>
                <div className="skeleton-line wide" />
                <div className="skeleton-line" />
                <div className="skeleton-block tall" />
              </div>
            ))}
          </div>
        )}
        {!showSkeleton && (
          <>
            {errorMessage && <div className="rank-status">{errorMessage}</div>}
            {emptyLabel && <div className="rank-status">{emptyLabel}</div>}
            <div className="rank-grid">
              {sortedItems.map((item, index) => {
                const payload = barsCache[listTimeframe]?.[item.code] ?? null;
                const status = barsStatus[listTimeframe][item.code];
                const series =
                  payload && payload.bars?.length ? payload.bars : item.series ?? [];
                const ticker = tickerMap.get(item.code);
                const earningsLabel = formatEventBadgeDate(ticker?.eventEarningsDate);
                const rightsLabel = formatEventBadgeDate(ticker?.eventRightsDate);
                return (
                  <ChartListCard
                    key={item.code}
                    code={item.code}
                    name={item.name ?? item.code}
                    payload={payload}
                    fallbackSeries={series}
                    status={status}
                    maSettings={resolvedMaSettings}
                    rangeBars={listRangeBars}
                    densityKey={densityKey}
                    signals={signalMap.get(item.code) ?? []}
                    onOpenDetail={handleOpenDetail}
                    tileClassName={selectedSet.has(item.code) ? "is-selected" : ""}
                    deferUntilInView
                    maxDate={item.asOf}
                    phaseBody={ticker?.bodyScore ?? null}
                    phaseEarly={ticker?.earlyScore ?? null}
                    phaseLate={ticker?.lateScore ?? null}
                    phaseN={ticker?.phaseN ?? null}
                    headerLeft={
                      <>
                        <span className="rank-badge">{index + 1}</span>
                        <div className="tile-id">
                          <label
                            className="tile-select-toggle"
                            onClick={(event) => event.stopPropagation()}
                            onDoubleClick={(event) => event.stopPropagation()}
                          >
                            <input
                              type="checkbox"
                              checked={selectedSet.has(item.code)}
                              onChange={() => toggleSelect(item.code)}
                              aria-label={`${item.code} を選択`}
                            />
                            <span className="tile-code">{item.code}</span>
                          </label>
                          <span className="tile-name">{item.name ?? item.code}</span>
                          {(rightsLabel || earningsLabel) && (
                            <span className="event-badges">
                              {rightsLabel && (
                                <span className="event-badge event-rights">権利 {rightsLabel}</span>
                              )}
                              {earningsLabel && (
                                <span className="event-badge event-earnings">
                                  決算 {earningsLabel}
                                </span>
                              )}
                            </span>
                          )}
                        </div>
                      </>
                    }
                    headerRight={
                      <>
                        <span className="rank-score-badge">
                          騰落率 {formatPct(item.changePct)}
                        </span>
                        <span className="rank-score-badge">
                          期待値 {formatPct(item.mlEv20Net)}
                        </span>
                        <span className="rank-score-badge">
                          {dir === "up"
                            ? `上昇確率 ${formatPct(item.mlPUp)}`
                            : `下落確率 ${formatDownProb(item.mlPUp)}`}
                        </span>
                        <span className="rank-score-badge">
                          総合 {formatPct(item.hybridScore)}
                        </span>
                        <span className="rank-score-badge">日付 {formatAsOf(item.asOf)}</span>
                        <button
                          type="button"
                          className={`favorite-toggle ${item.is_favorite ? "active" : ""}`}
                          aria-pressed={Boolean(item.is_favorite)}
                          aria-label={item.is_favorite ? "お気に入り解除" : "お気に入り追加"}
                          onClick={(event) => {
                            event.stopPropagation();
                            handleToggleFavorite(item.code, Boolean(item.is_favorite));
                          }}
                        >
                          {item.is_favorite ? <IconHeartFilled size={16} /> : <IconHeart size={16} />}
                        </button>
                      </>
                    }
                  />
                );
              })}

            </div>
          </>
        )}
      </div>
      <div
        className={`consult - sheet ${consultVisible ? "is-visible" : "is-hidden"} ${consultExpanded ? "is-expanded" : "is-mini"
          }`}
      >
        <button
          type="button"
          className="consult-handle"
          onClick={() => {
            if (!consultVisible) return;
            setConsultExpanded((prev) => !prev);
          }}
          aria-label={consultExpanded ? "相談バーを折りたたむ" : "相談バーを展開する"}
        />
        {!consultExpanded && (
          <div className="consult-mini">
            <div className="consult-mini-left">
              <div className="consult-mini-count">選択 {selectedCodes.length}件</div>
              <div className="consult-chips">
                {selectedChips.visible.map((code) => (
                  <span key={code} className="consult-chip">
                    {code}
                  </span>
                ))}
                {selectedChips.extra > 0 && (
                  <span className="consult-chip">+{selectedChips.extra}</span>
                )}
              </div>
            </div>
            <div className="consult-mini-actions">
              <button
                type="button"
                className="consult-primary"
                onClick={buildConsultation}
                disabled={!selectedCodes.length || consultBusy}
              >
                {consultBusy ? "作成中..." : "相談作成"}
              </button>
              <button
                type="button"
                onClick={handleCreateScreenshots}
                disabled={!selectedCodes.length || screenshotBusy}
              >
                {screenshotBusy ? "作成中..." : "スクショ作成"}
              </button>
              <button type="button" onClick={handleCopyConsult} disabled={!consultText}>
                コピー
              </button>
              <button
                type="button"
                onClick={() => window.pywebview?.api?.open_screenshot_dir?.()}
                disabled={!window.pywebview?.api?.open_screenshot_dir}
              >
                フォルダ
              </button>
              <button type="button" onClick={() => setConsultVisible(false)}>
                閉じる
              </button>
            </div>
          </div>
        )}
        {consultExpanded && (
          <div className="consult-expanded">
            <div className="consult-expanded-header">
              <div className="consult-tabs">
                <button
                  type="button"
                  className={consultTab === "selection" ? "active" : ""}
                  onClick={() => setConsultTab("selection")}
                >
                  選定相談
                </button>
                <button
                  type="button"
                  className={consultTab === "position" ? "active" : ""}
                  onClick={() => setConsultTab("position")}
                >
                  建玉相談
                </button>
              </div>
              <div className="consult-expanded-actions">
                <button
                  type="button"
                  className="consult-primary"
                  onClick={buildConsultation}
                  disabled={!selectedCodes.length || consultBusy}
                >
                  {consultBusy ? "作成中..." : "相談作成"}
                </button>
                <button
                  type="button"
                  onClick={handleCreateScreenshots}
                  disabled={!selectedCodes.length || screenshotBusy}
                >
                  {screenshotBusy ? "作成中..." : "スクショ作成"}
                </button>
                <button type="button" onClick={handleCopyConsult} disabled={!consultText}>
                  コピー
                </button>
                <button
                  type="button"
                  onClick={() => window.pywebview?.api?.open_screenshot_dir?.()}
                  disabled={!window.pywebview?.api?.open_screenshot_dir}
                >
                  フォルダ
                </button>
                <button type="button" onClick={() => setConsultVisible(false)}>
                  閉じる
                </button>
              </div>
            </div>
            <div className="consult-expanded-body">
              <div className="consult-expanded-meta-row">
                <div className="consult-expanded-meta">
                  選択 {selectedCodes.length}件
                  {consultMeta.omitted
                    ? ` / 表示外 ${consultMeta.omitted}件`
                    : " / 最大10件まで表示"}
                </div>
                <div className="consult-sort">
                  <span>並び順</span>
                  <div className="segmented segmented-compact">
                    {(["score", "code"] as ConsultationSort[]).map((key) => (
                      <button
                        key={key}
                        className={consultSort === key ? "active" : ""}
                        onClick={() => setConsultSort(key)}
                      >
                        {key === "score" ? "スコア順" : "コード順"}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
              {consultTab === "selection" ? (
                <textarea className="consult-drawer-body" value={consultText} readOnly />
              ) : (
                <div className="consult-placeholder">建玉相談は準備中です。</div>
              )}
            </div>
          </div>
        )}
      </div>
      <Toast
        message={toastMessage}
        onClose={() => {
          setToastMessage(null);
          setToastAction(null);
        }}
        action={toastAction}
      />
    </div>
  );
}




