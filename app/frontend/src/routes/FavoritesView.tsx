import { useCallback, useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import ChartListCard from "../components/ChartListCard";
import TradexListSummary from "../components/TradexListSummary";
import Toast from "../components/Toast";
import UnifiedListHeader from "../components/UnifiedListHeader";
import { IconHeartFilled } from "@tabler/icons-react";
import { useStore } from "../store";
import { computeSignalMetrics, getSignalDirectionSummary } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { useConsultScreenshot } from "../hooks/useConsultScreenshot";
import { buildTradexListSummaryKey } from "./list/tradexSummary";
import { TradexListSummaryMount } from "./list/TradexListSummaryMount";

type FavoriteItem = {
  code: string;
  name?: string;
};

type FavoritesResponse = {
  items?: FavoriteItem[];
  errors?: string[];
};

type FavoriteSortKey = "code" | "change" | "scoreUp" | "scoreDown";
const FAVORITES_VIEW_STATE_KEY = "favoritesViewState";

export default function FavoritesView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
  const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
  const replaceFavorites = useStore((state) => state.replaceFavorites);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const boxesCache = useStore((state) => state.boxesCache);
  const maSettings = useStore((state) => state.maSettings);
  const tickers = useStore((state) => state.tickers);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);

  const [items, setItems] = useState<FavoriteItem[]>([]);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<FavoriteSortKey>("code");
  const [loading, setLoading] = useState(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [filterSignalsOnly, setFilterSignalsOnly] = useState(false);
  const [filterDataOnly, setFilterDataOnly] = useState(false);
  const [filterBuySignalsOnly, setFilterBuySignalsOnly] = useState(false);
  const [filterSellSignalsOnly, setFilterSellSignalsOnly] = useState(false);
  const [consultVisible, setConsultVisible] = useState(false);
  const [consultExpanded, setConsultExpanded] = useState(false);
  const [consultTab, setConsultTab] = useState<"selection" | "position">("selection");
  const [consultText, setConsultText] = useState("");
  const [consultSort, setConsultSort] = useState<ConsultationSort>("score");
  const [consultBusy, setConsultBusy] = useState(false);
  const [consultMeta, setConsultMeta] = useState<{ omitted: number }>({ omitted: 0 });
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(FAVORITES_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as {
        search?: string;
        sortKey?: FavoriteSortKey;
        filterSignalsOnly?: boolean;
        filterDataOnly?: boolean;
        filterBuySignalsOnly?: boolean;
        filterSellSignalsOnly?: boolean;
      };
      if (typeof parsed.search === "string") {
        setSearch(parsed.search);
      }
      if (
        parsed.sortKey === "code" ||
        parsed.sortKey === "change" ||
        parsed.sortKey === "scoreUp" ||
        parsed.sortKey === "scoreDown"
      ) {
        setSortKey(parsed.sortKey);
      }
      if (typeof parsed.filterSignalsOnly === "boolean") {
        setFilterSignalsOnly(parsed.filterSignalsOnly);
      }
      if (typeof parsed.filterDataOnly === "boolean") {
        setFilterDataOnly(parsed.filterDataOnly);
      }
      if (typeof parsed.filterBuySignalsOnly === "boolean") {
        setFilterBuySignalsOnly(parsed.filterBuySignalsOnly);
      }
      if (typeof parsed.filterSellSignalsOnly === "boolean") {
        setFilterSellSignalsOnly(parsed.filterSellSignalsOnly);
      }
    } catch {
      // ignore storage failures
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const payload = {
        search,
        sortKey,
        filterSignalsOnly,
        filterDataOnly,
        filterBuySignalsOnly,
        filterSellSignalsOnly
      };
      window.sessionStorage.setItem(FAVORITES_VIEW_STATE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }, [search, sortKey, filterSignalsOnly, filterDataOnly, filterBuySignalsOnly, filterSellSignalsOnly]);

  const listStyles = useMemo(
    () =>
    ({
      "--list-cols": listColumns,
      "--list-rows": listRows
    } as CSSProperties),
    [listColumns, listRows]
  );
  const densityKey = `${listColumns}x${listRows}`;

  const sortOptions = useMemo(
    () => [
      { value: "code", label: "\u30b3\u30fc\u30c9\u9806" },
      { value: "change", label: "\u9a30\u843d\u9806" },
      { value: "scoreUp", label: "\u4e0a\u6607\u30b9\u30b3\u30a2\u9806" },
      { value: "scoreDown", label: "\u4e0b\u843d\u30b9\u30b3\u30a2\u9806" }
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
      },
      {
        key: "buy-signal",
        label: "\u8cb7\u3044\u5224\u5b9a\u3042\u308a",
        checked: filterBuySignalsOnly,
        onToggle: () => setFilterBuySignalsOnly((prev) => !prev)
      },
      {
        key: "sell-signal",
        label: "\u58f2\u308a\u5224\u5b9a\u3042\u308a",
        checked: filterSellSignalsOnly,
        onToggle: () => setFilterSellSignalsOnly((prev) => !prev)
      }
    ],
    [filterSignalsOnly, filterDataOnly, filterBuySignalsOnly, filterSellSignalsOnly]
  );

  useEffect(() => {
    if (!backendReady) return;
    setLoading(true);
    api
      .get("/favorites")
      .then((res) => {
        const payload = res.data as FavoritesResponse & { codes?: string[] };
        let list = Array.isArray(payload.items) ? payload.items : [];
        if (!list.length && Array.isArray(payload.codes)) {
          list = payload.codes.map((code) => ({ code }));
        }
        setItems(list);
        replaceFavorites(list.map((item) => item.code));
      })
      .catch((error) => {
        const err = error as {
          message?: string;
          response?: { status?: number; data?: unknown };
        };
        console.error("[favorites] load failed (view)", {
          status: err?.response?.status ?? null,
          data: err?.response?.data ?? null,
          message: err?.message ?? null
        });
        setItems([]);
        replaceFavorites([]);
        setToastMessage("お気に入りの取得に失敗しました。");
      })
      .finally(() => setLoading(false));
  }, [replaceFavorites, backendReady]);

  useEffect(() => {
    if (!backendReady) return;
    if (tickers.length) return;
    ensureListLoaded().catch(() => { });
  }, [backendReady, ensureListLoaded, tickers.length]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);

  const resolveName = useCallback(
    (item: FavoriteItem) => item.name ?? tickerMap.get(item.code)?.name ?? item.code,
    [tickerMap]
  );

  const searchResults = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return items;
    return items.filter((item) => {
      const codeMatch = item.code.toLowerCase().includes(term);
      const nameMatch = resolveName(item).toLowerCase().includes(term);
      return codeMatch || nameMatch;
    });
  }, [items, search, resolveName]);

  const signalMetricsMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>>();
    searchResults.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code];
      if (!payload?.bars?.length) return;
      map.set(item.code, computeSignalMetrics(payload.bars, 4));
    });
    return map;
  }, [searchResults, barsCache, listTimeframe]);

  const signalMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>["signals"]>();
    signalMetricsMap.forEach((metrics, code) => {
      if (metrics.signals.length) {
        map.set(code, metrics.signals);
      }
    });
    return map;
  }, [signalMetricsMap]);

  const filteredItems = useMemo(() => {
    const hasDirectionalFilter = filterBuySignalsOnly || filterSellSignalsOnly;
    if (!filterSignalsOnly && !filterDataOnly && !hasDirectionalFilter) return searchResults;
    return searchResults.filter((item) => {
      const payload = barsCache[listTimeframe]?.[item.code];
      const hasData = Boolean(payload?.bars?.length);
      const metrics = signalMetricsMap.get(item.code);
      const summary = metrics ? getSignalDirectionSummary(metrics) : null;
      if (filterDataOnly && !hasData) return false;
      if (filterSignalsOnly && !signalMap.has(item.code)) return false;
      if (hasDirectionalFilter) {
        const matchesBuy = filterBuySignalsOnly && Boolean(summary?.hasBuySignal);
        const matchesSell = filterSellSignalsOnly && Boolean(summary?.hasSellSignal);
        if (!(matchesBuy || matchesSell)) return false;
      }
      return true;
    });
  }, [
    searchResults,
    filterSignalsOnly,
    filterDataOnly,
    filterBuySignalsOnly,
    filterSellSignalsOnly,
    barsCache,
    listTimeframe,
    signalMap,
    signalMetricsMap
  ]);

  const metricsMap = useMemo(() => {
    const map = new Map<string, { change: number; score: number }>();
    filteredItems.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code];
      const bars = payload?.bars ?? [];
      if (!bars.length) {
        map.set(item.code, { change: 0, score: 0 });
        return;
      }
      const ordered =
        bars.length >= 2 && Number(bars[0][0]) > Number(bars[bars.length - 1][0])
          ? [...bars].reverse()
          : bars;
      const last = ordered[ordered.length - 1];
      const prev = ordered.length > 1 ? ordered[ordered.length - 2] : null;
      const lastClose = Number(last?.[4]);
      const prevClose = Number(prev?.[4]);
      const change =
        Number.isFinite(lastClose) && Number.isFinite(prevClose) && prevClose != 0
          ? (lastClose - prevClose) / prevClose
          : 0;
      const score = ordered.length ? computeSignalMetrics(ordered, 4).trendStrength : 0;
      map.set(item.code, { change, score });
    });
    return map;
  }, [filteredItems, barsCache, listTimeframe]);

  const sortedItems = useMemo(() => {
    const next = [...filteredItems];
    if (sortKey === "code") {
      next.sort((a, b) => a.code.localeCompare(b.code, "ja"));
    } else if (sortKey === "change") {
      next.sort(
        (a, b) =>
          (metricsMap.get(b.code)?.change ?? 0) - (metricsMap.get(a.code)?.change ?? 0)
      );
    } else if (sortKey === "scoreUp") {
      next.sort(
        (a, b) =>
          (metricsMap.get(b.code)?.score ?? 0) - (metricsMap.get(a.code)?.score ?? 0)
      );
    } else if (sortKey === "scoreDown") {
      next.sort(
        (a, b) =>
          (metricsMap.get(a.code)?.score ?? 0) - (metricsMap.get(b.code)?.score ?? 0)
      );
    }
    return next;
  }, [filteredItems, sortKey, metricsMap]);

  const listCodes = useMemo(() => sortedItems.map((item) => item.code), [sortedItems]);

  const consultTargets = useMemo(() => sortedItems.map((item) => item.code), [sortedItems]);
  const tradexListSummaryItems = useMemo(
    () => sortedItems.map((item) => ({ code: item.code, asof: null })),
    [sortedItems]
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && consultVisible) {
        setConsultVisible(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [consultVisible]);

  const handleRemoveFavorite = async (code: string) => {
    const prevItems = items;
    setItems((current) => current.filter((item) => item.code !== code));
    setFavoriteLocal(code, false);
    try {
      await api.delete(`/favorites/${encodeURIComponent(code)}`);
    } catch {
      setItems(prevItems);
      setFavoriteLocal(code, true);
      setToastMessage("お気に入りの削除に失敗しました。");
    }
  };

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

  const handleEnsureVisibleItem = useCallback(
    (code: string) => {
      if (!backendReady) return;
      void ensureBarsForVisible(listTimeframe, [code], "favorites-visible");
    },
    [backendReady, ensureBarsForVisible, listTimeframe]
  );

  const buildConsultation = useCallback(async () => {
    if (!consultTargets.length) return;
    setConsultBusy(true);
    try {
      try {
        await ensureBarsForVisible(consultTimeframe, consultTargets, "consult-pack");
      } catch {
        // Use available cache even if fetch fails.
      }
      const itemsForPack = consultTargets.map((code) => {
        const favorite = items.find((item) => item.code === code);
        const ticker = tickerMap.get(code);
        const payload = barsCache[consultTimeframe]?.[code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        return {
          code,
          name: favorite ? resolveName(favorite) : ticker?.name ?? null,
          market: null,
          sector: null,
          bars: payload?.bars ?? null,
          boxes,
          boxState: ticker?.boxState ?? null,
          hasBox: ticker?.hasBox ?? null,
          buyState: ticker?.buyState ?? null,
          buyStateScore:
            typeof ticker?.buyStateScore === "number" ? ticker.buyStateScore : null,
          buyStateReason: ticker?.buyStateReason ?? null,
          buyStateDetails: ticker?.buyStateDetails ?? null
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
    consultTargets,
    ensureBarsForVisible,
    consultTimeframe,
    items,
    barsCache,
    boxesCache,
    consultSort,
    tickerMap,
    resolveName
  ]);

  const handleCopyConsult = useCallback(async () => {
    if (!consultText) {
      setToastMessage("Consult text is empty.");
      return;
    }
    try {
      await navigator.clipboard.writeText(consultText);
      setToastMessage("Consult text copied.");
    } catch {
      setToastMessage("Clipboard write failed.");
    }
  }, [consultText]);

  const { generateScreenshots, isProcessing: screenshotBusy } = useConsultScreenshot();

  const handleCreateScreenshots = useCallback(async () => {
    if (consultTargets.length === 0) {
      setToastMessage("スクショ対象がありません。");
      return;
    }

    setToastMessage("スクショ生成を開始します...");
    const result = await generateScreenshots(consultTargets);

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
  }, [
    consultTargets,
    generateScreenshots
  ]);

  const emptyLabel =
    !loading && backendReady && sortedItems.length === 0
      ? search.trim() || filterSignalsOnly || filterDataOnly || filterBuySignalsOnly || filterSellSignalsOnly
        ? "該当する銘柄がありません。"
        : "お気に入りがありません。"
      : null;

  const isSingleDensity = listColumns === 1 && listRows === 1;
  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = consultTargets.slice(0, limit);
    const extra = Math.max(0, consultTargets.length - visible.length);
    return { visible, extra };
  }, [consultTargets]);

  return (
    <div className="app-shell list-view">
      <UnifiedListHeader
        timeframe={listTimeframe}
        onTimeframeChange={setListTimeframe}
        rangeBars={listRangeBars}
        onRangeChange={setListRangeBars}
        search={search}
        onSearchChange={setSearch}
        sortValue={sortKey}
        sortOptions={sortOptions}
        onSortChange={(value) => setSortKey(value as FavoriteSortKey)}
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
        className={`rank-shell list-shell${isSingleDensity ? " is-single" : ""} ${consultPaddingClass}`}
        style={listStyles}
      >
        <TradexListSummaryMount
          backendReady={backendReady}
          enabled={true}
          scope="favorites-visible"
          items={tradexListSummaryItems}
        >
          {(tradexListSummaryState) => (
            <>
              {loading && <div className="rank-status">読み込み中...</div>}
              {emptyLabel && <div className="rank-status">{emptyLabel}</div>}
              <div className="rank-grid">
                {sortedItems.map((item) => {
            const payload = barsCache[listTimeframe]?.[item.code] ?? null;
            const status = barsStatus[listTimeframe][item.code];
            const ticker = tickerMap.get(item.code);
            const tradexSummaryKey = buildTradexListSummaryKey(item.code, null);
            const tradexSummary = tradexListSummaryState.itemsByKey[tradexSummaryKey] ?? null;
            return (
              <ChartListCard
                key={item.code}
                code={item.code}
                name={resolveName(item)}
                payload={payload}
                status={status}
                maSettings={maSettings[listTimeframe]}
                rangeBars={listRangeBars}
                eventEarningsDate={ticker?.eventEarningsDate ?? null}
                eventRightsDate={ticker?.eventRightsDate ?? null}
                densityKey={densityKey}
                signals={signalMap.get(item.code) ?? []}
                onOpenDetail={handleOpenDetail}
                deferUntilInView
                onEnterView={handleEnsureVisibleItem}
                phaseBody={ticker?.bodyScore ?? null}
                phaseEarly={ticker?.earlyScore ?? null}
                phaseLate={ticker?.lateScore ?? null}
                phaseN={ticker?.phaseN ?? null}
                annotation={
                  <TradexListSummary
                    summary={tradexSummary}
                    loading={tradexListSummaryState.loading && !tradexSummary}
                  />
                }
                action={{
                  label: <IconHeartFilled size={20} />,
                  ariaLabel: "お気に入り解除",
                  className: "favorite-toggle active",
                  onClick: () => handleRemoveFavorite(item.code)
                }}
              />
            );
                })}
              </div>
            </>
          )}
        </TradexListSummaryMount>
      </div>
      <div
        className={`consult-sheet ${consultVisible ? "is-visible" : "is-hidden"} ${consultExpanded ? "is-expanded" : "is-mini"
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
              <div className="consult-mini-count">お気に入り {consultTargets.length}件</div>
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
                disabled={!consultTargets.length || consultBusy}
              >
                {consultBusy ? "作成中..." : "相談作成"}
              </button>
              <button
                type="button"
                onClick={handleCreateScreenshots}
                disabled={!consultTargets.length || screenshotBusy}
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
                  disabled={!consultTargets.length || consultBusy}
                >
                  {consultBusy ? "作成中..." : "相談作成"}
                </button>
                <button
                  type="button"
                  onClick={handleCreateScreenshots}
                  disabled={!consultTargets.length || screenshotBusy}
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
                  お気に入り {consultTargets.length}件
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




