import { useCallback, useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import ChartListCard from "../components/ChartListCard";
import Toast from "../components/Toast";
import UnifiedListHeader from "../components/UnifiedListHeader";
import { useStore } from "../store";
import { computeSignalMetrics, getSignalDirectionSummary } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { useConsultScreenshot } from "../hooks/useConsultScreenshot";

type CandidateItem = {
  code: string;
  name?: string;
};

type CandidateSortKey = "code" | "change" | "scoreUp" | "scoreDown";
const CANDIDATES_VIEW_STATE_KEY = "candidatesViewState";

type StateEvalRow = {
  code: string;
  side: string;
  holding_band?: string;
  strategy_tags?: string;
  decision_3way: string;
  confidence: number | null;
  reason_text_top3: string;
};

type TrendRow = {
  side: string;
  holding_band: string;
  strategy_tag: string;
  expectancy_delta: number;
  risk_delta: number;
};

type TrendSummaryResponse = {
  trends?: {
    improving?: TrendRow[];
    weakening?: TrendRow[];
    persistent_risk?: TrendRow[];
  };
};

const parseReasonTexts = (value: string | null | undefined): string[] => {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === "string") : [];
  } catch {
    return [];
  }
};

const parseStrategyTags = (value: string | null | undefined): string[] => {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === "string") : [];
  } catch {
    return [];
  }
};

const buildTrendReason = (trend: { label: string } | null | undefined) => {
  if (!trend) return null;
  if (trend.label === "Improving") return "Trend improving";
  if (trend.label === "Weakening") return "Trend weakening";
  if (trend.label === "Persistent Risk") return "Persistent risk";
  return trend.label;
};

const classifyPriorReason = (reason: string) => {
  if (/^Combo strength:/i.test(reason)) {
    return { label: reason.replace(/^Combo strength:\s*/i, ""), tone: "combo" as const };
  }
  if (/^Historically strong:/i.test(reason)) {
    return { label: reason.replace(/^Historically strong:\s*/i, ""), tone: "prior-strong" as const };
  }
  if (/^Historical caution:/i.test(reason)) {
    return { label: reason.replace(/^Historical caution:\s*/i, ""), tone: "prior-caution" as const };
  }
  return null;
};

export default function CandidatesView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
  const keepList = useStore((state) => state.keepList);
  const removeKeep = useStore((state) => state.removeKeep);
  const tickers = useStore((state) => state.tickers);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const loadingList = useStore((state) => state.loadingList);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const boxesCache = useStore((state) => state.boxesCache);
  const maSettings = useStore((state) => state.maSettings);
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);

  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<CandidateSortKey>("code");
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
  const [stateEvalRows, setStateEvalRows] = useState<StateEvalRow[]>([]);
  const [trendSummary, setTrendSummary] = useState<TrendSummaryResponse | null>(null);
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";

  // Use the screenshot hook
  const { generateScreenshots, isProcessing: screenshotBusy } = useConsultScreenshot();

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(CANDIDATES_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as {
        search?: string;
        sortKey?: CandidateSortKey;
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
      window.sessionStorage.setItem(CANDIDATES_VIEW_STATE_KEY, JSON.stringify(payload));
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
    if (tickers.length) return;
    ensureListLoaded().catch(() => setToastMessage("候補一覧の読み込みに失敗しました。"));
  }, [backendReady, ensureListLoaded, tickers.length]);

  useEffect(() => {
    if (!backendReady) return;
    let active = true;
    const run = async () => {
      try {
        const [stateEvalResponse, trendResponse] = await Promise.all([
          api.get<{ rows?: StateEvalRow[] }>("/analysis-bridge/state-eval", { params: { limit: 200 } }),
          api.get<TrendSummaryResponse>("/analysis-bridge/internal/state-eval-trends", { params: { lookback: 14, limit: 20 } })
        ]);
        if (!active) return;
        setStateEvalRows(Array.isArray(stateEvalResponse.data?.rows) ? stateEvalResponse.data.rows : []);
        setTrendSummary(trendResponse.data ?? null);
      } catch {
        if (active) {
          setStateEvalRows([]);
          setTrendSummary(null);
        }
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [backendReady]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);

  const items = useMemo<CandidateItem[]>(
    () =>
      keepList.map((code) => ({
        code,
        name: tickerMap.get(code)?.name
      })),
    [keepList, tickerMap]
  );

  const searchResults = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return items;
    return items.filter((item) => {
      const codeMatch = item.code.toLowerCase().includes(term);
      const nameMatch = (item.name ?? "").toLowerCase().includes(term);
      return codeMatch || nameMatch;
    });
  }, [items, search]);

  const signalMetricsMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>>();
    searchResults.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code];
      if (!payload?.bars?.length) return;
      map.set(item.code, computeSignalMetrics(payload.bars, 4));
    });
    return map;
  }, [searchResults, barsCache, listTimeframe]);
  const stateEvalMap = useMemo(
    () => new Map(stateEvalRows.map((row) => [row.code, row])),
    [stateEvalRows]
  );
  const trendTagMap = useMemo(() => {
    const map = new Map<string, { label: string; tone: "improving" | "weakening" | "risk" }>();
    const improving = trendSummary?.trends?.improving ?? [];
    const weakening = trendSummary?.trends?.weakening ?? [];
    const persistentRisk = trendSummary?.trends?.persistent_risk ?? [];
    improving.forEach((row) => map.set(row.strategy_tag, { label: "Improving", tone: "improving" }));
    weakening.forEach((row) => {
      if (!map.has(row.strategy_tag)) map.set(row.strategy_tag, { label: "Weakening", tone: "weakening" });
    });
    persistentRisk.forEach((row) => {
      if (!map.has(row.strategy_tag)) map.set(row.strategy_tag, { label: "Persistent Risk", tone: "risk" });
    });
    return map;
  }, [trendSummary]);

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

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && consultVisible) {
        setConsultVisible(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [consultVisible]);

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
        const candidate = items.find((item) => item.code === code);
        const ticker = tickerMap.get(code);
        const payload = barsCache[consultTimeframe]?.[code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        return {
          code,
          name: candidate?.name ?? ticker?.name ?? null,
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
    tickerMap
  ]);

  const handleEnsureVisibleItem = useCallback(
    (code: string) => {
      if (!backendReady) return;
      void ensureBarsForVisible(listTimeframe, [code], "candidates-visible");
    },
    [backendReady, ensureBarsForVisible, listTimeframe]
  );

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

  const handleCreateScreenshots = useCallback(async () => {
    if (!consultTargets.length) {
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

  const emptyLabel =
    !loadingList && backendReady && sortedItems.length === 0
      ? search.trim() || filterSignalsOnly || filterDataOnly || filterBuySignalsOnly || filterSellSignalsOnly
        ? "該当する銘柄がありません。"
        : "候補がありません。"
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
        onSortChange={(value) => setSortKey(value as CandidateSortKey)}
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
        {loadingList && <div className="rank-status">読み込み中...</div>}
        {emptyLabel && <div className="rank-status">{emptyLabel}</div>}
        <div className="rank-grid">
          {sortedItems.map((item) => {
            const payload = barsCache[listTimeframe]?.[item.code] ?? null;
            const status = barsStatus[listTimeframe][item.code];
            const ticker = tickerMap.get(item.code);
            const stateEval = stateEvalMap.get(item.code);
            const trendTag = parseStrategyTags(stateEval?.strategy_tags).map((tag) => trendTagMap.get(tag)).find(Boolean);
            const displayReasons = [...parseReasonTexts(stateEval?.reason_text_top3).slice(0, 2)];
            const trendReason = buildTrendReason(trendTag);
            const priorReason = displayReasons.map(classifyPriorReason).find(Boolean) ?? null;
            if (trendReason && !displayReasons.includes(trendReason)) {
              displayReasons.push(trendReason);
            }
            return (
              <ChartListCard
                key={item.code}
                code={item.code}
                name={item.name ?? item.code}
                payload={payload}
                status={status}
                maSettings={maSettings[listTimeframe]}
                rangeBars={listRangeBars}
                eventEarningsDate={ticker?.eventEarningsDate ?? null}
                eventRightsDate={ticker?.eventRightsDate ?? null}
                densityKey={densityKey}
                signals={signalMap.get(item.code) ?? []}
                annotation={
                  stateEval ? (
                    <div className="candidate-ai-annotation">
                      <span className={`candidate-ai-badge is-${String(stateEval.decision_3way || "wait")}`}>
                        {String(stateEval.decision_3way || "wait").toUpperCase()}
                      </span>
                      <span className="candidate-ai-confidence">
                        AI {typeof stateEval.confidence === "number" ? `${Math.round(stateEval.confidence * 100)}%` : "--"}
                      </span>
                      {priorReason ? (
                        <span className={`candidate-ai-prior-badge is-${priorReason.tone}`}>
                          {priorReason.tone === "combo" ? "COMBO" : priorReason.tone === "prior-caution" ? "CAUTION" : "PRIOR"} {priorReason.label}
                        </span>
                      ) : null}
                      <div className="candidate-ai-reasons">
                        {displayReasons.map((reason) => (
                          <span key={`${item.code}:${reason}`} className="candidate-ai-reason">
                            {reason}
                          </span>
                        ))}
                      </div>
                    </div>
                  ) : null
                }
                onOpenDetail={handleOpenDetail}
                deferUntilInView
                onEnterView={handleEnsureVisibleItem}
                phaseBody={ticker?.bodyScore ?? null}
                phaseEarly={ticker?.earlyScore ?? null}
                phaseLate={ticker?.lateScore ?? null}
                phaseN={ticker?.phaseN ?? null}
                action={{
                  label: "\u2713",
                  ariaLabel: "候補から外す",
                  className: "candidate-toggle active",
                  onClick: () => removeKeep(item.code)
                }}
              />
            );
          })}

        </div>
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
              <div className="consult-mini-count">候補 {consultTargets.length}件</div>
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
                  候補 {consultTargets.length}件
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
