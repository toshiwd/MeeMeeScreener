import { useEffect, useMemo, useState, useCallback, type CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useBackendReadyState } from "../backendReady";
import { api } from "../api";
import UnifiedListHeader from "../components/UnifiedListHeader";
import ChartListCard from "../components/ChartListCard";
import Toast from "../components/Toast";
import { useStore } from "../store";
import { computeSignalMetrics } from "../utils/signals";
import { IconRefresh, IconUpload } from "@tabler/icons-react";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { downloadChartScreenshots } from "../utils/chartScreenshot";

type HeldItem = {
  symbol: string;
  name: string;
  sell_buy_text: string;
  opened_at: string | null;
  has_issue: boolean;
  issue_note?: string | null;
  buy_qty: number;
  sell_qty: number;
};

type HistoryItem = {
  round_id: string;
  symbol: string;
  name: string;
  opened_at: string | null;
  closed_at: string | null;
  round_no: number;
  has_issue: boolean;
  issue_note?: string | null;
};

type PositionSortKey = "code" | "change" | "scoreUp" | "scoreDown";
const POSITIONS_VIEW_STATE_KEY = "positionsViewState";
const SCREENSHOT_LIMIT = 10;

type RoundEvent = {
  broker: string;
  exec_dt: string | null;
  action: string;
  qty: number;
  price: number | null;
};

const formatDate = (value: string | null | undefined) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd}`;
};

const extractErrorMessage = (err: unknown, fallback = "不明なエラー") => {
  if (!err || typeof err !== "object") return fallback;
  const maybeErr = err as {
    message?: unknown;
    response?: {
      data?: {
        error?: unknown;
      };
    };
  };
  const responseError = maybeErr.response?.data?.error;
  if (typeof responseError === "string" && responseError.trim()) return responseError;
  if (typeof maybeErr.message === "string" && maybeErr.message.trim()) return maybeErr.message;
  return fallback;
};

export default function PositionsView() {
  const { ready: backendReady } = useBackendReadyState();
  const navigate = useNavigate();
  const location = useLocation();

  // Store access
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const boxesCache = useStore((state) => state.boxesCache);
  const maSettings = useStore((state) => state.maSettings);
  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);

  // Settings
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);

  const [tab, setTab] = useState<"held" | "history">("held");
  const [sortKey, setSortKey] = useState<PositionSortKey>("code");
  const [filterSignalsOnly, setFilterSignalsOnly] = useState(false);
  const [filterDataOnly, setFilterDataOnly] = useState(false);
  const [consultVisible, setConsultVisible] = useState(false);
  const [consultExpanded, setConsultExpanded] = useState(false);
  const [consultTab, setConsultTab] = useState<"selection" | "position">("selection");
  const [consultText, setConsultText] = useState("");
  const [consultSort, setConsultSort] = useState<ConsultationSort>("score");
  const [consultBusy, setConsultBusy] = useState(false);
  const [screenshotBusy, setScreenshotBusy] = useState(false);
  const [consultMeta, setConsultMeta] = useState<{ omitted: number }>({ omitted: 0 });
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";
  const [heldItems, setHeldItems] = useState<HeldItem[]>([]);
  const [historyItems, setHistoryItems] = useState<HistoryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedRound, setSelectedRound] = useState<HistoryItem | null>(null);
  const [roundEvents, setRoundEvents] = useState<RoundEvent[]>([]);
  const [eventsLoading, setEventsLoading] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(POSITIONS_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as {
        tab?: "held" | "history";
        sortKey?: PositionSortKey;
        filterSignalsOnly?: boolean;
        filterDataOnly?: boolean;
      };
      if (parsed.tab === "held" || parsed.tab === "history") {
        setTab(parsed.tab);
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
    } catch {
      // ignore storage failures
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const payload = {
        tab,
        sortKey,
        filterSignalsOnly,
        filterDataOnly
      };
      window.sessionStorage.setItem(POSITIONS_VIEW_STATE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }, [tab, sortKey, filterSignalsOnly, filterDataOnly]);

  useEffect(() => {
    if (!backendReady) return;
    if (tickers.length) return;
    loadList().catch(() => { });
  }, [backendReady, loadList, tickers.length]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);

  // Load positions
  useEffect(() => {
    if (!backendReady) return;
    setLoading(true);
    const load = async () => {
      try {
        const heldRes = await api.get("/positions/held");
        const holdings = (heldRes.data?.items || []) as HeldItem[];
        const holdingSet = new Set(holdings.map((item) => item.symbol));

        if (tab === "held") {
          setHeldItems(holdings);
          setHistoryItems([]);
        } else {
          const res = await api.get("/positions/history");
          const rawItems = (res.data?.items || []) as HistoryItem[];
          const filtered = rawItems
            .filter((item) => !holdingSet.has(item.symbol))
            .map((item, index) => ({ ...item, round_no: index + 1 }));
          setHistoryItems(filtered);
          setHeldItems([]);
        }
      } catch (e) {
        console.error(e);
        if (tab === "held") setHeldItems([]);
        else setHistoryItems([]);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [backendReady, tab]);

  const signalMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>["signals"]>();
    heldItems.forEach((item) => {
      if (!item?.symbol) return;
      const timeline = barsCache[listTimeframe];
      const payload = timeline ? timeline[item.symbol] : null;
      if (!payload?.bars?.length) return;
      const signals = computeSignalMetrics(payload.bars, 4).signals;
      if (signals.length) {
        map.set(item.symbol, signals);
      }
    });
    return map;
  }, [heldItems, barsCache, listTimeframe]);

  const filteredHeldItems = useMemo(() => {
    if (tab !== "held") return heldItems;
    if (!filterSignalsOnly && !filterDataOnly) return heldItems;
    return heldItems.filter((item) => {
      if (!item?.symbol) return false;
      const timeline = barsCache[listTimeframe];
      const payload = timeline ? timeline[item.symbol] : null;
      const hasData = Boolean(payload?.bars?.length);
      if (filterDataOnly && !hasData) return false;
      if (filterSignalsOnly && !signalMap.has(item.symbol)) return false;
      return true;
    });
  }, [tab, heldItems, filterSignalsOnly, filterDataOnly, barsCache, listTimeframe, signalMap]);

  const heldMetricsMap = useMemo(() => {
    const map = new Map<string, { change: number; score: number }>();
    filteredHeldItems.forEach((item) => {
      if (!item?.symbol) return;
      const timeline = barsCache[listTimeframe];
      const payload = timeline ? timeline[item.symbol] : null;
      const bars = payload?.bars ?? [];
      if (!bars.length) {
        map.set(item.symbol, { change: 0, score: 0 });
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
      map.set(item.symbol, { change, score });
    });
    return map;
  }, [filteredHeldItems, barsCache, listTimeframe]);

  const sortedHeldItems = useMemo(() => {
    const next = [...filteredHeldItems];
    if (sortKey === "code") {
      next.sort((a, b) => a.symbol.localeCompare(b.symbol, "ja"));
    } else if (sortKey === "change") {
      next.sort(
        (a, b) =>
          (heldMetricsMap.get(b.symbol)?.change ?? 0) -
          (heldMetricsMap.get(a.symbol)?.change ?? 0)
      );
    } else if (sortKey === "scoreUp") {
      next.sort(
        (a, b) =>
          (heldMetricsMap.get(b.symbol)?.score ?? 0) -
          (heldMetricsMap.get(a.symbol)?.score ?? 0)
      );
    } else if (sortKey === "scoreDown") {
      next.sort(
        (a, b) =>
          (heldMetricsMap.get(a.symbol)?.score ?? 0) -
          (heldMetricsMap.get(b.symbol)?.score ?? 0)
      );
    }
    return next;
  }, [filteredHeldItems, sortKey, heldMetricsMap]);

  // Determine active items
  const activeItems = useMemo(() => {
    return tab === "held" ? sortedHeldItems : historyItems;
  }, [tab, sortedHeldItems, historyItems]);

  const consultTargets = useMemo(
    () => (tab === "held" ? sortedHeldItems.map((item) => item.symbol) : []),
    [tab, sortedHeldItems]
  );

  useEffect(() => {
    if (!backendReady) return;
    if (tab !== "held") return;
    if (!heldItems.length) return;
    const codes = heldItems.map((item) => item.symbol);
    const uniqueCodes = [...new Set(codes)];
    ensureBarsForVisible(listTimeframe, uniqueCodes, "positions-held");
  }, [backendReady, tab, heldItems, ensureBarsForVisible, listTimeframe]);

  // Load detail for selected round
  useEffect(() => {
    if (!selectedRound) return;
    setEventsLoading(true);
    api
      .get("/positions/history/events", { params: { round_id: selectedRound.round_id } })
      .then((res) => {
        setRoundEvents((res.data?.events || []) as RoundEvent[]);
      })
      .catch(() => setRoundEvents([]))
      .finally(() => setEventsLoading(false));
  }, [selectedRound]);

  // Ensure bars are loaded for visible items
  useEffect(() => {
    if (!backendReady || activeItems.length === 0) return;
    const codes = activeItems.map((item) => item.symbol);
    // Unique list
    const uniqueCodes = [...new Set(codes)];
    ensureBarsForVisible(listTimeframe, uniqueCodes, "positions");
  }, [backendReady, activeItems, ensureBarsForVisible, listTimeframe]);

  const handleImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length) return;
    const file = e.target.files[0];
    const formData = new FormData();
    formData.append("file", file);
    // Let backend detect broker from the CSV contents/headers.
    formData.append("broker", "auto");

    try {
      setLoading(true);
      await api.post("/imports/trade-history", formData, {
        headers: { "Content-Type": "multipart/form-data" },
        timeout: 120000
      });
      alert("インポートが完了しました");

      // Reload
      if (tab === "held") {
        const res = await api.get("/positions/held");
        setHeldItems((res.data?.items || []) as HeldItem[]);
      } else {
        const res = await api.get("/positions/history");
        setHistoryItems((res.data?.items || []) as HistoryItem[]);
      }
    } catch (err: unknown) {
      console.error(err);
      const msg = extractErrorMessage(err);
      const warnings =
        err && typeof err === "object" && "response" in err
          ? ((err as { response?: { data?: { warnings?: unknown[] } } }).response?.data?.warnings ?? [])
          : [];
      const warnMsg = warnings.length ? "\n" + warnings.join("\n") : "";
      alert(`インポートに失敗しました: ${msg}${warnMsg}`);
    } finally {
      e.target.value = "";
    }
  };

  const handleOpenDetail = useCallback(
    (code: string) => {
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname]
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
        const held = heldItems.find((item) => item.symbol === code);
        const ticker = tickerMap.get(code);
        const payload = barsCache[consultTimeframe]?.[code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        return {
          code,
          name: held?.name ?? ticker?.name ?? null,
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
    consultBarsCount,
    heldItems,
    barsCache,
    boxesCache,
    consultSort,
    tickerMap
  ]);

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
    const targets = consultTargets.slice(0, SCREENSHOT_LIMIT);
    const omitted = Math.max(0, consultTargets.length - targets.length);
    setScreenshotBusy(true);
    try {
      try {
        await ensureBarsForVisible(listTimeframe, targets, "chart-screenshot");
      } catch {
        // Use available cache even if fetch fails.
      }
      const itemsForShots = targets.map((code) => ({
        code,
        payload: (barsCache[listTimeframe] && barsCache[listTimeframe][code]) ?? null,
        boxes: [],
        maSettings: maSettings[listTimeframe] ?? []
      }));
      const result = await downloadChartScreenshots(itemsForShots, {
        rangeBars: listRangeBars,
        timeframeLabel: listTimeframe
      });
      if (!result.created) {
        setToastMessage("スクショを作成できませんでした。");
        return;
      }
      const omittedLabel = omitted ? ` (残り${omitted}件は省略)` : "";
      setToastMessage(`スクショを${result.created}件作成しました。${omittedLabel}`);
    } finally {
      setScreenshotBusy(false);
    }
  }, [
    consultTargets,
    ensureBarsForVisible,
    listTimeframe,
    barsCache,
    maSettings,
    listRangeBars
  ]);

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = consultTargets.slice(0, limit);
    const extra = Math.max(0, consultTargets.length - visible.length);
    return { visible, extra };
  }, [consultTargets]);

  const densityKey = `${listColumns}x${listRows}`;
  const listStyles = useMemo(
    () =>
    ({
      "--list-cols": listColumns,
      "--list-rows": listRows
    } as CSSProperties),
    [listColumns, listRows]
  );

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
    () =>
      tab === "held"
        ? [
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
        ]
        : [],
    [tab, filterSignalsOnly, filterDataOnly]
  );

  const isSingleDensity = listColumns === 1 && listRows === 1;
  const emptyLabel = tab === "held" ? "保有銘柄はありません" : "履歴はまだありません";

  const renderItem = (item: HeldItem | HistoryItem) => {
    if ("buy_qty" in item && !(item.buy_qty > 0 || item.sell_qty > 0)) {
      return null;
    }
    const code = item.symbol;
    if (!code) return null;

    const timeline = barsCache[listTimeframe];
    const statusMap = barsStatus[listTimeframe];
    const payload = timeline ? timeline[code] ?? null : null;
    const status = statusMap ? statusMap[code] : undefined;
    const signals = "buy_qty" in item ? (signalMap.get(code) ?? []) : [];
    const ticker = tickerMap.get(code);

    let displayName = item.name;
    let extraInfo = "";

    if ("sell_buy_text" in item) {
      extraInfo = ` ${item.sell_buy_text}`;
      if (item.has_issue) extraInfo += " ⚠️";
    } else {
      extraInfo = ` Round${item.round_no}`;
      if (item.has_issue) extraInfo += " ⚠️";
    }

    const uniqueKey = "round_id" in item ? item.round_id : code;

    return (
      <ChartListCard
        key={uniqueKey}
        code={code}
        name={`${displayName}${extraInfo}`}
        payload={payload}
        status={status}
        maSettings={maSettings[listTimeframe]}
        rangeBars={listRangeBars}
        eventEarningsDate={ticker?.eventEarningsDate ?? null}
        eventRightsDate={ticker?.eventRightsDate ?? null}
        densityKey={densityKey}
        signals={signals}
        onOpenDetail={handleOpenDetail}
        phaseBody={ticker?.bodyScore ?? null}
        phaseEarly={ticker?.earlyScore ?? null}
        phaseLate={ticker?.lateScore ?? null}
        phaseN={ticker?.phaseN ?? null}
        action={undefined}
      />
    );
  };

  return (
    <div className="app-shell list-view">
      <UnifiedListHeader
        timeframe={listTimeframe}
        onTimeframeChange={setListTimeframe}
        rangeBars={listRangeBars}
        onRangeChange={setListRangeBars}
        search=""
        onSearchChange={() => { }}
        sortValue={sortKey}
        sortOptions={sortOptions}
        onSortChange={(value) => setSortKey(value as PositionSortKey)}
        columns={listColumns}
        rows={listRows}
        onColumnsChange={setListColumns}
        onRowsChange={setListRows}
        filterItems={filterItems}
        helpLabel="相談"
        onHelpClick={() => {
          if (tab !== "held") return;
          setConsultVisible(true);
          setConsultExpanded(false);
          setConsultTab("selection");
        }}
      />

      <div style={{
        padding: "8px 16px",
        display: "flex",
        gap: "12px",
        alignItems: "center",
        borderBottom: "1px solid var(--theme-border)",
        background: "var(--theme-bg-secondary)"
      }}>
        <div className="positions-tabs" style={{ display: "flex", gap: "8px" }}>
          <button
            type="button"
            className={tab === "held" ? "active" : ""}
            onClick={() => { setSelectedRound(null); setTab("held"); }}
            style={{
              padding: "6px 12px",
              borderRadius: "999px",
              border: "none",
              background: tab === "held" ? "var(--theme-accent)" : "transparent",
              color: tab === "held" ? "#fff" : "var(--theme-text-secondary)",
              cursor: "pointer",
              fontWeight: 600
            }}
          >
            保有
          </button>
          <button
            type="button"
            className={tab === "history" ? "active" : ""}
            onClick={() => { setConsultVisible(false); setTab("history"); }}
            style={{
              padding: "6px 12px",
              borderRadius: "999px",
              border: "none",
              background: tab === "history" ? "var(--theme-accent)" : "transparent",
              color: tab === "history" ? "#fff" : "var(--theme-text-secondary)",
              cursor: "pointer",
              fontWeight: 600
            }}
          >
            履歴
          </button>
        </div>

        <div style={{ flex: 1 }}></div>

        <button
          className="positions-import-btn"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "6px",
            padding: "6px 12px",
            background: "var(--theme-bg-tertiary)",
            border: "1px solid var(--theme-border)",
            borderRadius: "6px",
            cursor: "pointer",
            fontSize: "13px"
          }}
          onClick={async () => {
            if (!window.confirm("建玉を再計算しますか？")) return;
            try {
              const res = await api.post("/positions/rebuild");
              const data = res.data;
              if (data.success) {
                alert(`再計算完了: ${data.message}`);
                window.location.reload();
              } else {
                alert(`エラー: ${data.message}`);
              }
            } catch (err: unknown) {
              alert(`再計算に失敗しました: ${extractErrorMessage(err)}`);
            }
          }}
        >
          <IconRefresh size={16} />
          <span>再計算</span>
        </button>

        <label className="positions-import-btn" style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "6px",
          padding: "6px 12px",
          background: "var(--theme-bg-tertiary)",
          border: "1px solid var(--theme-border)",
          borderRadius: "6px",
          cursor: "pointer",
          fontSize: "13px"
        }}>
          <IconUpload size={16} />
          <span>インポート</span>
          <input type="file" accept=".csv" onChange={handleImport} hidden />
        </label>
      </div>

      <div
        className={`rank-shell list-shell${isSingleDensity ? " is-single" : ""} ${tab === "held" ? consultPaddingClass : ""}`}
        style={listStyles}
      >
        {loading && <div className="rank-status">読み込み中...</div>}
        {!loading && activeItems.length === 0 && (
          <div className="positions-empty" style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            height: "100%",
            gap: "16px",
            color: "var(--theme-text-muted)"
          }}>
            <div>{emptyLabel}</div>
            <button
              type="button"
              onClick={async () => {
                try {
                  const res = await api.get("/debug/trade-sync");
                  alert(JSON.stringify(res.data, null, 2));
                } catch (e) {
                  alert("Debug fetch failed");
                }
              }}
              style={{
                fontSize: "0.8rem",
                opacity: 0.7,
                background: "transparent",
                border: "1px solid currentColor",
                borderRadius: 4,
                padding: "4px 8px",
                color: "inherit",
                cursor: "pointer"
              }}
            >
              同期ステータス詳細
            </button>
          </div>
        )}

        <div className="rank-grid">
          {activeItems.map((item) => renderItem(item))}
        </div>
      </div>

      <div
        className={`consult-sheet ${consultVisible ? "is-visible" : "is-hidden"} ${consultExpanded ? "is-expanded" : "is-mini"}`}
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
              <div className="consult-mini-count">保有 {consultTargets.length}件</div>
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
                <button type="button" onClick={() => setConsultVisible(false)}>
                  閉じる
                </button>
              </div>
            </div>
            <div className="consult-expanded-body">
              <div className="consult-expanded-meta-row">
                <div className="consult-expanded-meta">
                  保有 {consultTargets.length}件
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
      <Toast message={toastMessage} onClose={() => setToastMessage(null)} />

      {selectedRound && (
        <div className="positions-detail" style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.5)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          zIndex: 1000
        }}>
          <div style={{
            background: "var(--theme-bg-secondary)",
            width: "min(600px, 90vw)",
            borderRadius: "12px",
            overflow: "hidden",
            boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
            color: "var(--theme-text-primary)"
          }}>
            <div style={{
              padding: "16px",
              borderBottom: "1px solid var(--theme-border)",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center"
            }}>
              <div style={{ fontWeight: 600, fontSize: "16px" }}>
                {selectedRound.symbol} Round {selectedRound.round_no}
              </div>
              <button onClick={() => setSelectedRound(null)} style={{
                background: "transparent", border: "none", cursor: "pointer", fontSize: "20px", color: "var(--theme-text-secondary)"
              }}>×</button>
            </div>

            <div style={{ padding: "16px", maxHeight: "60vh", overflowY: "auto" }}>
              {eventsLoading ? (
                <div>読み込み中...</div>
              ) : roundEvents.length ? (
                <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                  {roundEvents.map((event, index) => (
                    <div key={`${event.exec_dt}-${index}`} style={{
                      display: "grid",
                      gridTemplateColumns: "100px 80px 80px 1fr",
                      gap: "12px",
                      padding: "8px",
                      borderBottom: "1px solid var(--theme-border-subtle)",
                      fontSize: "13px"
                    }}>
                      <span>{formatDate(event.exec_dt)}</span>
                      <span style={{ fontWeight: 600 }}>{event.action}</span>
                      <span>{event.qty}</span>
                      <span style={{ textAlign: "right" }}>
                        {event.price != null ? event.price.toLocaleString() : "--"}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div>イベントがありません</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
