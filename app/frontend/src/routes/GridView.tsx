import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  FixedSizeGrid as Grid,
  type FixedSizeGrid,
  type GridOnItemsRenderedProps
} from "react-window";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import type { MaSetting, SortDir, SortKey } from "../store";
import { useStore } from "../store";
import StockTile from "../components/StockTile";
import Toast from "../components/Toast";
import TopNav from "../components/TopNav";
import { computeSignalMetrics } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { applyTheme, getStoredTheme, setStoredTheme, toggleTheme, type Theme } from "../utils/theme";

const GRID_GAP = 12;
const KEEP_LIMIT = 24;
type Timeframe = "monthly" | "weekly" | "daily";
type SortOption = { key: SortKey; label: string };
type SortSection = { title: string; options: SortOption[] };

function useResizeObserver() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    if (!ref.current) return;
    const element = ref.current;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        setSize({ width, height });
      }
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  return { ref, size };
}

type HealthStatus = {
  txt_count: number;
  code_count: number;
  last_updated: string | null;
  code_txt_missing: boolean;
};

export default function GridView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ref, size } = useResizeObserver();
  const { ready: backendReady } = useBackendReadyState();
  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);
  const loadingList = useStore((state) => state.loadingList);
  const resetBarsCache = useStore((state) => state.resetBarsCache);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const boxesCache = useStore((state) => state.boxesCache);
  const columns = useStore((state) => state.settings.columns);
  const rows = useStore((state) => state.settings.rows);
  const search = useStore((state) => state.settings.search);
  const gridScrollTop = useStore((state) => state.settings.gridScrollTop);
  const gridTimeframe = useStore((state) => state.settings.gridTimeframe);
  const keepList = useStore((state) => state.keepList);
  const addKeep = useStore((state) => state.addKeep);
  const removeKeep = useStore((state) => state.removeKeep);
  const clearKeep = useStore((state) => state.clearKeep);
  const setColumns = useStore((state) => state.setColumns);
  const setRows = useStore((state) => state.setRows);
  const setSearch = useStore((state) => state.setSearch);
  const setGridScrollTop = useStore((state) => state.setGridScrollTop);
  const setGridTimeframe = useStore((state) => state.setGridTimeframe);
  const showBoxes = useStore((state) => state.settings.showBoxes);
  const setShowBoxes = useStore((state) => state.setShowBoxes);
  const sortKey = useStore((state) => state.settings.sortKey);
  const sortDir = useStore((state) => state.settings.sortDir);
  const setSortKey = useStore((state) => state.setSortKey);
  const setSortDir = useStore((state) => state.setSortDir);
  const maSettings = useStore((state) => state.maSettings);
  const updateMaSetting = useStore((state) => state.updateMaSetting);
  const resetMaSettings = useStore((state) => state.resetMaSettings);

  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [showIndicators, setShowIndicators] = useState(false);
  const [sortOpen, setSortOpen] = useState(false);
  const [displayOpen, setDisplayOpen] = useState(false);
  const [isSorting, setIsSorting] = useState(false);
  const [updateRequestInFlight, setUpdateRequestInFlight] = useState(false);
  const [txtUpdateStatus, setTxtUpdateStatus] = useState<TxtUpdateStatus | null>(null);
  const [splitSuspects, setSplitSuspects] = useState<SplitSuspect[]>([]);
  const [showSplitSuspects, setShowSplitSuspects] = useState(false);
  const [updateLogLines, setUpdateLogLines] = useState<string[]>([]);
  const [showUpdateLog, setShowUpdateLog] = useState(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const [consultVisible, setConsultVisible] = useState(false);
  const [consultExpanded, setConsultExpanded] = useState(false);
  const [consultTab, setConsultTab] = useState<"selection" | "position">("selection");
  const [consultText, setConsultText] = useState("");
  const [consultSort, setConsultSort] = useState<ConsultationSort>("score");
  const [consultBusy, setConsultBusy] = useState(false);
  const [consultMeta, setConsultMeta] = useState<{ omitted: number }>({ omitted: 0 });
  const [undoInfo, setUndoInfo] = useState<{ code: string; trashToken?: string | null } | null>(
    null
  );
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [currentTheme, setCurrentTheme] = useState<Theme>(() => getStoredTheme());
  const sortRef = useRef<HTMLDivElement | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const settingsRef = useRef<HTMLDivElement | null>(null);
  const gridRef = useRef<FixedSizeGrid | null>(null);
  const prevUpdateRunningRef = useRef(false);
  const lastVisibleCodesRef = useRef<string[]>([]);
  const lastVisibleRangeRef = useRef<{ start: number; stop: number } | null>(null);
  const undoTimerRef = useRef<number | null>(null);
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";

  useEffect(() => {
    if (!backendReady) return;
    loadList();
  }, [backendReady, loadList]);

  useEffect(() => {
    if (!backendReady) return;
    api
      .get("/health", { validateStatus: () => true })
      .then((res) => {
        if (res.status >= 200 && res.status < 300) {
          setHealth(res.data as HealthStatus);
        }
      })
      .catch(() => undefined);
  }, [backendReady]);

  useEffect(() => {
    if (!sortOpen && !displayOpen && !settingsOpen) return;
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (sortRef.current && sortRef.current.contains(target)) return;
      if (displayRef.current && displayRef.current.contains(target)) return;
      if (settingsRef.current && settingsRef.current.contains(target)) return;
      setSortOpen(false);
      setDisplayOpen(false);
      setSettingsOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [sortOpen, displayOpen, settingsOpen]);

  useEffect(() => {
    return () => {
      if (undoTimerRef.current) {
        window.clearTimeout(undoTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    setIsSorting(true);
    const timer = window.setTimeout(() => setIsSorting(false), 120);
    return () => window.clearTimeout(timer);
  }, [sortKey, sortDir]);

  const sortSections = useMemo<SortSection[]>(
    () => [
      {
        title: "買い候補",
        options: [
          { key: "buyCandidate", label: "買い候補（初動→底がため）" },
          { key: "buyInitial", label: "買い候補（初動のみ）" },
          { key: "buyBase", label: "監視（底がためのみ）" }
        ]
      },
      {
        title: "基本",
        options: [
          { key: "code", label: "コード" },
          { key: "name", label: "銘柄名" }
        ]
      },
      {
        title: "騰落率（直近）",
        options: [
          { key: "chg1D", label: "騰落率（1日）" },
          { key: "chg1W", label: "騰落率（1週）" },
          { key: "chg1M", label: "騰落率（1ヶ月）" },
          { key: "chg1Q", label: "騰落率（3ヶ月）" },
          { key: "chg1Y", label: "騰落率（1年）" }
        ]
      },
      {
        title: "騰落率（確定期間）",
        options: [
          { key: "prevWeekChg", label: "前週騰落率" },
          { key: "prevMonthChg", label: "前月騰落率" },
          { key: "prevQuarterChg", label: "前四半期騰落率" },
          { key: "prevYearChg", label: "前年騰落率" }
        ]
      },
      {
        title: "スコア",
        options: [
          { key: "upScore", label: "上昇スコア" },
          { key: "downScore", label: "下落スコア" },
          { key: "overheatUp", label: "過熱（上）" },
          { key: "overheatDown", label: "過熱（下）" }
        ]
      },
      {
        title: "ボックス",
        options: [{ key: "boxState", label: "ボックス状態" }]
      }
    ],
    []
  );

  const sortOptions = useMemo(
    () => sortSections.flatMap((section) => section.options),
    [sortSections]
  );

  const sortLabel = useMemo(
    () => sortOptions.find((option) => option.key === sortKey)?.label ?? "コード",
    [sortOptions, sortKey]
  );

  const sortDirLabel = sortDir === "desc" ? "降順" : "昇順";

  const normalizeWatchCode = useCallback((value: string) => {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const fullwidth = "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ";
    const halfwidth = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let normalized = "";
    for (const ch of trimmed) {
      const idx = fullwidth.indexOf(ch);
      normalized += idx >= 0 ? halfwidth[idx] : ch;
    }
    normalized = normalized.replace(/\s+/g, "").toUpperCase();
    if (!/^\d{4}[A-Z]?$/.test(normalized)) return null;
    return normalized;
  }, []);

  const normalizedSearch = useMemo(
    () => (search ? normalizeWatchCode(search) : null),
    [search, normalizeWatchCode]
  );

  const filtered = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return tickers;
    return tickers.filter((item) => {
      return item.code.toLowerCase().includes(term) || item.name.toLowerCase().includes(term);
    });
  }, [tickers, search]);

  const canAddWatchlist = useMemo(() => {
    if (!normalizedSearch) return null;
    if (filtered.length > 0) return null;
    if (tickers.some((item) => item.code === normalizedSearch)) return null;
    return normalizedSearch;
  }, [normalizedSearch, filtered.length, tickers]);

  const scoredTickers = useMemo(() => {
    return filtered.map((ticker, index) => {
      const payload = barsCache[gridTimeframe][ticker.code];
      const metrics = payload?.bars?.length ? computeSignalMetrics(payload.bars, 4) : null;
      return { ticker, metrics, index };
    });
  }, [filtered, barsCache, gridTimeframe]);

  const collator = useMemo(
    () => new Intl.Collator("ja-JP", { numeric: true, sensitivity: "base" }),
    []
  );

  const sortedTickers = useMemo(() => {
    const boxOrder: Record<string, number> = {
      IN_BOX: 3,
      JUST_BREAKOUT: 2,
      BREAKOUT_UP: 2,
      BREAKOUT_DOWN: 2,
      NONE: 0
    };
    const isBuyCandidate =
      sortKey === "buyCandidate" || sortKey === "buyInitial" || sortKey === "buyBase";

    const items = scoredTickers.map((item) => {
      const ticker = item.ticker;
      let sortValue: string | number | null = null;
      if ((sortKey === "upScore" || sortKey === "downScore") && ticker.statusLabel === "UNKNOWN") {
        sortValue = null;
      } else if (sortKey === "code") {
        sortValue = ticker.code;
      } else if (sortKey === "name") {
        sortValue = ticker.name ?? "";
      } else if (sortKey === "chg1D") {
        sortValue = ticker.chg1D ?? null;
      } else if (sortKey === "chg1W") {
        sortValue = ticker.chg1W ?? null;
      } else if (sortKey === "chg1M") {
        sortValue = ticker.chg1M ?? null;
      } else if (sortKey === "chg1Q") {
        sortValue = ticker.chg1Q ?? null;
      } else if (sortKey === "chg1Y") {
        sortValue = ticker.chg1Y ?? null;
      } else if (sortKey === "prevWeekChg") {
        sortValue = ticker.prevWeekChg ?? null;
      } else if (sortKey === "prevMonthChg") {
        sortValue = ticker.prevMonthChg ?? null;
      } else if (sortKey === "prevQuarterChg") {
        sortValue = ticker.prevQuarterChg ?? null;
      } else if (sortKey === "prevYearChg") {
        sortValue = ticker.prevYearChg ?? null;
      } else if (sortKey === "upScore") {
        sortValue = ticker.scores?.upScore ?? null;
      } else if (sortKey === "downScore") {
        sortValue = ticker.scores?.downScore ?? null;
      } else if (sortKey === "overheatUp") {
        sortValue = ticker.scores?.overheatUp ?? null;
      } else if (sortKey === "overheatDown") {
        sortValue = ticker.scores?.overheatDown ?? null;
      } else if (sortKey === "boxState") {
        const state = ticker.boxState ?? "NONE";
        sortValue = boxOrder[state] ?? 0;
      } else if (isBuyCandidate) {
        sortValue = null;
      }
      return { ...item, sortValue };
    });

    const compareNumeric = (av: number | null, bv: number | null, dir: SortDir) => {
      const aMissing = av == null || !Number.isFinite(av);
      const bMissing = bv == null || !Number.isFinite(bv);
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;
      const diff = (av ?? 0) - (bv ?? 0);
      return dir === "desc" ? -diff : diff;
    };

    const compareBuyState = (a: typeof items[number], b: typeof items[number]) => {
      const aState = a.ticker.buyState ?? "";
      const bState = b.ticker.buyState ?? "";
      const aRank = Number.isFinite(a.ticker.buyStateRank)
        ? (a.ticker.buyStateRank as number)
        : 0;
      const bRank = Number.isFinite(b.ticker.buyStateRank)
        ? (b.ticker.buyStateRank as number)
        : 0;
      const aScore = Number.isFinite(a.ticker.buyStateScore)
        ? (a.ticker.buyStateScore as number)
        : null;
      const bScore = Number.isFinite(b.ticker.buyStateScore)
        ? (b.ticker.buyStateScore as number)
        : null;
      const aRisk = Number.isFinite(a.ticker.buyRiskDistance)
        ? (a.ticker.buyRiskDistance as number)
        : null;
      const bRisk = Number.isFinite(b.ticker.buyRiskDistance)
        ? (b.ticker.buyRiskDistance as number)
        : null;

      if (sortKey === "buyInitial" || sortKey === "buyBase") {
        const target = sortKey === "buyInitial" ? "初動" : "底がため";
        const aEligible = aState === target;
        const bEligible = bState === target;
        if (aEligible !== bEligible) return aEligible ? -1 : 1;
        if (!aEligible && !bEligible) return a.ticker.code.localeCompare(b.ticker.code);
        const scoreResult = compareNumeric(aScore, bScore, sortDir);
        if (scoreResult !== 0) return scoreResult;
        const riskResult = compareNumeric(aRisk, bRisk, "asc");
        if (riskResult !== 0) return riskResult;
        return a.ticker.code.localeCompare(b.ticker.code);
      }

      if (aRank !== bRank) return bRank - aRank;
      const scoreResult = compareNumeric(aScore, bScore, sortDir);
      if (scoreResult !== 0) return scoreResult;
      const riskResult = compareNumeric(aRisk, bRisk, "asc");
      if (riskResult !== 0) return riskResult;
      const totalResult = compareNumeric(a.ticker.score ?? null, b.ticker.score ?? null, "desc");
      if (totalResult !== 0) return totalResult;
      return a.ticker.code.localeCompare(b.ticker.code);
    };

    const compare = (a: typeof items[number], b: typeof items[number]) => {
      if (isBuyCandidate) {
        return compareBuyState(a, b);
      }
      const av = a.sortValue;
      const bv = b.sortValue;
      const aMissing =
        av === null ||
        av === undefined ||
        (typeof av === "number" && !Number.isFinite(av)) ||
        (typeof av === "string" && av.trim() === "");
      const bMissing =
        bv === null ||
        bv === undefined ||
        (typeof bv === "number" && !Number.isFinite(bv)) ||
        (typeof bv === "string" && bv.trim() === "");
      if (aMissing && bMissing) return a.ticker.code.localeCompare(b.ticker.code);
      if (aMissing) return 1;
      if (bMissing) return -1;
      let result = 0;
      if (typeof av === "string" || typeof bv === "string") {
        result = collator.compare(String(av), String(bv));
      } else {
        result = Number(av) - Number(bv);
      }
      if (result === 0) return a.ticker.code.localeCompare(b.ticker.code);
      return sortDir === "desc" ? -result : result;
    };
    items.sort(compare);
    return items;
  }, [scoredTickers, sortKey, sortDir, collator]);
  const sortedCodes = useMemo(
    () => sortedTickers.map((item) => item.ticker.code),
    [sortedTickers]
  );

  useEffect(() => {
    if (sortedTickers.length === 0) {
      setActiveIndex(0);
      return;
    }
    setActiveIndex((prev) => Math.min(Math.max(0, prev), sortedTickers.length - 1));
  }, [sortedTickers.length]);

  useEffect(() => {
    if (!sortedTickers.length || columns <= 0) return;
    const rowIndex = Math.floor(activeIndex / columns);
    const columnIndex = activeIndex % columns;
    gridRef.current?.scrollToItem({ rowIndex, columnIndex, align: "smart" });
  }, [activeIndex, sortedTickers.length, columns]);

  const tickerMap = useMemo(() => {
    const map = new Map<string, typeof tickers[number]>();
    tickers.forEach((ticker) => map.set(ticker.code, ticker));
    return map;
  }, [tickers]);

  const keepSet = useMemo(() => new Set(keepList), [keepList]);
  const activeItem = sortedTickers[activeIndex] ?? null;
  const activeCode = activeItem?.ticker.code ?? null;
  const moveActive = useCallback(
    (delta: number) => {
      if (!sortedTickers.length) return;
      setActiveIndex((prev) =>
        Math.min(Math.max(0, prev + delta), Math.max(0, sortedTickers.length - 1))
      );
    },
    [sortedTickers.length]
  );
  const activateByCode = useCallback(
    (code: string) => {
      if (!code) return;
      const index = sortedTickers.findIndex((item) => item.ticker.code === code);
      if (index >= 0) setActiveIndex(index);
    },
    [sortedTickers]
  );

  const gridHeight = Math.max(200, size.height);
  const gridWidth = Math.max(0, size.width);
  const rowHeight = Math.max(1, Math.floor(gridHeight / Math.max(1, rows)));
  const innerHeight = Math.max(0, gridHeight);
  const rowCount = Math.ceil(sortedTickers.length / columns);
  const columnWidth = gridWidth > 0 ? gridWidth / columns : 300;
  const showSkeleton = backendReady && loadingList && tickers.length === 0;

  const onItemsRendered = ({
    visibleRowStartIndex,
    visibleRowStopIndex,
    visibleColumnStartIndex,
    visibleColumnStopIndex
  }: GridOnItemsRenderedProps) => {
    if (!backendReady) return;
    const rowsPerViewport = Math.max(1, Math.floor(gridHeight / rowHeight));
    const prefetchStop = visibleRowStopIndex + rowsPerViewport;
    const start = visibleRowStartIndex * columns + visibleColumnStartIndex;
    const stop = Math.min(
      sortedTickers.length - 1,
      prefetchStop * columns + visibleColumnStopIndex
    );
    if (start > stop) return;
    const codes: string[] = [];
    for (let index = start; index <= stop; index += 1) {
      const item = sortedTickers[index];
      if (item) codes.push(item.ticker.code);
    }
    lastVisibleCodesRef.current = codes;
    lastVisibleRangeRef.current = { start, stop };
    ensureBarsForVisible(gridTimeframe, codes, "scroll");
  };

  useEffect(() => {
    if (!backendReady) return;
    if (!lastVisibleCodesRef.current.length) return;
    ensureBarsForVisible(gridTimeframe, lastVisibleCodesRef.current, "timeframe-change");
  }, [backendReady, gridTimeframe, maSettings, ensureBarsForVisible]);

  useEffect(() => {
    if (!backendReady) return;
    const range = lastVisibleRangeRef.current;
    if (!range) return;
    const codes: string[] = [];
    for (let index = range.start; index <= range.stop; index += 1) {
      const item = sortedTickers[index];
      if (item) codes.push(item.ticker.code);
    }
    if (!codes.length) return;
    ensureBarsForVisible(gridTimeframe, codes, "sort-change");
  }, [backendReady, sortedTickers, gridTimeframe, ensureBarsForVisible]);

  const itemKey = useCallback(
    ({
      columnIndex,
      rowIndex,
      data
    }: {
      columnIndex: number;
      rowIndex: number;
      data: typeof sortedTickers;
    }) => {
      const index = rowIndex * columns + columnIndex;
      const item = data[index];
      return item ? item.ticker.code : `${rowIndex}-${columnIndex}`;
    },
    [columns]
  );

  const handleOpenDetail = useCallback(
    (code: string) => {
      try {
        sessionStorage.setItem("detailListBack", location.pathname);
        sessionStorage.setItem("detailListCodes", JSON.stringify(sortedCodes));
      } catch {
        // ignore storage failures
      }
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname, sortedCodes]
  );

  const handleAddWatchlist = useCallback(
    async (code: string) => {
      if (!code) return;
      try {
        const res = await api.post("/watchlist/add", { code });
        const already = Boolean(res.data?.alreadyExisted);
        await loadList();
        setToastMessage(
          already
            ? `${code} は既に追加済みです。`
            : `${code} を追加しました。次回TXT更新で反映されます。`
        );
      } catch {
        setToastMessage("ウォッチリスト追加に失敗しました。");
      }
    },
    [loadList]
  );

  const handleRemoveWatchlist = useCallback(
    async (code: string, deleteArtifacts: boolean) => {
      if (!code) return;
      try {
        const res = await api.post("/watchlist/remove", { code, deleteArtifacts });
        await loadList();
        const trashToken = res.data?.trashToken || null;
        setUndoInfo({ code, trashToken });
        if (undoTimerRef.current) {
          window.clearTimeout(undoTimerRef.current);
        }
        undoTimerRef.current = window.setTimeout(() => {
          setUndoInfo(null);
        }, 5000);
        setToastMessage(`${code} を除外しました。`);
      } catch (error) {
        const message = error instanceof Error ? error.message : "ウォッチリスト削除に失敗しました。";
        setToastMessage(message);
      }
    },
    [loadList]
  );

  const handleToggleKeep = useCallback(
    (code: string) => {
      if (!code) return;
      if (keepList.includes(code)) {
        removeKeep(code);
        return;
      }
      if (keepList.length >= KEEP_LIMIT) {
        setToastMessage(`候補箱は最大${KEEP_LIMIT}件までです。`);
        return;
      }
      addKeep(code);
    },
    [keepList, addKeep, removeKeep]
  );

  const handleExclude = useCallback(
    (code: string) => {
      if (!code) return;
      handleRemoveWatchlist(code, false);
    },
    [handleRemoveWatchlist]
  );

  const handleKeepNavigate = useCallback(
    (code: string) => {
      if (!code) return;
      const index = sortedTickers.findIndex((item) => item.ticker.code === code);
      if (index >= 0) {
        setActiveIndex(index);
        return;
      }
      try {
        sessionStorage.setItem("detailListBack", location.pathname);
        sessionStorage.setItem("detailListCodes", JSON.stringify(keepList));
      } catch {
        // ignore storage failures
      }
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [sortedTickers, navigate, location.pathname, keepList]
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target) {
        const tag = target.tagName;
        if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target.isContentEditable) {
          return;
        }
      }
      const key = event.key.toLowerCase();
      if (event.key === "Escape") {
        setSortOpen(false);
        setDisplayOpen(false);
        if (consultVisible) {
          setConsultVisible(false);
        }
        return;
      }
      if (key === "arrowdown" || key === "j") {
        event.preventDefault();
        moveActive(columns);
        return;
      }
      if (key === "arrowup" || key === "k") {
        event.preventDefault();
        moveActive(-columns);
        return;
      }
      if (key === "arrowleft" || key === "h") {
        event.preventDefault();
        moveActive(-1);
        return;
      }
      if (key === "arrowright" || key === "l") {
        event.preventDefault();
        moveActive(1);
        return;
      }
      if (key === "enter" && activeCode) {
        event.preventDefault();
        handleOpenDetail(activeCode);
        return;
      }
      if (key === "s" && activeCode) {
        event.preventDefault();
        handleToggleKeep(activeCode);
        return;
      }
      if (key === "e" && activeCode) {
        event.preventDefault();
        handleExclude(activeCode);
        return;
      }
      if (event.key === "1") {
        setGridTimeframe("monthly");
      } else if (event.key === "2") {
        setGridTimeframe("weekly");
      } else if (event.key === "3") {
        setGridTimeframe("daily");
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [
    setGridTimeframe,
    consultVisible,
    columns,
    moveActive,
    activeCode,
    handleOpenDetail,
    handleToggleKeep,
    handleExclude
  ]);

  const handleUndoRemove = useCallback(async () => {
    if (!undoInfo) return;
    try {
      await api.post("/watchlist/undo_remove", {
        code: undoInfo.code,
        trashToken: undoInfo.trashToken
      });
      await loadList();
      setToastMessage(`${undoInfo.code} を復元しました。`);
    } catch {
      setToastMessage("復元に失敗しました。");
    } finally {
      if (undoTimerRef.current) {
        window.clearTimeout(undoTimerRef.current);
      }
      setUndoInfo(null);
    }
  }, [undoInfo, loadList]);

  const resetDisplay = useCallback(() => {
    setColumns(3);
    setRows(3);
    setShowBoxes(true);
  }, [setColumns, setRows, setShowBoxes]);

  const updateSetting = (frame: Timeframe, index: number, patch: Partial<MaSetting>) => {
    updateMaSetting(frame, index, patch);
  };

  const resetSettings = (frame: Timeframe) => {
    resetMaSettings(frame);
  };

  const handleThemeToggle = useCallback(() => {
    const next = toggleTheme(currentTheme);
    setCurrentTheme(next);
    setStoredTheme(next);
    applyTheme(next);
  }, [currentTheme]);

  type UpdateSummary = {
    total?: number;
    ok?: number;
    err?: number;
    split?: number;
  };

  type UpdateTxtPayload = {
    ok?: boolean;
    error?: string;
    last_updated_at?: string;
    summary?: UpdateSummary;
    searched?: string[];
    stdout_tail?: string[];
  };

  type TxtUpdateStatus = {
    running?: boolean;
    phase?: string;
    started_at?: string;
    finished_at?: string;
    processed?: number;
    total?: number;
    summary?: UpdateSummary;
    error?: string | null;
    last_updated_at?: string | null;
    stdout_tail?: string[];
    elapsed_ms?: number | null;
    timeout_sec?: number;
    warning?: boolean;
  };

  type SplitSuspect = {
    code: string;
    file_date?: string;
    file_close?: string;
    pan_date?: string;
    pan_close?: string;
    diff_ratio?: string;
    reason?: string;
    detected_at?: string;
  };

  const formatUpdatedAt = (value: string | null | undefined) => {
    if (!value) return null;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    const pad = (num: number) => String(num).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(
      date.getHours()
    )}:${pad(date.getMinutes())}`;
  };

  const lastUpdatedLabel = formatUpdatedAt(
    (txtUpdateStatus?.last_updated_at as string | null | undefined) ?? health?.last_updated
  );
  const isUpdatingTxt = updateRequestInFlight || Boolean(txtUpdateStatus?.running);
  const updateProgressLabel = (() => {
    if (!txtUpdateStatus?.running) return null;
    if (txtUpdateStatus.phase === "ingesting") return "取り込み中";
    if (
      typeof txtUpdateStatus.processed === "number" &&
      typeof txtUpdateStatus.total === "number" &&
      txtUpdateStatus.total > 0
    ) {
      return `更新中 ${txtUpdateStatus.processed}/${txtUpdateStatus.total}`;
    }
    return "更新中";
  })();

  const formatUpdateSummary = (summary?: UpdateSummary) => {
    if (!summary) return null;
    const parts: string[] = [];
    if (typeof summary.ok === "number") {
      parts.push(`成功 ${summary.ok}`);
    }
    if (typeof summary.err === "number" && summary.err > 0) {
      parts.push(`エラー ${summary.err}`);
    }
    if (typeof summary.split === "number" && summary.split > 0) {
      parts.push(`分割疑い ${summary.split}`);
    }
    return parts.length > 0 ? parts.join(" / ") : null;
  };

  const formatUpdateToast = (message: string, summary?: UpdateSummary) => {
    const suffix = formatUpdateSummary(summary);
    return suffix ? `${message}（${suffix}）` : message;
  };

  const handleUpdateError = (payload?: UpdateTxtPayload) => {
    const error = payload?.error ?? "unknown";
    if (error === "already_updated_today") {
      const lastUpdated = formatUpdatedAt(payload?.last_updated_at);
      setToastMessage(
        lastUpdated
          ? `本日はTXT更新済みです（最終 ${lastUpdated}）`
          : "本日はTXT更新済みです。"
      );
      return;
    }
    if (error === "update_in_progress") {
      setToastMessage("TXT更新は実行中です。");
      return;
    }
    if (error.startsWith("vbs_failed")) {
      setToastMessage(formatUpdateToast("TXT更新でエラーが発生しました。", payload?.summary));
      return;
    }
    if (error.startsWith("ingest_failed")) {
      setToastMessage(formatUpdateToast("TXT取り込みでエラーが発生しました。", payload?.summary));
      return;
    }
    if (error.startsWith("vbs_not_found")) {
      setToastMessage("TXT更新スクリプトが見つかりません。");
      return;
    }
    if (error === "code_txt_missing") {
      const searched = payload?.searched?.filter(Boolean).join(" / ");
      setToastMessage(
        searched ? `code.txt が見つかりません（探索: ${searched}）` : "code.txt が見つかりません。"
      );
      return;
    }
    if (error.startsWith("ingest_not_found")) {
      setToastMessage("TXT取り込みスクリプトが見つかりません。");
      return;
    }
    setToastMessage("TXT更新に失敗しました。");
  };

  const fetchTxtUpdateStatus = useCallback(async () => {
    if (!backendReady) return;
    try {
      const res = await api.get("/txt_update/status");
      const payload = res.data as TxtUpdateStatus;
      setTxtUpdateStatus(payload);
      if (payload.stdout_tail && payload.stdout_tail.length) {
        setUpdateLogLines(payload.stdout_tail);
      }
    } catch {
      // Ignore status fetch errors while offline.
    }
  }, [backendReady]);

  const fetchSplitSuspects = useCallback(async () => {
    if (!backendReady) return [];
    try {
      const res = await api.get("/txt_update/split_suspects");
      const items = (res.data?.items as SplitSuspect[]) ?? [];
      setSplitSuspects(items);
      return items;
    } catch {
      return [];
    }
  }, [backendReady]);

  const buildConsultation = useCallback(async () => {
    if (!keepList.length) return;
    setConsultBusy(true);
    try {
      try {
        await ensureBarsForVisible(consultTimeframe, keepList, "consult-pack");
      } catch {
        // Use available cache even if fetch fails.
      }
      const items = keepList.map((code) => {
        const ticker = tickerMap.get(code);
        const payload = barsCache[consultTimeframe][code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        return {
          code,
          name: ticker?.name ?? null,
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
        items,
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
    keepList,
    ensureBarsForVisible,
    consultTimeframe,
    barsCache,
    boxesCache,
    tickerMap,
    consultSort
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

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = keepList.slice(0, limit);
    const extra = Math.max(0, keepList.length - visible.length);
    return { visible, extra };
  }, [keepList]);

  const handleUpdateTxt = useCallback(async () => {
    if (isUpdatingTxt || !backendReady) return;
    setUpdateRequestInFlight(true);
    setShowSplitSuspects(false);
    setSplitSuspects([]);
    setShowUpdateLog(false);
    setUpdateLogLines([]);
    setToastMessage("TXT更新を開始しました。");
    try {
      const res = await api.post("/txt_update/run");
      const payload = res.data as UpdateTxtPayload;
      if (payload.ok) {
        await fetchTxtUpdateStatus();
      } else {
        handleUpdateError(payload);
      }
    } catch (error) {
      let payload: UpdateTxtPayload | null = null;
      if (typeof error === "object" && error && "response" in error) {
        const response = (error as { response?: { data?: UpdateTxtPayload } }).response;
        payload = response?.data ?? null;
      }
      if (payload) {
        handleUpdateError(payload);
      } else {
        setToastMessage("TXT更新に失敗しました。");
      }
    } finally {
      setUpdateRequestInFlight(false);
    }
  }, [isUpdatingTxt, backendReady, fetchTxtUpdateStatus, handleUpdateError]);

  useEffect(() => {
    if (!backendReady) return;
    fetchTxtUpdateStatus();
  }, [backendReady, fetchTxtUpdateStatus]);

  useEffect(() => {
    if (!backendReady) return;
    if (!txtUpdateStatus?.running) return;
    const timer = window.setInterval(() => {
      fetchTxtUpdateStatus();
    }, 1000);
    return () => window.clearInterval(timer);
  }, [backendReady, txtUpdateStatus?.running, fetchTxtUpdateStatus]);

  useEffect(() => {
    const wasRunning = prevUpdateRunningRef.current;
    const isRunning = Boolean(txtUpdateStatus?.running);
    if (wasRunning && !isRunning) {
      if (txtUpdateStatus?.phase === "done") {
        const summary = txtUpdateStatus.summary;
        resetBarsCache();
        loadList();
        const hasWarning = Boolean(txtUpdateStatus.warning);
        setToastMessage(
          formatUpdateToast(
            hasWarning ? "TXT更新が完了しました（警告あり）。" : "TXT更新が完了しました。",
            summary
          )
        );
        if (hasWarning) {
          setShowUpdateLog(true);
        }
        fetchSplitSuspects().then((items) => {
          if (items.length) {
            setShowSplitSuspects(true);
            setToastMessage(`分割疑い ${items.length}件。TXT削除→再更新してください。`);
          }
        });
        api
          .get("/health")
          .then((res) => setHealth(res.data as HealthStatus))
          .catch(() => undefined);
      } else if (txtUpdateStatus?.phase === "error") {
        setToastMessage("TXT更新に失敗しました。");
        setShowUpdateLog(true);
      }
    }
    prevUpdateRunningRef.current = isRunning;
  }, [txtUpdateStatus, resetBarsCache, loadList, formatUpdateToast, fetchSplitSuspects]);

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="top-bar-row top-bar-row-nav">
          <div className="app-brand">
            <div className="title">MeeMee Screener</div>
            <div className="subtitle">Mee Mee - Fast grid with canvas sparklines</div>
          </div>
          <TopNav />
        </div>
        <div className="top-bar-row top-bar-row-tools">
          <div className="toolbar-left">
            <div className="segmented timeframe-segment">
              {(["monthly", "weekly", "daily"] as const).map((value) => (
                <button
                  key={value}
                  className={gridTimeframe === value ? "active" : ""}
                  onClick={() => setGridTimeframe(value)}
                >
                  {value === "monthly" ? "月足" : value === "weekly" ? "週足" : "日足"}
                </button>
              ))}
            </div>
            <div className="search-area">
              <div className="search-field">
                <input
                  className="search-input"
                  placeholder="コード / 銘柄名で検索"
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                />
                {search && (
                  <button type="button" className="search-clear" onClick={() => setSearch("")}>
                    クリア
                  </button>
                )}
              </div>
              {canAddWatchlist && (
                <button
                  type="button"
                  className="search-add-row"
                  onClick={() => handleAddWatchlist(canAddWatchlist)}
                >
                  “{canAddWatchlist}” は未登録です → ウォッチリストに追加
                </button>
              )}
            </div>
          </div>
          <div className="toolbar-right">
            <button
              type="button"
              className="consult-trigger"
              onClick={() => {
                setConsultVisible(true);
                setConsultExpanded(false);
              }}
            >
              相談
            </button>
            <div className="popover-anchor" ref={sortRef}>
              <button
                type="button"
                className={`sort-button ${isSorting ? "is-sorting" : ""}`}
                onClick={() => {
                  setSortOpen((prev) => !prev);
                  setDisplayOpen(false);
                }}
              >
                並び替え：{sortLabel}・{sortDirLabel}
                <span className="caret">▼</span>
              </button>
              {sortOpen && (
                <div className="popover-panel">
                  <div className="popover-section">
                    <div className="popover-title">並び替え項目</div>
                    <div className="popover-groups">
                      {sortSections.map((section) => (
                        <div className="popover-group" key={section.title}>
                          <div className="popover-group-title">{section.title}</div>
                          <div className="popover-group-list">
                            {section.options.map((option) => (
                              <button
                                type="button"
                                key={option.key}
                                className={
                                  sortKey === option.key ? "popover-item active" : "popover-item"
                                }
                                onClick={() => {
                                  setSortKey(option.key);
                                  setSortOpen(false);
                                }}
                              >
                                <span>{option.label}</span>
                                {sortKey === option.key && (
                                  <span className="popover-status">選択中</span>
                                )}
                              </button>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="popover-section">
                    <div className="popover-title">順序</div>
                    <div className="segmented">
                      {(["desc", "asc"] as SortDir[]).map((dir) => (
                        <button
                          key={dir}
                          className={sortDir === dir ? "active" : ""}
                          onClick={() => setSortDir(dir)}
                        >
                          {dir === "desc" ? "降順" : "昇順"}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
            <div className="popover-anchor" ref={displayRef}>
              <button
                type="button"
                className="display-button"
                onClick={() => {
                  setDisplayOpen((prev) => !prev);
                  setSortOpen(false);
                }}
              >
                表示
                <span className="caret">▼</span>
              </button>
              {displayOpen && (
                <div className="popover-panel">
                  <div className="popover-section">
                    <div className="popover-title">列数</div>
                    <div className="segmented">
                      {[1, 2, 3, 4].map((count) => (
                        <button
                          key={count}
                          className={columns === count ? "active" : ""}
                          onClick={() => setColumns(count as 1 | 2 | 3 | 4)}
                        >
                          {count}列
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="popover-section">
                    <div className="popover-title">行数</div>
                    <div className="segmented">
                      {[1, 2, 3, 4, 5, 6].map((count) => (
                        <button
                          key={count}
                          className={rows === count ? "active" : ""}
                          onClick={() => setRows(count as 1 | 2 | 3 | 4 | 5 | 6)}
                        >
                          {count}行
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="popover-section">
                    <div className="popover-title">チャート表示</div>
                    <div className="popover-list">
                      <button
                        type="button"
                        className={showBoxes ? "popover-item active" : "popover-item"}
                        onClick={() => setShowBoxes(!showBoxes)}
                      >
                        <span>ボックス表示</span>
                        <span className="popover-status">{showBoxes ? "オン" : "オフ"}</span>
                      </button>
                      <button
                        type="button"
                        className="popover-item"
                        onClick={() => {
                          setShowIndicators(true);
                          setDisplayOpen(false);
                        }}
                      >
                        <span>移動平均設定</span>
                        <span className="popover-status">開く</span>
                      </button>
                    </div>
                  </div>
                  <div className="popover-section">
                    <button type="button" className="popover-reset" onClick={resetDisplay}>
                      おすすめに戻す
                    </button>
                  </div>
                </div>
              )}
            </div>
            <div className="txt-update-group">
              <button
                type="button"
                className={`txt-update-button ${isUpdatingTxt ? "is-updating" : ""}`}
                onClick={handleUpdateTxt}
                disabled={!backendReady || isUpdatingTxt}
              >
                {isUpdatingTxt ? "TXT更新中..." : "TXT更新"}
              </button>
              {(updateProgressLabel || lastUpdatedLabel) && (
                <div className="txt-update-meta">
                  <span>{updateProgressLabel ?? "更新待ち"}</span>
                  <span>最終更新：{lastUpdatedLabel ?? "--"}</span>
                </div>
              )}
            </div>
            <div className="popover-anchor" ref={settingsRef}>
              <button
                type="button"
                className="icon-button settings-trigger"
                onClick={() => {
                  setSettingsOpen(!settingsOpen);
                  setSortOpen(false);
                  setDisplayOpen(false);
                }}
                title="設定"
                style={{ padding: "6px", display: "flex", alignItems: "center", justifyContent: "center", minHeight: "34px", width: "34px" }}
              >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.72l-.15.1a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.38a2 2 0 0 0-.73-2.73l-.15-.1a2 2 0 0 1-1-1.72v-.51a2 2 0 0 1 1-1.72l.15-.1a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
                  <circle cx="12" cy="12" r="3" />
                </svg>
              </button>
              {settingsOpen && (
                <div className="popover-panel popover-right-aligned" style={{ right: 0 }}>
                  <div className="popover-section">
                    <div className="popover-title">外観設定</div>
                    <div className="segmented">
                      <button
                        className={currentTheme === "dark" ? "active" : ""}
                        onClick={() => currentTheme !== "dark" && handleThemeToggle()}
                      >
                        ダーク
                      </button>
                      <button
                        className={currentTheme === "light" ? "active" : ""}
                        onClick={() => currentTheme !== "light" && handleThemeToggle()}
                      >
                        ライト
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </header>
      {health && health.txt_count === 0 && (
        <div className="data-warning">
          TXTが見つかりません。PANROLLINGで出力したTXTを `data/txt` に配置してください。
        </div>
      )}
      {health && health.code_txt_missing && health.txt_count > 0 && (
        <div className="data-warning subtle">
          code.txt がありません。ファイル名から銘柄コードを推定しています（code.txt推奨）。
        </div>
      )}
      {showSplitSuspects && splitSuspects.length > 0 && (
        <div className="split-suspects-panel">
          <div className="split-suspects-header">
            <div className="split-suspects-title">分割疑い {splitSuspects.length}件</div>
            <button type="button" onClick={() => setShowSplitSuspects(false)}>
              閉じる
            </button>
          </div>
          <div className="split-suspects-body">
            <div className="split-suspects-note">
              該当銘柄のTXTを削除してから再更新してください。
            </div>
            <div className="split-suspects-list">
              {splitSuspects.slice(0, 50).map((item) => (
                <div key={`${item.code}-${item.file_date}`} className="split-suspects-row">
                  <span className="split-suspects-code">{item.code}</span>
                  <span className="split-suspects-date">{item.file_date ?? "--"}</span>
                  <span className="split-suspects-diff">
                    差異 {item.diff_ratio ?? "--"}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
      {showUpdateLog && (
        <div className="update-log-panel">
          <div className="update-log-header">
            <div className="update-log-title">TXT更新ログ（末尾）</div>
            <button type="button" onClick={() => setShowUpdateLog(false)}>
              閉じる
            </button>
          </div>
          {txtUpdateStatus?.error && (
            <div className="update-log-error">
              原因: {txtUpdateStatus.error}
              {txtUpdateStatus.error === "timeout" && txtUpdateStatus.timeout_sec
                ? `（${txtUpdateStatus.timeout_sec}s）`
                : ""}
            </div>
          )}
          <pre className="update-log-body">
            {updateLogLines.length ? updateLogLines.join("\n") : "ログはまだありません。"}
          </pre>
        </div>
      )}
      <div className="keep-bar">
        <div className="keep-bar-header">
          <div className="keep-bar-title">候補箱</div>
          <div className="keep-bar-meta">
            {keepList.length}/{KEEP_LIMIT}
          </div>
          <div className="keep-bar-hint">S:候補 / E:除外 / J,K:上下 / ←→:横</div>
          {keepList.length > 0 && (
            <button type="button" className="keep-bar-clear" onClick={clearKeep}>
              クリア
            </button>
          )}
        </div>
        {keepList.length > 0 ? (
          <div className="keep-bar-chips">
            {keepList.map((code) => (
              <div className="keep-chip" key={code}>
                <button
                  type="button"
                  className="keep-chip-main"
                  onClick={() => handleKeepNavigate(code)}
                >
                  {code}
                </button>
                <button
                  type="button"
                  className="keep-chip-remove"
                  onClick={() => removeKeep(code)}
                  aria-label={`${code} を候補箱から外す`}
                >
                  x
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div className="keep-bar-empty">Sキーまたは + で候補に追加</div>
        )}
      </div>
      <div className={`grid-shell ${consultPaddingClass}`} ref={ref}>
        {showSkeleton && (
          <div className="grid-skeleton">
            {Array.from({ length: 8 }).map((_, index) => (
              <div className="tile skeleton-card" key={`skeleton-${index}`}>
                <div className="skeleton-line wide" />
                <div className="skeleton-line" />
                <div className="skeleton-block" />
              </div>
            ))}
          </div>
        )}
        {!showSkeleton && size.width > 0 && (
          <div className="grid-inner">
            <Grid
              key={`${gridTimeframe}-${currentTheme}`}
              ref={gridRef}
              columnCount={columns}
              columnWidth={columnWidth}
              height={innerHeight}
              rowCount={rowCount}
              rowHeight={rowHeight}
              width={gridWidth}
              overscanRowCount={2}
              itemData={sortedTickers}
              itemKey={itemKey}
              onItemsRendered={onItemsRendered}
              initialScrollTop={gridScrollTop}
              onScroll={({ scrollTop }) => setGridScrollTop(scrollTop)}
            >
              {({ columnIndex, rowIndex, style, data }) => {
                const index = rowIndex * columns + columnIndex;
                const item = data[index];
                if (!item) return null;
                const cellStyle = {
                  ...style,
                  padding: GRID_GAP / 2,
                  boxSizing: "border-box"
                };
                return (
                  <div style={cellStyle}>
                    <StockTile
                      ticker={item.ticker}
                      timeframe={gridTimeframe}
                      signals={item.metrics?.signals ?? []}
                      active={activeCode === item.ticker.code}
                      kept={keepSet.has(item.ticker.code)}
                      onActivate={activateByCode}
                      onOpenDetail={handleOpenDetail}
                      onToggleKeep={handleToggleKeep}
                      onExclude={handleExclude}
                      theme={currentTheme}
                    />
                  </div>
                );
              }}
            </Grid>
          </div>
        )}
      </div>
      {undoInfo && (
        <div
          className={`undo-bar ${consultVisible ? (consultExpanded ? "offset-expanded" : "offset-mini") : ""
            }`}
        >
          <span>{undoInfo.code} を除外しました</span>
          <button type="button" onClick={handleUndoRemove}>
            元に戻す
          </button>
        </div>
      )}
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
              <div className="consult-mini-count">候補 {keepList.length}件</div>
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
                disabled={!keepList.length || consultBusy}
              >
                {consultBusy ? "作成中..." : "相談作成"}
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
                  disabled={!keepList.length || consultBusy}
                >
                  {consultBusy ? "作成中..." : "相談作成"}
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
                  候補 {keepList.length}件
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
      {showIndicators && (
        <div className="indicator-overlay" onClick={() => setShowIndicators(false)}>
          <div className="indicator-panel" onClick={(event) => event.stopPropagation()}>
            <div className="indicator-header">
              <div className="indicator-title">Indicators</div>
              <button className="indicator-close" onClick={() => setShowIndicators(false)}>
                Close
              </button>
            </div>
            {(["daily", "weekly", "monthly"] as Timeframe[]).map((frame) => (
              <div className="indicator-section" key={frame}>
                <div className="indicator-subtitle">Moving Averages ({frame})</div>
                <div className="indicator-rows">
                  {maSettings[frame].map((setting, index) => (
                    <div className="indicator-row" key={setting.key}>
                      <input
                        type="checkbox"
                        checked={setting.visible}
                        onChange={() => updateSetting(frame, index, { visible: !setting.visible })}
                      />
                      <div className="indicator-label">{setting.label}</div>
                      <input
                        className="indicator-input"
                        type="number"
                        min={1}
                        value={setting.period}
                        onChange={(event) =>
                          updateSetting(frame, index, { period: Number(event.target.value) || 1 })
                        }
                      />
                      <input
                        className="indicator-input indicator-width"
                        type="number"
                        min={1}
                        max={6}
                        value={setting.lineWidth}
                        onChange={(event) =>
                          updateSetting(frame, index, { lineWidth: Number(event.target.value) })
                        }
                      />
                      <input
                        className="indicator-color-input"
                        type="color"
                        value={setting.color}
                        onChange={(event) => updateSetting(frame, index, { color: event.target.value })}
                      />
                    </div>
                  ))}
                </div>
                <button className="indicator-reset" onClick={() => resetSettings(frame)}>
                  Reset {frame}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
      <Toast message={toastMessage} onClose={() => setToastMessage(null)} />
    </div>
  );
}
