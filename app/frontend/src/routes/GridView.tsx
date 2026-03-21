// @ts-nocheck
import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import {
  FixedSizeGrid as Grid,
  type FixedSizeGrid,
  type GridOnItemsRenderedProps
} from "react-window";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../api";
import {
  useBackendReadyState,
  type HealthDeepResponse,
  type HealthReadyResponse
} from "../backendReady";
import type { MaSetting, SortDir } from "../store";
import { useStore } from "../store";
import StockTile from "../components/StockTile";
import TradexListSummary from "../components/TradexListSummary";
import Toast from "../components/Toast";
import TopNav from "../components/TopNav";
import IconButton from "../components/IconButton";
import {
  IconMessage,
  IconArrowsSort,
  IconLayoutGrid,
  IconFilter,
  IconRefresh,
  IconPlayerStop,
  IconSettings,
  IconMoon,
  IconSun,
  IconUpload,
  IconDownload,
  IconFileText,
  IconBuildingArch // Added IconBuildingArch for Sector Sort
} from "@tabler/icons-react";
import TechnicalFilterDrawer from "../components/TechnicalFilterDrawer";
import { computeSignalMetrics } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { applyTheme, getStoredTheme, setStoredTheme, toggleTheme, type Theme } from "../utils/theme";
import { saveAsFile } from "../utils/aiExport";
import {
  computeMAAt,
  evaluateBuilderCondition,
  formatDateYMD,
  getLatestAnchorTime,
  resolveAnchorInfo,
  resolveOperandValue,
  sanitizeTechnicalConditions,
  type AnchorInfo,
  type TechnicalFilterState
} from "../utils/technicalFilter";
import { formatEventDateYmd, parseEventDateMs } from "../utils/events";
import {
  extractTxtUpdateJobId,
  formatTxtUpdateStatusLabel,
  isTxtUpdateConflictError,
  type TxtUpdateStartPayload
} from "../utils/txtUpdate";
import { useResizeObserver } from "./grid/hooks/useResizeObserver";
import GridIndicatorOverlay from "./grid/components/GridIndicatorOverlay";
import { useTerminalJobPolling } from "./grid/hooks/useTerminalJobPolling";
import { buildTradexListSummaryKey } from "./list/tradexSummary";
import { TradexListSummaryMount } from "./list/TradexListSummaryMount";
import type {
  BuyStateFilter,
  HealthStatus,
  JobHistoryItem,
  JobStatusPayload,
  SortSection,
  Timeframe,
  ToastAction,
  TxtUpdateJobState,
  WalkforwardAttributionBucket,
  WalkforwardLatest,
  WalkforwardParams,
  WalkforwardPreset,
  WalkforwardResearchLatest,
} from "./grid/gridTypes";
import {
  ACTIVE_JOB_STATUS,
  APP_VERSION_LABEL,
  BARS_ERROR_RETRY_COOLDOWN_MS,
  BARS_ERROR_RETRY_INTERVAL_MS,
  GRID_GAP,
  GRID_REFACTOR_ENABLED,
  KP_LIMIT,
  TERMINAL_JOB_STATUS,
  WALKFORWARD_PRESETS_LIMIT,
  WALKFORWARD_PRESETS_STORAGE_KEY,
  createDefaultTechFilter,
  createDefaultWalkforwardParams,
  extractErrorDetail,
  gridPresetOptions,
  mergeHealthStatus,
  normalizeHealthStatus,
  resolveGridRangeBars,
  resolveGridVolumeSurgeRatio,
  buildAvailableSectorOptions,
  resolveGridSignalSortScore,
  toWalkforwardParams
} from "./grid/gridHelpers";

export default function GridView() {
  const location = useLocation();
  const navigate = useNavigate();
  const sectorParam = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const value = params.get("sector");
    return value && value.trim() ? value.trim() : null;
  }, [location.search]);
  const { ref, size } = useResizeObserver();
  const { ready: backendReady } = useBackendReadyState();
  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const loadingList = useStore((state) => state.loadingList);
  const listLoadError = useStore((state) => state.listLoadError);
  const listSnapshotMeta = useStore((state) => state.listSnapshotMeta);
  const loadFavorites = useStore((state) => state.loadFavorites);
  const favoritesLoaded = useStore((state) => state.favoritesLoaded);
  const resetBarsCache = useStore((state) => state.resetBarsCache);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const boxesCache = useStore((state) => state.boxesCache);
  const columns = useStore((state) => state.settings.columns);
  const rows = useStore((state) => state.settings.rows);
  const search = useStore((state) => state.settings.search);
  const gridScrollTop = useStore((state) => state.settings.gridScrollTop);
  const gridTimeframe = useStore((state) => state.settings.gridTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const keepList = useStore((state) => state.keepList);
  const addKeep = useStore((state) => state.addKeep);
  const removeKeep = useStore((state) => state.removeKeep);
  const setRows = useStore((state) => state.setRows);
  const setSearch = useStore((state) => state.setSearch);
  const setGridScrollTop = useStore((state) => state.setGridScrollTop);
  const setGridTimeframe = useStore((state) => state.setGridTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const showBoxes = useStore((state) => state.settings.showBoxes);
  const setShowBoxes = useStore((state) => state.setShowBoxes);
  const sortKey = useStore((state) => state.settings.sortKey);
  const sortDir = useStore((state) => state.settings.sortDir);
  const setSortKey = useStore((state) => state.setSortKey);
  const setSortDir = useStore((state) => state.setSortDir);
  const performancePeriod = useStore((state) => state.settings.performancePeriod);
  const maSettings = useStore((state) => state.maSettings);
  const updateMaSetting = useStore((state) => state.updateMaSetting);
  const resetMaSettings = useStore((state) => state.resetMaSettings);
  const eventsRefreshing = useStore((state) => state.eventsMeta?.isRefreshing ?? false);
  const eventsLastError = useStore((state) => state.eventsMeta?.lastError ?? null);
  const eventsLastAttemptAt = useStore((state) => state.eventsMeta?.lastAttemptAt ?? null);
  const eventsEarningsLastSuccessAt = useStore((state) => state.eventsMeta?.earningsLastSuccessAt ?? null);
  const eventsRightsLastSuccessAt = useStore((state) => state.eventsMeta?.rightsLastSuccessAt ?? null);
  const eventsRightsMaxDate = useStore(
    (state) => state.eventsMeta?.dataCoverage?.rightsMaxDate ?? null
  );
  const refreshEvents = useStore((state) => state.refreshEvents);

  const eventsAttemptLabel = useMemo(
    () => formatEventDateYmd(eventsLastAttemptAt),
    [eventsLastAttemptAt]
  );
  const eventsLastSuccessLabel = useMemo(() => {
    const earningsMs = parseEventDateMs(eventsEarningsLastSuccessAt);
    const rightsMs = parseEventDateMs(eventsRightsLastSuccessAt);
    const candidates = [
      { value: eventsEarningsLastSuccessAt, ms: earningsMs },
      { value: eventsRightsLastSuccessAt, ms: rightsMs }
    ].filter((item) => item.value && item.ms != null) as { value: string; ms: number }[];
    if (!candidates.length) return null;
    const oldest = candidates.reduce((prev, next) => (next.ms < prev.ms ? next : prev));
    return formatEventDateYmd(oldest.value);
  }, [eventsEarningsLastSuccessAt, eventsRightsLastSuccessAt]);
  const rightsCoverageLabel = useMemo(() => {
    const maxMs = parseEventDateMs(eventsRightsMaxDate);
    if (!eventsRightsMaxDate || maxMs == null) return null;
    const thresholdMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
    if (maxMs >= thresholdMs) return null;
    const formatted = formatEventDateYmd(eventsRightsMaxDate);
    return formatted ? `権利データ範囲: ～${formatted}` : null;
  }, [eventsRightsMaxDate]);

  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [showIndicators, setShowIndicators] = useState(false);
  const [sortOpen, setSortOpen] = useState(false);  // Candidate sort menu
  const [displayOpen, setDisplayOpen] = useState(false);
  const [toastMessage, setToastMessage] = useState<{ text: string; key: number } | null>(null);
  const [toastAction, setToastAction] = useState<ToastAction | null>(null);
  const toastKeyRef = useRef(0);
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
  const [settingsPanelMode, setSettingsPanelMode] = useState<"general" | "walkforward">("general");
  const [settingsDetailsOpen, setSettingsDetailsOpen] = useState(false);
  const [dataDir, setDataDir] = useState("");
  const [dataDirInput, setDataDirInput] = useState("");
  const [dataDirLoading, setDataDirLoading] = useState(false);
  const [dataDirSaving, setDataDirSaving] = useState(false);
  const [dataDirMessage, setDataDirMessage] = useState<string | null>(null);
  const [currentTheme, setCurrentTheme] = useState<Theme>(() => getStoredTheme());
  const [tradeUploadInFlight, setTradeUploadInFlight] = useState(false);
  const [tradeSyncInFlight, setTradeSyncInFlight] = useState(false);
  const [analysisBatchSubmitting, setAnalysisBatchSubmitting] = useState(false);
  const [txtUpdateJob, setTxtUpdateJob] = useState<TxtUpdateJobState | null>(null);
  const [txtUpdatePolling, setTxtUpdatePolling] = useState(false);
  const [walkforwardSubmitting, setWalkforwardSubmitting] = useState(false);
  const [walkforwardLoading, setWalkforwardLoading] = useState(false);
  const [walkforwardLatest, setWalkforwardLatest] = useState<WalkforwardLatest | null>(null);
  const [walkforwardResearchLatest, setWalkforwardResearchLatest] =
    useState<WalkforwardResearchLatest | null>(null);
  const [walkforwardParams, setWalkforwardParams] = useState<WalkforwardParams>(
    createDefaultWalkforwardParams
  );
  const [walkforwardPresetName, setWalkforwardPresetName] = useState("");
  const [walkforwardPresets, setWalkforwardPresets] = useState<WalkforwardPreset[]>([]);
  const [walkforwardPresetImporting, setWalkforwardPresetImporting] = useState(false);
  const [watchlistExporting, setWatchlistExporting] = useState(false);
  const [techFilterOpen, setTechFilterOpen] = useState(false);
  const [techFilterDraft, setTechFilterDraft] = useState<TechnicalFilterState>(() =>
    createDefaultTechFilter(gridTimeframe)
  );
  const [techFilterActive, setTechFilterActive] = useState<TechnicalFilterState>(() =>
    createDefaultTechFilter(gridTimeframe)
  );
  const [buyStateFilter, setBuyStateFilter] = useState<BuyStateFilter>("all");
  const [buyStateFilterDraft, setBuyStateFilterDraft] = useState<BuyStateFilter>("all");
  const [shortTierAbOnly, setShortTierAbOnly] = useState(false);
  const [shortTierAbOnlyDraft, setShortTierAbOnlyDraft] = useState(false);
  const [sectorSortOpen, setSectorSortOpen] = useState(false); // Popover state for Sector Sort
  const sortRef = useRef<HTMLDivElement | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const settingsRef = useRef<HTMLDivElement | null>(null);
  const sectorSortRef = useRef<HTMLDivElement | null>(null); // Ref for Sector Sort Popover
  const techFilterDropNoticeRef = useRef(false);
  const gridRef = useRef<FixedSizeGrid | null>(null);
  const tradeCsvInputRef = useRef<HTMLInputElement | null>(null);
  const walkforwardPresetImportInputRef = useRef<HTMLInputElement | null>(null);
  const lastVisibleCodesRef = useRef<string[]>([]);
  const lastVisibleRangeRef = useRef<{ start: number; stop: number } | null>(null);
  const lastVisibleSummarySignatureRef = useRef<string>("");
  const lastVisibleRequestKeyRef = useRef<string | null>(null);
  const deferredVisibleRequestTimerRef = useRef<number | null>(null);
  const barsErrorRetryCooldownRef = useRef<Record<string, number>>({});
  const undoTimerRef = useRef<number | null>(null);
  const txtUpdateTerminalStatusRef = useRef<string | null>(null);
  const txtUpdateDailyFollowupRef = useRef(false);
  const seenTerminalJobsRef = useRef<Set<string>>(new Set());
  const terminalJobsInitializedRef = useRef(false);
  const walkforwardPresetsLoadedRef = useRef(false);
  const [tradexVisibleCodes, setTradexVisibleCodes] = useState<string[]>([]);
  const tradexListSummaryItems = useMemo(
    () => tradexVisibleCodes.map((code) => ({ code, asof: null })),
    [tradexVisibleCodes]
  );


  const showToast = useCallback((text: string, action?: ToastAction | null) => {
    toastKeyRef.current += 1;
    setToastAction(action ?? null);
    setToastMessage({ text, key: toastKeyRef.current });
  }, []);

  const clearSectorFilter = useCallback(() => {
    const params = new URLSearchParams(location.search);
    params.delete("sector");
    const next = params.toString();
    navigate(`${location.pathname}${next ? `?${next}` : ""}`);
  }, [location.pathname, location.search, navigate]);
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";

  const gridMaxBars = useMemo(() => {
    const count = listRangeBars ?? 120;
    return Math.max(12, Math.min(260, Math.floor(count)));
  }, [listRangeBars]);

  const listRangeBarsRef = useRef(listRangeBars);
  useEffect(() => {
    listRangeBarsRef.current = listRangeBars;
  }, [listRangeBars]);

  useEffect(() => {
    const resolvedBars = resolveGridRangeBars(rows, columns, listRangeBarsRef.current ?? 60);
    if (resolvedBars !== listRangeBarsRef.current) {
      setListRangeBars(resolvedBars);
    }
  }, [columns, rows, setListRangeBars]);

  const formatRate = useCallback((value: number | null | undefined) => {
    if (typeof value !== "number" || Number.isNaN(value)) return "--";
    return `${(value * 100).toFixed(1)}%`;
  }, []);

  const formatSigned = useCallback((value: number | null | undefined) => {
    if (typeof value !== "number" || Number.isNaN(value)) return "--";
    const sign = value > 0 ? "+" : "";
    return `${sign}${value.toFixed(3)}`;
  }, []);

  const formatCompactDate = useCallback((value: number | null | undefined) => {
    if (typeof value !== "number" || Number.isNaN(value)) return "--";
    const text = String(Math.trunc(value));
    if (!/^\d{8}$/.test(text)) return text;
    return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  }, []);

  const formatAttributionRows = useCallback(
    (bucket: WalkforwardAttributionBucket | undefined, limit = 3) => {
      const top = Array.isArray(bucket?.top) ? bucket?.top?.slice(0, limit) : [];
      const bottom = Array.isArray(bucket?.bottom) ? bucket?.bottom?.slice(0, limit) : [];
      return { top, bottom };
    },
    []
  );

  const parseOptionalNumber = useCallback((value: string) => {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    const num = Number(trimmed);
    if (!Number.isFinite(num)) return undefined;
    return num;
  }, []);

  const normalizeWalkforwardPresetName = useCallback((value: string) => {
    return value.trim().replace(/\s+/g, " ").slice(0, 40);
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(WALKFORWARD_PRESETS_STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      const items: WalkforwardPreset[] = [];
      for (const entry of parsed) {
        if (!entry || typeof entry !== "object") continue;
        const nameRaw = (entry as { name?: unknown }).name;
        const name = typeof nameRaw === "string" ? normalizeWalkforwardPresetName(nameRaw) : "";
        if (!name) continue;
        const paramsRaw = (entry as { params?: unknown }).params;
        const createdAtRaw = (entry as { createdAt?: unknown }).createdAt;
        const updatedAtRaw = (entry as { updatedAt?: unknown }).updatedAt;
        const nowIso = new Date().toISOString();
        items.push({
          name,
          params: toWalkforwardParams(paramsRaw),
          createdAt: typeof createdAtRaw === "string" && createdAtRaw ? createdAtRaw : nowIso,
          updatedAt: typeof updatedAtRaw === "string" && updatedAtRaw ? updatedAtRaw : nowIso,
        });
      }
      items.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
      setWalkforwardPresets(items.slice(0, WALKFORWARD_PRESETS_LIMIT));
    } catch {
      // Ignore corrupted local storage.
    } finally {
      walkforwardPresetsLoadedRef.current = true;
    }
  }, [normalizeWalkforwardPresetName]);

  useEffect(() => {
    if (!walkforwardPresetsLoadedRef.current) return;
    try {
      window.localStorage.setItem(
        WALKFORWARD_PRESETS_STORAGE_KEY,
        JSON.stringify(walkforwardPresets)
      );
    } catch {
      // Ignore storage errors.
    }
  }, [walkforwardPresets]);

  const handleTradeCsvPick = () => {
    tradeCsvInputRef.current?.click();
  };

  const handleTradeCsvChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file || tradeUploadInFlight) return;
    setTradeUploadInFlight(true);
    try {
      const form = new FormData();
      form.append("file", file);
      await api.post("/trade_csv/upload", form, {
        headers: { "Content-Type": "multipart/form-data" },
        timeout: 120000
      });
      showToast("トレードCSVをアップロードしました。");
    } catch (err: unknown) {
      const detail = extractErrorDetail(err);
      showToast(`トレードCSVのアップロードに失敗しました。(${detail})`);
    } finally {
      setTradeUploadInFlight(false);
      event.target.value = "";
    }
  };

  const handleForceTradeSync = async () => {
    if (tradeSyncInFlight) return;
    setTradeSyncInFlight(true);
    try {
      const res = await api.post("/jobs/force-sync");
      if (res.data?.ok) {
        showToast("強制同期（全件取込）を開始しました。");
      } else {
        const error = res.data?.error ?? "不明なエラー";
        showToast(`強制同期でエラーが発生しました。(${error})`);
      }
    } catch (err: unknown) {
      const detail = extractErrorDetail(err);
      showToast(`強制同期に失敗しました。(${detail})`);
    } finally {
      setTradeSyncInFlight(false);
    }
  };

  const handleExportWatchlist = async () => {
    if (watchlistExporting) return;
    const exportItems = sortedTickers.map((item) => item.ticker);
    if (!exportItems.length) {
      showToast("書き出す銘柄がありません。");
      return;
    }
    setWatchlistExporting(true);
    try {
      const lines = exportItems.map((item) => `JP#${item.code}`);
      const filename = "watchlist.ebk";
      const ok = await saveAsFile(lines.join("\n"), filename, "text/plain");
      showToast(ok ? "watchlist.ebk を保存しました。" : "watchlist.ebk の保存に失敗しました。");
    } catch {
      showToast("watchlist.ebk の保存に失敗しました。");
    } finally {
      setWatchlistExporting(false);
    }
  };

  const handleOpenCodeTxt = async () => {
    try {
      const res = await api.post("/watchlist/open");
      if (res.status >= 200 && res.status < 300 && res.data?.ok) {
        showToast("code.txt を開きました。");
      } else {
        showToast("code.txt を開けませんでした。");
      }
    } catch {
      showToast("code.txt を開けませんでした。");
    }
  };

  const handleDataDirSave = async () => {
    if (!dataDirInput.trim()) {
      setDataDirMessage("パスを入力してください。");
      return;
    }
    setDataDirSaving(true);
    setDataDirMessage(null);
    try {
      const res = await api.post("/system/data-dir", {
        dataDir: dataDirInput.trim()
      });
      const next = res.data?.dataDir;
      if (next) {
        setDataDir(next);
        setDataDirInput(next);
      }
      setDataDirMessage("データ保存先を更新しました。");
      showToast("データ保存先を更新しました。");
    } catch (err: unknown) {
      const detail = extractErrorDetail(err);
      setDataDirMessage(`保存に失敗しました: ${detail}`);
      showToast("データ保存先の更新に失敗しました。");
    } finally {
      setDataDirSaving(false);
    }
  };

  useEffect(() => {
    if (!backendReady) return;
    void ensureListLoaded();
  }, [backendReady, ensureListLoaded]);

  useEffect(() => {
    if (!backendReady || favoritesLoaded) return;
    void loadFavorites();
  }, [backendReady, favoritesLoaded, loadFavorites]);

  const availableSectors = useMemo(() => {
    return buildAvailableSectorOptions(tickers);
  }, [tickers]);
  const hasMeaningfulSectorOptions = availableSectors.length >= 2;
  const activeSectorParam = hasMeaningfulSectorOptions ? sectorParam : null;

  const handleSectorSelect = useCallback((code: string | null) => {
    const params = new URLSearchParams(location.search);
    if (code) {
      params.set("sector", code);
    } else {
      params.delete("sector");
    }
    navigate({ search: params.toString() });
    setSectorSortOpen(false);
  }, [location.search, navigate]);

  useEffect(() => {
    if (hasMeaningfulSectorOptions) return;
    if (sortKey !== "sector") return;
    setSortKey("code");
    setSortDir("asc");
  }, [hasMeaningfulSectorOptions, setSortDir, setSortKey, sortDir, sortKey]);

  useEffect(() => {
    if (!backendReady) return;
    let canceled = false;
    const loadHealth = async () => {
      try {
        const deepRes = await api.get("/health/deep", { validateStatus: () => true });
        if (canceled) return;
        if (deepRes.status >= 200 && deepRes.status < 300) {
          setHealth(normalizeHealthStatus(deepRes.data as HealthDeepResponse));
          return;
        }
      } catch {
        // fall through to lightweight health
      }
      try {
        const lightRes = await api.get("/health", { validateStatus: () => true });
        if (canceled) return;
        if (lightRes.status >= 200 && lightRes.status < 300) {
          const lightData = lightRes.data as HealthReadyResponse;
          setHealth((prev) => mergeHealthStatus(prev, lightData));
        }
      } catch {
        // keep previous health view on fetch error
      }
    };
    void loadHealth();
    return () => {
      canceled = true;
    };
  }, [backendReady]);

  useEffect(() => {
    if (!backendReady) return;
    setDataDirLoading(true);
    api
      .get("/system/data-dir")
      .then((res) => {
        const dir = res.data?.dataDir ?? "";
        if (dir) {
          setDataDir(dir);
          setDataDirInput(dir);
        }
      })
      .catch(() => undefined)
      .finally(() => setDataDirLoading(false));
  }, [backendReady]);

  useEffect(() => {
    if (!sortOpen && !displayOpen && !settingsOpen && !sectorSortOpen) return;
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (sortRef.current && sortRef.current.contains(target)) return;
      if (displayRef.current && displayRef.current.contains(target)) return;
      if (settingsRef.current && settingsRef.current.contains(target)) return;
      if (sectorSortRef.current && sectorSortRef.current.contains(target)) return;
      setSortOpen(false);
      setDisplayOpen(false);
      setSettingsOpen(false);
      setSectorSortOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [sortOpen, displayOpen, settingsOpen, sectorSortOpen]);

  useEffect(() => {
    return () => {
      if (undoTimerRef.current) {
        window.clearTimeout(undoTimerRef.current);
      }
    };
  }, []);

  const mainSortSections = useMemo<SortSection[]>(
    () => [
      {
        title: "基本",
        options: [
          { key: "code", label: "コード順", defaultDirection: "asc" },
          { key: "chg1D", label: "騰落順", defaultDirection: "desc" },
          { key: "volumeSurge", label: "出来高急増順", fixedDirection: "desc" }
        ]
      }
    ],
    []
  );

  const detailSortSections = useMemo<SortSection[]>(
    () => [
      {
        title: "詳細",
        options: [
          { key: "name", label: "銘柄名", defaultDirection: "asc" },
          { key: "buySignalLatest", label: "最新買い判定", fixedDirection: "desc" },
          { key: "sellSignalLatest", label: "最新売り判定", fixedDirection: "desc" },
          { key: "entryPriority", label: "仕込み優先度(A/B/C)" },
          { key: "buyCandidate", label: "買い候補(総合)" },
          { key: "swingScore", label: "スイング候補(総合)" },
          { key: "shortPriority", label: "売り精度優先(A/B/C)" },
          { key: "shortScore", label: "売り候補(総合)" },
          { key: "aScore", label: "売り候補(反転確実)" },
          { key: "bScore", label: "売り候補(戻り売り)" },
          { key: "ma20Dev", label: "乖離率(MA20)" },
          { key: "ma60Dev", label: "乖離率(MA60)" },
          { key: "ma20Slope", label: "MA20傾き" },
          { key: "ma60Slope", label: "MA60傾き" },
          { key: "performance", label: "騰落率(期間選択)", defaultDirection: "desc" },
          { key: "chg1W", label: "単純騰落(1W)", fixedDirection: "desc" },
          { key: "chg1M", label: "単純騰落(1M)", fixedDirection: "desc" },
          { key: "chg1Q", label: "単純騰落(1Q)", fixedDirection: "desc" },
          { key: "chg1Y", label: "単純騰落(1Y)", fixedDirection: "desc" },
          { key: "prevWeekChg", label: "前週比" },
          { key: "prevMonthChg", label: "前月比" },
          { key: "prevQuarterChg", label: "前四半期比" },
          { key: "prevYearChg", label: "前年差" },
          { key: "upScore", label: "上昇スコア" },
          { key: "downScore", label: "下落スコア" },
          { key: "overheatUp", label: "過熱(上)" },
          { key: "overheatDown", label: "過熱(下)" },
          { key: "mlEv20Net", label: "期待値(20D)" },
          { key: "mlPUpShort", label: "上昇確率(短期)" },
          { key: "mlPDownShort", label: "下落確率(短期)" },
          { key: "boxState", label: "ボックス状態" }
        ].concat(
          hasMeaningfulSectorOptions ? [{ key: "sector", label: "業種", defaultDirection: "asc" }] : []
        )
      }
    ],
    [hasMeaningfulSectorOptions]
  );

  const sortSections = useMemo<SortSection[]>(
    () => [...mainSortSections, ...detailSortSections],
    [mainSortSections, detailSortSections]
  );

  const sortOptions = useMemo(
    () => sortSections.flatMap((section) => section.options),
    [sortSections]
  );

  const visibleSortSections = useMemo<SortSection[]>(() => {
    const seenKeys = new Set<string>();
    const merged: SortSection[] = [];
    for (const section of sortSections) {
      const options = section.options.filter((option) => {
        if (seenKeys.has(option.key)) return false;
        seenKeys.add(option.key);
        return true;
      });
      if (!options.length) continue;
      const existing = merged.find((item) => item.title === section.title);
      if (existing) {
        existing.options.push(...options);
        continue;
      }
      merged.push({ ...section, options: [...options] });
    }
    return merged;
  }, [sortSections]);
  const [openSortSections, setOpenSortSections] = useState<string[]>([]);
  useEffect(() => {
    if (!sortOpen) return;
    if (!visibleSortSections.length) {
      setOpenSortSections([]);
      return;
    }
    const activeSectionTitle =
      visibleSortSections.find((section) => section.options.some((opt) => opt.key === sortKey))?.title ?? null;
    const next = new Set<string>();
    next.add(visibleSortSections[0]?.title ?? "");
    if (activeSectionTitle) next.add(activeSectionTitle);
    setOpenSortSections(Array.from(next).filter(Boolean));
  }, [sortOpen, visibleSortSections, sortKey]);

  const defaultSortLabel = "コード順";
  const sortLabel = useMemo(
    () => sortOptions.find((option) => option.key === sortKey)?.label ?? defaultSortLabel,
    [sortOptions, sortKey]
  );

  const gridPresetLabel = useMemo(() => {
    return gridPresetOptions.find((item) => item.value === rows)?.label ?? `${rows}x${columns}`;
  }, [rows, columns]);

  const sortDirLabel = sortDir === "desc" ? "降順" : "昇順";
  const txtUpdateCanCancel = Boolean(
    txtUpdateJob && txtUpdateJob.id && !TERMINAL_JOB_STATUS.has(txtUpdateJob.status)
  );
  const txtUpdateStatusLabel = useMemo(() => {
    if (!txtUpdateJob) return null;
    return formatTxtUpdateStatusLabel(txtUpdateJob.status);
  }, [txtUpdateJob]);
  const txtUpdateStatusTone = useMemo(() => {
    if (!txtUpdateJob) return "is-idle";
    if (txtUpdateJob.status === "success") return "is-done";
    if (txtUpdateJob.status === "failed" || txtUpdateJob.status === "canceled") return "is-error";
    return "is-running";
  }, [txtUpdateJob]);
  const txtUpdateProgressValue = useMemo(() => {
    const raw = txtUpdateJob?.progress;
    if (typeof raw !== "number" || !Number.isFinite(raw)) return null;
    return Math.max(0, Math.min(100, Math.round(raw)));
  }, [txtUpdateJob?.progress]);
  const txtUpdateStageLabel = useMemo(() => {
    const message = txtUpdateJob?.message?.toLowerCase() ?? "";
    if (!message) return txtUpdateStatusLabel ?? "待機中";
    if (message.includes("pan import") || message.includes("launching pan")) return "PAN取込";
    if (message.includes("pan rolling export") || message.includes("vbs export") || message.includes("export completed")) {
      return "TXT出力";
    }
    if (message.includes("ingesting")) return "TXT取込";
    if (message.includes("phase")) return "Phase更新";
    if (message.includes("ml")) return "ML更新";
    if (message.includes("score")) return "スコア更新";
    if (message.includes("cache")) return "キャッシュ更新";
    if (message.includes("walkforward")) return "検証";
    if (message.includes("final")) return "仕上げ";
    if (message.includes("queue")) return "待機";
    return txtUpdateStatusLabel ?? "更新中";
  }, [txtUpdateJob?.message, txtUpdateStatusLabel]);
  const txtUpdateShortDetail = useMemo(() => {
    const message = txtUpdateJob?.message?.trim();
    if (!message) return null;
    return message.length > 72 ? `${message.slice(0, 72)}...` : message;
  }, [txtUpdateJob?.message]);

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

  const normalizeMonthLabel = useCallback((value: string | null | undefined) => {
    if (!value) return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (/^\d{6}$/.test(trimmed)) {
      return `${trimmed.slice(0, 4)}-${trimmed.slice(4, 6)}`;
    }
    const match = trimmed.match(/^(\d{4})[/-](\d{1,2})/);
    if (match) {
      return `${match[1]}-${String(match[2]).padStart(2, "0")}`;
    }
    return trimmed;
  }, []);

  const normalizedSearch = useMemo(
    () => (search ? normalizeWatchCode(search) : null),
    [search, normalizeWatchCode]
  );

  const searchFiltered = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return tickers;
    return tickers.filter((item) => {
      return item.code.toLowerCase().includes(term) || item.name.toLowerCase().includes(term);
    });
  }, [tickers, search]);

  const sectorFiltered = useMemo(() => {
    if (!activeSectorParam) return searchFiltered;
    return searchFiltered.filter((item) => item.sector33Code === activeSectorParam);
  }, [activeSectorParam, searchFiltered]);

  const sectorLabel = useMemo(() => {
    if (!activeSectorParam) return null;
    const match = tickers.find(
      (item) => item.sector33Code === activeSectorParam && item.sector33Name
    );
    return match?.sector33Name ?? activeSectorParam;
  }, [activeSectorParam, tickers]);

  useEffect(() => {
    if (!backendReady) return;
    if (techFilterActive.conditions.length === 0) return;
    const codes = sectorFiltered.map((item) => item.code);
    if (!codes.length) return;
    const timeframes = Array.from(
      new Set(techFilterActive.conditions.map((condition) => condition.timeframe))
    );
    timeframes.forEach((frame) => {
      ensureBarsForVisible(frame, codes, "tech-filter");
    });
  }, [
    backendReady,
    techFilterActive.conditions.length,
    techFilterActive.conditions,
    sectorFiltered,
    ensureBarsForVisible
  ]);

  const resolveAnchorTime = useCallback(
    (state: TechnicalFilterState) => {
      const targetTimeframe = state.defaultTimeframe ?? gridTimeframe;
      if (state.anchorMode === "latest") {
        return getLatestAnchorTime(barsCache[targetTimeframe]);
      }
      if (!state.anchorDate) return null;
      const parts = state.anchorDate.split(/[-/]/).map((value) => Number(value));
      if (parts.length < 3) return null;
      const [year, month, day] = parts;
      if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
      return Math.floor(Date.UTC(year, month - 1, day) / 1000);
    },
    [barsCache, gridTimeframe]
  );

  const resolveAnchorTimeForTimeframe = useCallback(
    (timeframe: Timeframe, state: TechnicalFilterState) => {
      if (state.anchorMode === "latest") {
        return getLatestAnchorTime(barsCache[timeframe]);
      }
      if (!state.anchorDate) return null;
      const parts = state.anchorDate.split(/[-/]/).map((value) => Number(value));
      if (parts.length < 3) return null;
      const [year, month, day] = parts;
      if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
      return Math.floor(Date.UTC(year, month - 1, day) / 1000);
    },
    [barsCache]
  );

  const activeAnchorTime = useMemo(
    () => resolveAnchorTime(techFilterActive),
    [resolveAnchorTime, techFilterActive]
  );

  const draftAnchorTime = useMemo(
    () => resolveAnchorTime(techFilterDraft),
    [resolveAnchorTime, techFilterDraft]
  );

  const activeAnchorLabel = activeAnchorTime ? formatDateYMD(activeAnchorTime) : null;
  const draftAnchorLabel = draftAnchorTime ? formatDateYMD(draftAnchorTime) : null;
  const listAnchorTime = useMemo(
    () => getLatestAnchorTime(barsCache[gridTimeframe]),
    [barsCache, gridTimeframe]
  );
  const listAnchorLabel = listAnchorTime ? formatDateYMD(listAnchorTime) : null;
  const boxMonthLabel = useMemo(() => {
    const base = listAnchorTime ?? activeAnchorTime ?? Math.floor(Date.now() / 1000);
    const date = new Date(base * 1000);
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    return `${year}-${String(month).padStart(2, "0")}`;
  }, [listAnchorTime, activeAnchorTime]);
  const latestBoxMonthLabel = useMemo(() => {
    let latest: string | null = null;
    const consider = (value: string | null | undefined) => {
      const normalized = normalizeMonthLabel(value);
      if (!normalized) return;
      if (!latest || normalized > latest) {
        latest = normalized;
      }
    };
    sectorFiltered.forEach((ticker) => {
      consider(ticker.boxEndMonth);
      consider(ticker.breakoutMonth);
    });
    return latest;
  }, [sectorFiltered, normalizeMonthLabel]);

  const filterAsofTimeframe = useMemo(() => {
    if (techFilterActive.conditions.length === 0) return null;
    const unique = new Set(techFilterActive.conditions.map((condition) => condition.timeframe));
    if (unique.size === 1) return Array.from(unique)[0];
    return techFilterActive.defaultTimeframe;
  }, [techFilterActive.conditions, techFilterActive.defaultTimeframe]);

  const filterAnchorInfoByCode = useMemo(() => {
    const map = new Map<string, AnchorInfo>();
    if (activeAnchorTime == null || !filterAsofTimeframe) return map;
    sectorFiltered.forEach((ticker) => {
      const payload = barsCache[filterAsofTimeframe][ticker.code];
      if (!payload?.bars?.length) return;
      const anchor = resolveAnchorInfo(payload.bars, activeAnchorTime);
      if (anchor) map.set(ticker.code, anchor);
    });
    return map;
  }, [sectorFiltered, barsCache, filterAsofTimeframe, activeAnchorTime]);

  const listAnchorInfoByCode = useMemo(() => {
    const map = new Map<string, AnchorInfo>();
    if (listAnchorTime == null) return map;
    sectorFiltered.forEach((ticker) => {
      const payload = barsCache[gridTimeframe][ticker.code];
      if (!payload?.bars?.length) return;
      const anchor = resolveAnchorInfo(payload.bars, listAnchorTime);
      if (anchor) map.set(ticker.code, anchor);
    });
    return map;
  }, [sectorFiltered, barsCache, gridTimeframe, listAnchorTime]);

  const buildFilterResult = useCallback(
    (state: TechnicalFilterState) => {
      const { conditions } = state;
      const applyBoxFilter = (items: typeof sectorFiltered) => {
        if (!state.boxThisMonth) return items;
        const targetMonth = latestBoxMonthLabel ?? boxMonthLabel;
        if (!targetMonth) return [];
        return items.filter((ticker) => {
          const end = normalizeMonthLabel(ticker.boxEndMonth);
          const breakout = normalizeMonthLabel(ticker.breakoutMonth);
          if (end === targetMonth || breakout === targetMonth) return true;
          const stateLabel = ticker.boxState ?? "NONE";
          if (stateLabel !== "NONE") return true;
          if (ticker.boxActive === true || ticker.hasBox === true) return true;
          return false;
        });
      };
      if (!conditions.length) {
        return {
          items: applyBoxFilter(sectorFiltered),
          asofMap: new Map<string, string>()
        };
      }
      const asofMap = new Map<string, string>();
      const timeframes = Array.from(
        new Set(conditions.map((condition) => condition.timeframe))
      );
      const anchorTimes = new Map<Timeframe, number | null>();
      timeframes.forEach((timeframe) => {
        anchorTimes.set(timeframe, resolveAnchorTimeForTimeframe(timeframe, state));
      });
      const items = sectorFiltered.filter((ticker) => {
        const anchorCache = new Map<Timeframe, AnchorInfo | null>();
        for (const condition of conditions) {
          const timeframe = condition.timeframe;
          const anchorTime = anchorTimes.get(timeframe) ?? null;
          if (anchorTime == null) return false;
          const payload = barsCache[timeframe][ticker.code];
          if (!payload?.bars?.length) return false;
          let anchor = anchorCache.get(timeframe);
          if (anchor === undefined) {
            anchor = resolveAnchorInfo(payload.bars, anchorTime);
            anchorCache.set(timeframe, anchor ?? null);
          }
          if (!anchor) return false;
          if (!evaluateBuilderCondition(condition, payload.bars, anchor)) return false;
          if (anchor.asof && !asofMap.has(ticker.code)) {
            asofMap.set(ticker.code, formatDateYMD(anchor.time));
          }
        }
        return true;
      });
      return { items: applyBoxFilter(items), asofMap };
    },
    [
      sectorFiltered,
      barsCache,
      resolveAnchorTimeForTimeframe,
      boxMonthLabel,
      latestBoxMonthLabel,
      normalizeMonthLabel
    ]
  );

  const canAddWatchlist = useMemo(() => {
    if (!normalizedSearch) return null;
    if (searchFiltered.length > 0) return null;
    if (tickers.some((item) => item.code === normalizedSearch)) return null;
    return normalizedSearch;
  }, [normalizedSearch, searchFiltered.length, tickers]);
  const activeFilterResult = useMemo(
    () => buildFilterResult(techFilterActive),
    [buildFilterResult, techFilterActive]
  );

  const draftFilterResult = useMemo(
    () => buildFilterResult(techFilterDraft),
    [buildFilterResult, techFilterDraft]
  );

  const technicalFiltered = activeFilterResult.items;
  const activeConditionTimeframes = useMemo(() => {
    return new Set(techFilterActive.conditions.map((condition) => condition.timeframe));
  }, [techFilterActive.conditions]);
  const hasActiveFilters = useMemo(
    () => techFilterActive.conditions.length > 0 || techFilterActive.boxThisMonth,
    [techFilterActive.conditions.length, techFilterActive.boxThisMonth]
  );
  const hasActiveFilterChips = hasActiveFilters || buyStateFilter !== "all" || shortTierAbOnly;
  const activeTimeframeLabel = useMemo(() => {
    if (activeConditionTimeframes.size === 0) return "未設定";
    if (activeConditionTimeframes.size === 1) {
      const value = Array.from(activeConditionTimeframes)[0];
      return value === "daily" ? "日足" : value === "weekly" ? "週足" : "月足";
    }
    return "複数";
  }, [activeConditionTimeframes]);

  const shouldShowAsof = useMemo(() => {
    if (techFilterActive.conditions.length > 0) return true;
    return ["ma20Dev", "ma60Dev", "ma20Slope", "ma60Slope"].includes(sortKey);
  }, [techFilterActive.conditions.length, sortKey]);

  const scoredTickers = useMemo(() => {
    return technicalFiltered.map((ticker, index) => {
      const payload = barsCache[gridTimeframe][ticker.code];
      const metrics = payload?.bars?.length ? computeSignalMetrics(payload.bars, 4) : null;
      return { ticker, metrics, index };
    });
  }, [technicalFiltered, barsCache, gridTimeframe]);

  const collator = useMemo(
    () => new Intl.Collator("ja-JP", { numeric: true, sensitivity: "base" }),
    []
  );

  const sortedTickers = useMemo(() => {
    const boxOrder: Record<string, number> = {
      IN_BOX: 3,
      JUST_BRAKOUT: 2,
      BRAKOUT_UP: 2,
      BRAKOUT_DOWN: 2,
      NON: 0
    };

    const activeKey = sortKey;

    const isBuyStateSort = activeKey === "buyCandidate";

    const resolveDeviation = (bars: number[][] | undefined, anchor: AnchorInfo | undefined, period: number) => {
      if (!bars || !anchor) return null;
      const close = resolveOperandValue(bars, anchor.index, { type: "field", field: "C" });
      const ma = computeMAAt(bars, anchor.index, period);
      if (close == null || ma == null || ma === 0) return null;
      return (close - ma) / ma;
    };
    const resolveSlope = (bars: number[][] | undefined, anchor: AnchorInfo | undefined, period: number) => {
      if (!bars || !anchor || anchor.prevIndex == null) return null;
      const now = computeMAAt(bars, anchor.index, period);
      const prev = computeMAAt(bars, anchor.prevIndex, period);
      if (now == null || prev == null) return null;
      return now - prev;
    };

    const items = scoredTickers.map((item) => {
      const ticker = item.ticker;
      const bars = barsCache[gridTimeframe][ticker.code]?.bars;
      const anchor = listAnchorInfoByCode.get(ticker.code);
      let sortValue: string | number | null = null;

      // Calculate sort value based on activeKey
      if ((activeKey === "upScore" || activeKey === "downScore") && ticker.statusLabel === "UNKNOWN") {
        sortValue = null;
      } else if (activeKey === "code") {
        sortValue = ticker.code;
      } else if (activeKey === "name") {
        sortValue = ticker.name ?? "";
      } else if (activeKey === "sector") {
        sortValue = ticker.sector33Code ?? null;
      } else if (activeKey === "ma20Dev") {
        sortValue = resolveDeviation(bars, anchor, 20);
      } else if (activeKey === "ma60Dev") {
        sortValue = resolveDeviation(bars, anchor, 60);
      } else if (activeKey === "ma20Slope") {
        sortValue = resolveSlope(bars, anchor, 20);
      } else if (activeKey === "ma60Slope") {
        sortValue = resolveSlope(bars, anchor, 60);
      } else if (activeKey === "chg1D") {
        sortValue = ticker.chg1D ?? null;
      } else if (activeKey === "chg1W") {
        sortValue = ticker.chg1W ?? null;
      } else if (activeKey === "chg1M") {
        sortValue = ticker.chg1M ?? null;
      } else if (activeKey === "chg1Q") {
        sortValue = ticker.chg1Q ?? null;
      } else if (activeKey === "chg1Y") {
        sortValue = ticker.chg1Y ?? null;
      } else if (activeKey === "prevWeekChg") {
        sortValue = ticker.prevWeekChg ?? null;
      } else if (activeKey === "prevMonthChg") {
        sortValue = ticker.prevMonthChg ?? null;
      } else if (activeKey === "prevQuarterChg") {
        sortValue = ticker.prevQuarterChg ?? null;
      } else if (activeKey === "prevYearChg") {
        sortValue = ticker.prevYearChg ?? null;
      } else if (activeKey === "upScore") {
        sortValue = ticker.scores?.upScore ?? null;
      } else if (activeKey === "downScore") {
        sortValue = ticker.scores?.downScore ?? null;
      } else if (activeKey === "overheatUp") {
        sortValue = ticker.scores?.overheatUp ?? null;
      } else if (activeKey === "overheatDown") {
        sortValue = ticker.scores?.overheatDown ?? null;
      } else if (activeKey === "swingScore") {
        sortValue = ticker.swingScore ?? ticker.swingLongScore ?? ticker.swingShortScore ?? null;
      } else if (activeKey === "mlEv20Net") {
        sortValue = ticker.mlEv20Net ?? null;
      } else if (activeKey === "mlPUpShort") {
        sortValue = ticker.mlPUpShort ?? ticker.mlPUp ?? null;
      } else if (activeKey === "mlPDownShort") {
        sortValue = ticker.mlPDownShort ?? ticker.mlPDown ?? null;
      } else if (activeKey === "boxState") {
        const state = ticker.boxState ?? "NON";
        sortValue = boxOrder[state] ?? 0;
      } else if (activeKey === "shortScore") {
        sortValue = ticker.shortScore ?? null;
      } else if (activeKey === "aScore") {
        sortValue = ticker.aScore ?? null;
      } else if (activeKey === "bScore") {
        sortValue = ticker.bScore ?? null;
      } else if (activeKey === "shortPriority") {
        sortValue = ticker.shortPriorityScore ?? null;
      } else if (activeKey === "entryPriority") {
        sortValue = ticker.entryPriorityScore ?? null;
      } else if (activeKey === "buySignalLatest") {
        sortValue = resolveGridSignalSortScore(item.metrics, ticker.liquidity20d, "up");
      } else if (activeKey === "sellSignalLatest") {
        sortValue = resolveGridSignalSortScore(item.metrics, ticker.liquidity20d, "down");
      } else if (activeKey === "volumeSurge") {
        sortValue = resolveGridVolumeSurgeRatio(bars, 20);
      } else if (activeKey === "performance") {
        // Use selected performance period
        switch (performancePeriod) {
          case "1D": sortValue = ticker.chg1D ?? null; break;
          case "1W": sortValue = ticker.chg1W ?? null; break;
          case "1M": sortValue = ticker.chg1M ?? null; break;
          case "1Q": sortValue = ticker.chg1Q ?? null; break;
          case "1Y": sortValue = ticker.chg1Y ?? null; break;
          default: sortValue = ticker.chg1M ?? null;
        }
      } else if (isBuyStateSort) {
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
      // ... same logic as before, just using activeKey
      const aRank = Number.isFinite(a.ticker.buyStateRank) ? (a.ticker.buyStateRank as number) : 0;
      const bRank = Number.isFinite(b.ticker.buyStateRank) ? (b.ticker.buyStateRank as number) : 0;
      const aScore = Number.isFinite(a.ticker.buyStateScore) ? (a.ticker.buyStateScore as number) : null;
      const bScore = Number.isFinite(b.ticker.buyStateScore) ? (b.ticker.buyStateScore as number) : null;
      const aRisk = Number.isFinite(a.ticker.buyRiskDistance) ? (a.ticker.buyRiskDistance as number) : null;
      const bRisk = Number.isFinite(b.ticker.buyRiskDistance) ? (b.ticker.buyRiskDistance as number) : null;

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
      // 1. Inner Sort (using activeKey)
      if (isBuyStateSort) {
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

    let filteredItems = items;
    if (buyStateFilter === "initial") {
      filteredItems = filteredItems.filter((item) => item.ticker.buyState === "初動");
    } else if (buyStateFilter === "base") {
      filteredItems = filteredItems.filter((item) => item.ticker.buyState === "底がため");
    }
    if (shortTierAbOnly) {
      filteredItems = filteredItems.filter((item) => {
        const tier = item.ticker.shortPriorityTier;
        return tier === "A" || tier === "B";
      });
    }
    filteredItems.sort(compare);
    return filteredItems;
  }, [
    scoredTickers,
    sortKey,
    sortDir,
    collator,
    barsCache,
    gridTimeframe,
    listAnchorInfoByCode,
    performancePeriod,
    buyStateFilter,
    shortTierAbOnly
  ]);
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
  const compactTileHeader = columns >= 3 && rows >= 3;
  const showSkeleton = backendReady && loadingList && tickers.length === 0;
  const visibleMaSignature = useMemo(
    () =>
      maSettings[gridTimeframe]
        .map((setting) => `${setting.period}:${setting.visible ? 1 : 0}`)
        .join("|"),
    [gridTimeframe, maSettings]
  );

  const buildVisibleRequestKey = useCallback(
    (codes: string[]) =>
      `${gridTimeframe}:${listRangeBars}:${visibleMaSignature}:${codes.join(",")}`,
    [gridTimeframe, listRangeBars, visibleMaSignature]
  );

  const requestVisibleBars = useCallback(
    (codes: string[], reason: string, mode: "immediate" | "deferred" = "immediate") => {
      if (!backendReady || !codes.length) return;
      const requestKey = buildVisibleRequestKey(codes);
      if (lastVisibleRequestKeyRef.current === requestKey) return;

      const run = () => {
        lastVisibleRequestKeyRef.current = requestKey;
        ensureBarsForVisible(gridTimeframe, codes, reason);
      };

      if (mode === "deferred") {
        if (deferredVisibleRequestTimerRef.current !== null) {
          window.clearTimeout(deferredVisibleRequestTimerRef.current);
        }
        deferredVisibleRequestTimerRef.current = window.setTimeout(() => {
          deferredVisibleRequestTimerRef.current = null;
          run();
        }, 120);
        return;
      }

      if (deferredVisibleRequestTimerRef.current !== null) {
        window.clearTimeout(deferredVisibleRequestTimerRef.current);
        deferredVisibleRequestTimerRef.current = null;
      }
      run();
    },
    [backendReady, buildVisibleRequestKey, ensureBarsForVisible, gridTimeframe]
  );

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
    const summarySignature = codes.join(",");
    if (summarySignature !== lastVisibleSummarySignatureRef.current) {
      lastVisibleSummarySignatureRef.current = summarySignature;
      setTradexVisibleCodes(codes);
    }
    requestVisibleBars(codes, "scroll");
  };

  useEffect(() => {
    if (!backendReady) return;
    if (!lastVisibleCodesRef.current.length) return;
    requestVisibleBars(lastVisibleCodesRef.current, "timeframe-or-range-change", "deferred");
  }, [backendReady, gridTimeframe, listRangeBars, maSettings, requestVisibleBars]);

  useEffect(() => {
    if (!backendReady) return;
    let disposed = false;
    const retryVisibleErrorTiles = async () => {
      const visibleCodes = lastVisibleCodesRef.current;
      if (!visibleCodes.length) return;
      const now = Date.now();
      const state = useStore.getState();
      const statusMap = state.barsStatus[gridTimeframe] ?? {};
      const retryCodes: string[] = [];
      visibleCodes.forEach((code) => {
        if (statusMap[code] !== "error") return;
        const cooldownKey = `${gridTimeframe}:${code}`;
        const nextAllowedAt = barsErrorRetryCooldownRef.current[cooldownKey] ?? 0;
        if (now < nextAllowedAt) return;
        barsErrorRetryCooldownRef.current[cooldownKey] = now + BARS_ERROR_RETRY_COOLDOWN_MS;
        retryCodes.push(code);
      });
      if (!retryCodes.length || disposed) return;
      try {
        await ensureBarsForVisible(gridTimeframe, retryCodes, "visible-error-retry");
      } catch {
        // Keep polling with cooldown; transient failures are expected.
      }
    };
    void retryVisibleErrorTiles();
    const timer = window.setInterval(() => {
      void retryVisibleErrorTiles();
    }, BARS_ERROR_RETRY_INTERVAL_MS);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [backendReady, gridTimeframe, ensureBarsForVisible]);

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
    requestVisibleBars(codes, "sort-change", "deferred");
  }, [backendReady, sortedTickers, gridTimeframe, requestVisibleBars]);

  useEffect(() => {
    return () => {
      if (deferredVisibleRequestTimerRef.current !== null) {
        window.clearTimeout(deferredVisibleRequestTimerRef.current);
      }
    };
  }, []);

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
        showToast(`${code} を除外しました。`);
      } catch (error) {
        const message = error instanceof Error ? error.message : "ウォッチリスト削除に失敗しました。";
        showToast(message);
      }
    },
    [loadList, showToast]
  );

  const handleToggleKeep = useCallback(
    (code: string) => {
      if (!code) return;
      if (keepList.includes(code)) {
        removeKeep(code);
        return;
      }
      if (keepList.length >= KP_LIMIT) {
        showToast(`候補キープは最大 ${KP_LIMIT} 件です。`);
        return;
      }
      addKeep(code);
    },
    [keepList, addKeep, removeKeep, showToast]
  );

  const handleExclude = useCallback(
    (code: string) => {
      if (!code) return;
      handleRemoveWatchlist(code, false);
    },
    [handleRemoveWatchlist]
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
        setSettingsOpen(false);
        setSectorSortOpen(false); // Close sector sort popover
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
      showToast(`${undoInfo.code} の除外を取り消しました。`);
    } catch {
      showToast("除外の取り消しに失敗しました。");
    } finally {
      if (undoTimerRef.current) {
        window.clearTimeout(undoTimerRef.current);
      }
      setUndoInfo(null);
    }
  }, [undoInfo, loadList, showToast]);

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

  const sanitizeTechFilterState = (
    state: TechnicalFilterState,
    fallbackTimeframe: Timeframe
  ) => {
    const defaultTimeframe =
      state.defaultTimeframe === "daily" ||
        state.defaultTimeframe === "weekly" ||
        state.defaultTimeframe === "monthly"
        ? state.defaultTimeframe
        : fallbackTimeframe;
    const result = sanitizeTechnicalConditions(
      state.conditions as unknown[],
      defaultTimeframe
    );
    if (result.dropped > 0 && !techFilterDropNoticeRef.current) {
      showToast("不正なフィルタ条件を自動で除外しました。");
      techFilterDropNoticeRef.current = true;
    }
    return {
      ...state,
      defaultTimeframe,
      conditions: result.conditions,
      boxThisMonth: typeof state.boxThisMonth === "boolean" ? state.boxThisMonth : false
    };
  };

  const handleOpenTechFilter = () => {
    setTechFilterDraft(sanitizeTechFilterState(techFilterActive, gridTimeframe));
    setBuyStateFilterDraft(buyStateFilter);
    setShortTierAbOnlyDraft(shortTierAbOnly);
    setTechFilterOpen(true);
  };

  const handleApplyTechFilter = () => {
    const normalized = sanitizeTechFilterState(techFilterDraft, gridTimeframe);
    setTechFilterActive(normalized);
    setTechFilterDraft(normalized);
    setBuyStateFilter(buyStateFilterDraft);
    setShortTierAbOnly(shortTierAbOnlyDraft);
    setTechFilterOpen(false);
  };

  const handleCancelTechFilter = () => {
    setTechFilterDraft(techFilterActive);
    setBuyStateFilterDraft(buyStateFilter);
    setShortTierAbOnlyDraft(shortTierAbOnly);
    setTechFilterOpen(false);
  };

  const handleResetTechFilterDraft = () => {
    setTechFilterDraft(createDefaultTechFilter(techFilterDraft.defaultTimeframe));
    setBuyStateFilterDraft("all");
    setShortTierAbOnlyDraft(false);
  };

  const handleClearActiveFilters = () => {
    setTechFilterActive(createDefaultTechFilter(techFilterActive.defaultTimeframe));
    if (!techFilterOpen) {
      setTechFilterDraft(createDefaultTechFilter(techFilterDraft.defaultTimeframe));
    }
  };

  const handleClearAllActiveFilters = () => {
    handleClearActiveFilters();
    setBuyStateFilter("all");
    setBuyStateFilterDraft("all");
    setShortTierAbOnly(false);
    setShortTierAbOnlyDraft(false);
  };

  const handleUpdateError = useCallback((payload?: TxtUpdateStartPayload) => {
    const error = payload?.error ?? 'unknown';
    if (isTxtUpdateConflictError(error) || payload?.status === "conflict") {
      showToast("日次更新は既に実行中です。");
      return;
    }
    if (error === 'code_txt_missing') {
      showToast("code.txt が見つかりません。");
      return;
    }
    if (error.startsWith('vbs_not_found')) {
      showToast("日次更新スクリプトが見つかりません。");
      return;
    }
    showToast("日次更新の起動に失敗しました。");
  }, [showToast]);

  const applyTxtUpdateStatus = useCallback((payload?: JobStatusPayload | null) => {
    if (!payload || typeof payload.id !== "string" || !payload.id) return;
    const nextStatus = typeof payload.status === "string" ? payload.status : "running";
    const nextMessage = typeof payload.message === "string" ? payload.message : null;
    const hasBackgroundFollowup =
      typeof nextMessage === "string" &&
      (nextMessage.includes("バックグラウンド") || nextMessage.includes("followup=queued("));
    const nextProgress =
      typeof payload.progress === "number" && Number.isFinite(payload.progress)
        ? payload.progress
        : null;
    setTxtUpdateJob({ id: payload.id, status: nextStatus, progress: nextProgress, message: nextMessage });

    if (!TERMINAL_JOB_STATUS.has(nextStatus)) {
      setTxtUpdatePolling(true);
      return;
    }

    setTxtUpdatePolling(false);
    const terminalKey = `${payload.id}:${nextStatus}`;
    if (txtUpdateTerminalStatusRef.current === terminalKey) return;
    txtUpdateTerminalStatusRef.current = terminalKey;

    if (nextStatus === "success") {
      const runDailyFollowup = txtUpdateDailyFollowupRef.current;
      txtUpdateDailyFollowupRef.current = false;
      resetBarsCache();
      void loadList();
      if (!runDailyFollowup) {
        showToast(
          hasBackgroundFollowup
            ? "TXT更新が完了しました。重い後続処理はバックグラウンドで継続します。"
            : "TXT更新が完了しました。"
        );
        return;
      }
      if (eventsRefreshing) {
        showToast(
          hasBackgroundFollowup
            ? "日次更新が完了しました。重い後続処理はバックグラウンドで継続します。イベント更新は既に実行中です。"
            : "日次更新が完了しました。イベント更新は既に実行中です。"
        );
        return;
      }
      void refreshEvents();
      showToast(
        hasBackgroundFollowup
          ? "日次更新が完了しました。重い後続処理はバックグラウンドで継続します。続けてイベント更新を開始しました。"
          : "日次更新が完了しました。続けてイベント更新を開始しました。"
      );
      return;
    }

    if (nextStatus === "canceled") {
      txtUpdateDailyFollowupRef.current = false;
      showToast("日次更新をキャンセルしました。");
      return;
    }
    txtUpdateDailyFollowupRef.current = false;
    const detail = payload.error || payload.message || "詳細不明";
    showToast(`日次更新が失敗しました。(${detail})`, {
      label: "設定",
      onClick: () => {
        setSettingsPanelMode("general");
        setSettingsOpen(true);
      }
    });
  }, [eventsRefreshing, loadList, refreshEvents, showToast, resetBarsCache]);

  useEffect(() => {
    if (!backendReady) return;
    let disposed = false;
    const loadCurrentTxtJob = async () => {
      try {
        const res = await api.get("/jobs/current");
        if (disposed) return;
        const payload = (res.data ?? null) as JobStatusPayload | null;
        if (!payload || payload.type !== "txt_update") return;
        applyTxtUpdateStatus(payload);
      } catch {
        // ignore initial current-job fetch failures
      }
    };
    void loadCurrentTxtJob();
    return () => {
      disposed = true;
    };
  }, [backendReady, applyTxtUpdateStatus]);

  useEffect(() => {
    if (!txtUpdatePolling || !txtUpdateJob?.id) return;
    let disposed = false;
    const poll = async () => {
      try {
        const res = await api.get(`/jobs/${txtUpdateJob.id}`);
        if (disposed) return;
        applyTxtUpdateStatus((res.data ?? null) as JobStatusPayload | null);
      } catch {
        // keep polling; transient errors are common during backend restart
      }
    };
    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 2000);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [txtUpdatePolling, txtUpdateJob?.id, applyTxtUpdateStatus]);



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
      showToast("相場メモがまだありません。");
      return;
    }
    try {
      await navigator.clipboard.writeText(consultText);
      showToast("相場メモをコピーしました。");
    } catch {
      showToast("コピーに失敗しました。");
    }
  }, [consultText, showToast]);

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = keepList.slice(0, limit);
    const extra = Math.max(0, keepList.length - visible.length);
    return { visible, extra };
  }, [keepList]);

  const handleUpdateTxt = useCallback(async () => {
    if (!backendReady) return;
    showToast("日次更新を開始しました。");
    try {
      const res = await api.post("/jobs/txt-update", null, {
        params: { completion_mode: "practical_fast", auto_fill_missing_history: true }
      });
      const payload = (res.data ?? {}) as TxtUpdateStartPayload;
      if (payload.ok === false) {
        txtUpdateDailyFollowupRef.current = false;
        handleUpdateError(payload);
        if (isTxtUpdateConflictError(payload.error) || payload.status === "conflict") {
          try {
            const current = await api.get("/jobs/current");
            const job = (current.data ?? null) as JobStatusPayload | null;
            if (job?.type === "txt_update") {
              applyTxtUpdateStatus(job);
            }
          } catch {
            // ignore current-job fetch failures after conflict
          }
        }
        return;
      }
      const jobId = extractTxtUpdateJobId(payload);
      if (jobId) {
        txtUpdateDailyFollowupRef.current = true;
        txtUpdateTerminalStatusRef.current = null;
        setTxtUpdateJob({
          id: jobId,
          status: "queued",
          progress: 0,
          message: "Waiting in queue..."
        });
        setTxtUpdatePolling(true);
      } else {
        txtUpdateDailyFollowupRef.current = false;
      }
    } catch (error) {
      txtUpdateDailyFollowupRef.current = false;
      let payload: TxtUpdateStartPayload | null = null;
      if (typeof error === "object" && error && "response" in error) {
        const response = (error as { response?: { data?: TxtUpdateStartPayload } }).response;
        payload = response?.data ?? null;
      }
      if (payload) {
        handleUpdateError(payload);
      } else {
        showToast("日次更新の起動に失敗しました。");
      }
    }
  }, [backendReady, handleUpdateError, applyTxtUpdateStatus, showToast]);

  const handleCancelTxtUpdate = useCallback(async () => {
    if (!txtUpdateJob?.id) return;
    try {
      const res = await api.post(`/jobs/${txtUpdateJob.id}/cancel`);
      const payload = (res.data ?? {}) as { cancel_requested?: boolean; status?: string };
      if (payload.cancel_requested) {
        txtUpdateTerminalStatusRef.current = null;
        setTxtUpdatePolling(true);
        setTxtUpdateJob((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            status: typeof payload.status === "string" ? payload.status : "cancel_requested"
          };
        });
        showToast("日次更新のキャンセルを要求しました。");
      } else {
        setTxtUpdatePolling(false);
        showToast("日次更新は既に終了しています。");
      }
    } catch (err) {
      const detail = extractErrorDetail(err);
      showToast(`日次更新のキャンセルに失敗しました。(${detail})`);
    }
  }, [txtUpdateJob?.id, showToast]);

  const handlePhaseRebuild = useCallback(async () => {
    if (!backendReady) return;
    showToast("Phase\u518d\u8a08\u7b97\u3092\u958b\u59cb\u3057\u307e\u3057\u305f\u3002");
    try {
      const res = await api.post("/phase/rebuild");
      const payload = res.data as { ok?: boolean; error?: string };
      if (payload && payload.ok === false) {
        showToast("Phase\u518d\u8a08\u7b97\u306e\u8d77\u52d5\u306b\u5931\u6557\u3057\u307e\u3057\u305f\u3002");
      }
    } catch {
      showToast("Phase\u518d\u8a08\u7b97\u306e\u8d77\u52d5\u306b\u5931\u6557\u3057\u307e\u3057\u305f\u3002");
    }
  }, [backendReady, showToast]);

  const handleAnalysisBatchPrewarm = useCallback(async () => {
    if (!backendReady || analysisBatchSubmitting) return;
    setAnalysisBatchSubmitting(true);
    try {
      const res = await api.post("/jobs/analysis/prewarm-latest", null, {
        params: {
          force_recompute: true,
        },
        timeout: 120000,
      });
      const payload = (res.data ?? {}) as {
        ok?: boolean;
        error?: string;
        message?: string;
        predicted_dates?: number[];
        sell_refreshed_dates?: number[];
        errors?: string[];
      };
      if (payload.ok === false) {
        showToast(`売買判定キャッシュの起動に失敗しました。(${payload.error ?? payload.message ?? "不明"})`);
        return;
      }
      void loadList();
      showToast(
        `売買判定キャッシュの一括計算が完了しました。(ML=${payload.predicted_dates?.length ?? 0}, 売り=${payload.sell_refreshed_dates?.length ?? 0})`
      );
    } catch (err) {
      const response = (err as { response?: { status?: number } }).response;
      if (response?.status === 409) {
        showToast("売買判定キャッシュは既に実行中です。");
      } else {
        showToast(`売買判定キャッシュの起動に失敗しました。(${extractErrorDetail(err)})`);
      }
    } finally {
      setAnalysisBatchSubmitting(false);
    }
  }, [analysisBatchSubmitting, backendReady, loadList, showToast]);

  const fetchLatestWalkforward = useCallback(async (silent = false) => {
    if (!backendReady) return;
    setWalkforwardLoading(true);
    try {
      const res = await api.get("/jobs/strategy/walkforward/latest");
      const payload = (res.data ?? {}) as {
        has_run?: boolean;
        latest?: WalkforwardLatest | null;
      };
      if (payload.has_run && payload.latest) {
        setWalkforwardLatest(payload.latest);
      } else {
        setWalkforwardLatest(null);
      }
      try {
        const researchRes = await api.get("/jobs/strategy/walkforward/research/latest");
        const researchPayload = (researchRes.data ?? {}) as {
          has_snapshot?: boolean;
          latest?: WalkforwardResearchLatest | null;
        };
        if (researchPayload.has_snapshot && researchPayload.latest) {
          setWalkforwardResearchLatest(researchPayload.latest);
        } else {
          setWalkforwardResearchLatest(null);
        }
      } catch (researchErr) {
        setWalkforwardResearchLatest(null);
        if (!silent) {
          const detail = extractErrorDetail(researchErr);
          showToast(`ウォークフォワード研究結果の取得に失敗しました。(${detail})`);
        }
      }
    } catch (err) {
      if (!silent) {
        const detail = extractErrorDetail(err);
        showToast(`ウォークフォワード結果の取得に失敗しました。(${detail})`);
      }
    } finally {
      setWalkforwardLoading(false);
    }
  }, [backendReady, showToast]);

  useEffect(() => {
    if (!backendReady) return;
    void fetchLatestWalkforward(true);
  }, [backendReady, fetchLatestWalkforward]);

  const applyWalkforwardTenYearPreset = useCallback(() => {
    setWalkforwardParams((prev) => ({
      ...prev,
      trainMonths: 120,
      testMonths: 12,
      stepMonths: 12,
      minWindows: 1
    }));
    showToast("10年単位プリセットを適用しました。");
  }, [showToast]);

  const handleRunWalkforward = useCallback(async () => {
    if (!backendReady || walkforwardSubmitting) return;
    setWalkforwardSubmitting(true);
    try {
      const maxNewEntriesPerMonth = parseOptionalNumber(walkforwardParams.maxNewEntriesPerMonth);
      const minMlPUpLong = parseOptionalNumber(walkforwardParams.minMlPUpLong);
      const regimeLongMin = parseOptionalNumber(walkforwardParams.regimeLongMinBreadthAbove60);
      const regimeShortMax = parseOptionalNumber(walkforwardParams.regimeShortMaxBreadthAbove60);
      const params: Record<string, string | number | boolean> = {
        train_months: walkforwardParams.trainMonths,
        test_months: walkforwardParams.testMonths,
        step_months: walkforwardParams.stepMonths,
        min_windows: walkforwardParams.minWindows,
        max_codes: walkforwardParams.maxCodes,
        allowed_sides: walkforwardParams.allowedSides,
        min_long_score: walkforwardParams.minLongScore,
        min_short_score: walkforwardParams.minShortScore,
        max_new_entries_per_day: walkforwardParams.maxNewEntriesPerDay,
        use_regime_filter: walkforwardParams.useRegimeFilter,
        regime_breadth_lookback_days: walkforwardParams.regimeBreadthLookbackDays
      };
      if (typeof maxNewEntriesPerMonth === "number") {
        params.max_new_entries_per_month = maxNewEntriesPerMonth;
      }
      if (typeof minMlPUpLong === "number") {
        params.min_ml_p_up_long = minMlPUpLong;
      }
      if (typeof regimeLongMin === "number") {
        params.regime_long_min_breadth_above60 = regimeLongMin;
      }
      if (typeof regimeShortMax === "number") {
        params.regime_short_max_breadth_above60 = regimeShortMax;
      }
      const allowedLongSetups = walkforwardParams.allowedLongSetups.trim();
      const allowedShortSetups = walkforwardParams.allowedShortSetups.trim();
      if (allowedLongSetups) {
        params.allowed_long_setups = allowedLongSetups;
      }
      if (allowedShortSetups) {
        params.allowed_short_setups = allowedShortSetups;
      }
      const res = await api.post("/jobs/strategy/walkforward", null, {
        params
      });
      const payload = (res.data ?? {}) as { ok?: boolean; error?: string; job_id?: string };
      if (payload.ok === false) {
        showToast(`ウォークフォワード検証の起動に失敗しました。(${payload.error ?? "不明"})`);
        return;
      }
      showToast("ウォークフォワード検証ジョブを開始しました。");
    } catch (err) {
      const detail = extractErrorDetail(err);
      showToast(`ウォークフォワード検証の起動に失敗しました。(${detail})`);
    } finally {
      setWalkforwardSubmitting(false);
    }
  }, [backendReady, parseOptionalNumber, walkforwardSubmitting, walkforwardParams, showToast]);

  const walkforwardSummary = walkforwardLatest?.report?.summary ?? null;
  const walkforwardTopWindows = useMemo(() => {
    const windows = Array.isArray(walkforwardLatest?.report?.windows)
      ? walkforwardLatest?.report?.windows
      : [];
    const successRows = windows.filter((row) => row?.status === "success");
    return successRows.slice(0, 5);
  }, [walkforwardLatest]);
  const walkforwardAttributionCode = useMemo(
    () => formatAttributionRows(walkforwardLatest?.report?.attribution?.code),
    [formatAttributionRows, walkforwardLatest]
  );
  const walkforwardAttributionSetup = useMemo(
    () =>
      formatAttributionRows(
        walkforwardLatest?.report?.attribution?.setup_id ??
          walkforwardLatest?.report?.attribution?.setup
      ),
    [formatAttributionRows, walkforwardLatest]
  );
  const walkforwardAttributionSector = useMemo(
    () => formatAttributionRows(walkforwardLatest?.report?.attribution?.sector33_code),
    [formatAttributionRows, walkforwardLatest]
  );
  const walkforwardResearchReport = walkforwardResearchLatest?.report ?? null;
  const walkforwardResearchAdoptedSetups = useMemo(() => {
    const rows = Array.isArray(walkforwardResearchReport?.adopted_setups)
      ? walkforwardResearchReport?.adopted_setups
      : [];
    return rows.slice(0, 5);
  }, [walkforwardResearchReport]);
  const walkforwardResearchRejectedReasons = useMemo(() => {
    const rows = Array.isArray(walkforwardResearchReport?.rejected_reasons)
      ? walkforwardResearchReport?.rejected_reasons
      : [];
    return rows.slice(0, 5);
  }, [walkforwardResearchReport]);
  const walkforwardResearchHedgeContribution = walkforwardResearchReport?.hedge_contribution ?? null;
  const normalizedWalkforwardPresetName = useMemo(
    () => normalizeWalkforwardPresetName(walkforwardPresetName),
    [normalizeWalkforwardPresetName, walkforwardPresetName]
  );
  const matchedWalkforwardPreset = useMemo(() => {
    if (!normalizedWalkforwardPresetName) return null;
    const key = normalizedWalkforwardPresetName.toLowerCase();
    return (
      walkforwardPresets.find((preset) => preset.name.toLowerCase() === key) ?? null
    );
  }, [normalizedWalkforwardPresetName, walkforwardPresets]);

  const handleSaveWalkforwardPreset = useCallback(() => {
    const name = normalizeWalkforwardPresetName(walkforwardPresetName);
    if (!name) {
      showToast("プリセット名を入力してください。");
      return;
    }
    const nowIso = new Date().toISOString();
    setWalkforwardPresets((prev) => {
      const key = name.toLowerCase();
      const index = prev.findIndex((item) => item.name.toLowerCase() === key);
      const payload: WalkforwardPreset = {
        name,
        params: { ...walkforwardParams },
        createdAt: index >= 0 ? prev[index].createdAt : nowIso,
        updatedAt: nowIso,
      };
      const next = [...prev];
      if (index >= 0) {
        next[index] = payload;
      } else {
        next.push(payload);
      }
      next.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
      return next.slice(0, WALKFORWARD_PRESETS_LIMIT);
    });
    setWalkforwardPresetName(name);
    showToast(`プリセットを保存しました。(${name})`);
  }, [normalizeWalkforwardPresetName, showToast, walkforwardParams, walkforwardPresetName]);

  const handleLoadWalkforwardPreset = useCallback(() => {
    if (!matchedWalkforwardPreset) {
      showToast("読み込むプリセットが見つかりません。");
      return;
    }
    setWalkforwardParams({ ...matchedWalkforwardPreset.params });
    setWalkforwardPresetName(matchedWalkforwardPreset.name);
    showToast(`プリセットを読み込みました。(${matchedWalkforwardPreset.name})`);
  }, [matchedWalkforwardPreset, showToast]);

  const handleDeleteWalkforwardPreset = useCallback(() => {
    if (!matchedWalkforwardPreset) {
      showToast("削除対象のプリセットが見つかりません。");
      return;
    }
    const target = matchedWalkforwardPreset.name;
    setWalkforwardPresets((prev) =>
      prev.filter((item) => item.name.toLowerCase() !== target.toLowerCase())
    );
    setWalkforwardPresetName("");
    showToast(`プリセットを削除しました。(${target})`);
  }, [matchedWalkforwardPreset, showToast]);

  const normalizeImportedWalkforwardPresets = useCallback(
    (payload: unknown) => {
      const rawList = Array.isArray(payload)
        ? payload
        : payload && typeof payload === "object" && Array.isArray((payload as { presets?: unknown }).presets)
          ? ((payload as { presets: unknown[] }).presets)
          : [];
      const nowIso = new Date().toISOString();
      const deduped = new Map<string, WalkforwardPreset>();
      for (const entry of rawList) {
        if (!entry || typeof entry !== "object") continue;
        const rawName = (entry as { name?: unknown }).name;
        const name = typeof rawName === "string" ? normalizeWalkforwardPresetName(rawName) : "";
        if (!name) continue;
        const params = toWalkforwardParams((entry as { params?: unknown }).params);
        const createdAtRaw = (entry as { createdAt?: unknown }).createdAt;
        const updatedAtRaw = (entry as { updatedAt?: unknown }).updatedAt;
        const item: WalkforwardPreset = {
          name,
          params,
          createdAt: typeof createdAtRaw === "string" && createdAtRaw ? createdAtRaw : nowIso,
          updatedAt: typeof updatedAtRaw === "string" && updatedAtRaw ? updatedAtRaw : nowIso,
        };
        deduped.set(name.toLowerCase(), item);
      }
      return Array.from(deduped.values());
    },
    [normalizeWalkforwardPresetName]
  );

  const handleExportWalkforwardPresets = useCallback(async () => {
    if (!walkforwardPresets.length) {
      showToast("エクスポート対象のプリセットがありません。");
      return;
    }
    const now = new Date();
    const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(
      now.getDate()
    ).padStart(2, "0")}_${String(now.getHours()).padStart(2, "0")}${String(
      now.getMinutes()
    ).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
    const payload = {
      version: 1,
      exportedAt: now.toISOString(),
      presets: walkforwardPresets,
    };
    try {
      const ok = await saveAsFile(
        JSON.stringify(payload, null, 2),
        `walkforward-presets-${stamp}.json`,
        "application/json"
      );
      showToast(ok ? "ウォークフォワードプリセットを書き出しました。" : "書き出しに失敗しました。");
    } catch {
      showToast("書き出しに失敗しました。");
    }
  }, [showToast, walkforwardPresets]);

  const handlePickWalkforwardPresetImport = useCallback(() => {
    if (walkforwardPresetImporting) return;
    walkforwardPresetImportInputRef.current?.click();
  }, [walkforwardPresetImporting]);

  const handleImportWalkforwardPresetFile = useCallback(
    async (event: ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      if (!file || walkforwardPresetImporting) return;
      setWalkforwardPresetImporting(true);
      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        const imported = normalizeImportedWalkforwardPresets(parsed);
        if (!imported.length) {
          showToast("インポート可能なプリセットが見つかりませんでした。");
          return;
        }
        const currentKeys = new Set(walkforwardPresets.map((item) => item.name.toLowerCase()));
        const overwriteCount = imported.filter((item) => currentKeys.has(item.name.toLowerCase())).length;
        const nowIso = new Date().toISOString();
        setWalkforwardPresets((prev) => {
          const byKey = new Map<string, WalkforwardPreset>();
          for (const item of prev) {
            byKey.set(item.name.toLowerCase(), item);
          }
          for (const incoming of imported) {
            const key = incoming.name.toLowerCase();
            const existing = byKey.get(key);
            byKey.set(key, {
              ...incoming,
              createdAt: existing?.createdAt ?? incoming.createdAt,
              updatedAt: nowIso,
            });
          }
          const merged = Array.from(byKey.values());
          merged.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
          return merged.slice(0, WALKFORWARD_PRESETS_LIMIT);
        });
        if (imported.length === 1) {
          setWalkforwardPresetName(imported[0].name);
        }
        showToast(
          overwriteCount > 0
            ? `${imported.length}件インポートしました（上書き ${overwriteCount}件）。`
            : `${imported.length}件インポートしました。`
        );
      } catch {
        showToast("インポートに失敗しました。JSON形式を確認してください。");
      } finally {
        setWalkforwardPresetImporting(false);
        event.target.value = "";
      }
    },
    [
      normalizeImportedWalkforwardPresets,
      showToast,
      walkforwardPresetImporting,
      walkforwardPresets
    ]
  );

  const formatJobTypeLabel = useCallback((jobType: string | null | undefined) => {
    switch (jobType) {
      case "txt_update":
        return "日次更新";
      case "txt_followup":
        return "後続更新";
      case "force_sync":
        return "強制同期";
      case "phase_rebuild":
        return "Phase再計算";
      case "analysis_backfill":
        return "売買判定一括計算";
      case "ml_train":
        return "ML学習";
      case "ml_predict":
        return "ML予測";
      case "strategy_backtest":
        return "戦略バックテスト";
      case "strategy_walkforward":
        return "ウォークフォワード検証";
      default:
        return jobType || "ジョブ";
    }
  }, []);

  const notifyTerminalJob = useCallback(async (item: JobHistoryItem) => {
    const id = typeof item.id === "string" ? item.id : "";
    const type = typeof item.type === "string" ? item.type : "";
    const status = typeof item.status === "string" ? item.status : "";
    if (!id || !type || !status) return;
    if (type === "txt_update") return;

    let detail: string | null = typeof item.message === "string" ? item.message : null;
    if (status === "failed") {
      try {
        const detailRes = await api.get(`/jobs/${id}`);
        const payload = (detailRes.data ?? null) as JobStatusPayload | null;
        if (payload) {
          const fromError = typeof payload.error === "string" && payload.error.trim() ? payload.error : null;
          const fromMessage =
            typeof payload.message === "string" && payload.message.trim() ? payload.message : null;
          detail = fromError ?? fromMessage ?? detail;
        }
      } catch {
        // Keep history-level detail when detail fetch fails.
      }
    }

    const label = formatJobTypeLabel(type);
    if (status === "success") {
      if (type === "strategy_walkforward") {
        void fetchLatestWalkforward(true);
      }
      if (type === "force_sync") {
        resetBarsCache();
        void loadList();
      }
      if (type === "analysis_backfill") {
        void loadList();
      }
      if (type === "txt_followup") {
        void loadList();
      }
      showToast(`${label}が完了しました。`);
      return;
    }
    if (status === "canceled") {
      showToast(`${label}をキャンセルしました。`);
      return;
    }
    if (status === "skipped") {
      showToast(`${label}はスキップされました。(${detail ?? "詳細不明"})`);
      return;
    }

    let action: ToastAction | null = null;
    if (type === "phase_rebuild") {
      action = {
        label: "再実行",
        onClick: () => {
          void handlePhaseRebuild();
        }
      };
    } else if (type === "analysis_backfill") {
      action = {
        label: "再実行",
        onClick: () => {
          setSettingsPanelMode("general");
          setSettingsDetailsOpen(true);
          setSettingsOpen(true);
          void handleAnalysisBatchPrewarm();
        }
      };
    } else if (type === "strategy_walkforward") {
      action = {
        label: "再実行",
        onClick: () => {
          setSettingsPanelMode("walkforward");
          setSettingsDetailsOpen(true);
          setSettingsOpen(true);
          void handleRunWalkforward();
        }
      };
    }
    if (type === "txt_followup") {
      showToast(`後続更新が失敗しました。日次データ更新は完了済みです。(${detail ?? "詳細不明"})`);
      return;
    }
    showToast(`${label}が失敗しました。(${detail ?? "詳細不明"})`, action);
  }, [fetchLatestWalkforward, formatJobTypeLabel, handleAnalysisBatchPrewarm, handlePhaseRebuild, handleRunWalkforward, loadList, resetBarsCache, showToast]);
  useEffect(() => {
    if (!backendReady || GRID_REFACTOR_ENABLED) return;
    let disposed = false;
    let timer: number | null = null;
    const scheduleNext = (delayMs: number) => {
      if (disposed) return;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
      timer = window.setTimeout(() => {
        void pollTerminalJobs();
      }, delayMs);
    };

    const pollTerminalJobs = async () => {
      let nextDelayMs = 15000;
      try {
        const res = await api.get("/jobs/history", { params: { limit: 20 } });
        if (disposed) return;
        const list = Array.isArray(res.data) ? (res.data as JobHistoryItem[]) : [];
        const hasActiveJobs = list.some((entry) => ACTIVE_JOB_STATUS.has(String(entry?.status ?? "")));
        nextDelayMs = hasActiveJobs ? 4000 : 15000;
        const terminalItems = list.filter((entry) =>
          TERMINAL_JOB_STATUS.has(String(entry?.status ?? ""))
        );
        if (!terminalJobsInitializedRef.current) {
          for (const entry of terminalItems) {
            if (typeof entry.id === "string" && entry.id) {
              seenTerminalJobsRef.current.add(entry.id);
            }
          }
          terminalJobsInitializedRef.current = true;
          scheduleNext(nextDelayMs);
          return;
        }
        for (const entry of [...terminalItems].reverse()) {
          const id = typeof entry.id === "string" ? entry.id : "";
          if (!id) continue;
          if (seenTerminalJobsRef.current.has(id)) continue;
          seenTerminalJobsRef.current.add(id);
          void notifyTerminalJob(entry);
        }
      } catch {
        // Keep silent; polling failures are transient.
      }
      scheduleNext(nextDelayMs);
    };

    void pollTerminalJobs();
    return () => {
      disposed = true;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
    };
  }, [backendReady, notifyTerminalJob]);
  useTerminalJobPolling({ enabled: backendReady && GRID_REFACTOR_ENABLED, onTerminalJob: notifyTerminalJob });


  return (
    <div className="app-shell list-view">
      <header className="unified-list-header">
        <div className="list-header-row">
          <div className="header-row-top">
            <div className="header-row-left">
              <TopNav />
            </div>
            <div className="list-header-actions-wrapper">
              <div className="list-header-actions">
                {keepList.length > 0 && (
                  <button
                    type="button"
                    className={`consult-trigger ${consultVisible ? "active" : ""}`}
                    onClick={() => setConsultVisible(!consultVisible)}
                  >
                    <IconMessage size={16} />
                    <span>相談</span>
                    <span className="badge">{keepList.length}</span>
                  </button>
                )}
                <div className="list-header-spacer" style={{ width: 8 }} />
                <div className="popover-anchor" ref={sortRef}>
                  <IconButton
                    icon={<IconArrowsSort size={18} />}
                    label={`並び: ${sortLabel}`}
                    variant="iconLabel"
                    tooltip="並び替え"
                    ariaLabel="並び替えメニューを開く"
                    selected={sortOpen}
                    onClick={() => {
                      setSortOpen(!sortOpen);
                      setDisplayOpen(false);
                      setSettingsOpen(false);
                      setSectorSortOpen(false);
                    }}
                  />
                  {sortOpen && (
                    <div className="popover-panel sort-popover-panel">
                      {visibleSortSections.map((section) => {
                        const expanded = openSortSections.includes(section.title);
                        return (
                        <div className="popover-section" key={section.title}>
                          <button
                            type="button"
                            className={`popover-section-toggle ${expanded ? "active" : ""}`}
                            onClick={() =>
                              setOpenSortSections((current) =>
                                current.includes(section.title)
                                  ? current.filter((title) => title !== section.title)
                                  : [...current, section.title]
                              )
                            }
                          >
                            <span className="popover-title">{section.title}</span>
                            <span className="popover-section-meta">
                              {section.options.length}件 {expanded ? "−" : "+"}
                            </span>
                          </button>
                          {expanded && (
                            <div className="popover-grid">
                              {section.options.map((opt) => (
                                <button
                                  key={opt.key}
                                  type="button"
                                  className={`popover-item ${sortKey === opt.key ? "active" : ""}`}
                                  onClick={() => {
                                    if (opt.fixedDirection) {
                                      setSortKey(opt.key);
                                      setSortDir(opt.fixedDirection);
                                    } else if (sortKey === opt.key) {
                                      setSortDir(sortDir === "asc" ? "desc" : "asc");
                                    } else {
                                      setSortKey(opt.key);
                                      setSortDir(opt.defaultDirection ?? "desc");
                                    }
                                    setSortOpen(false);
                                  }}
                                >
                                  <span className="popover-item-label">{opt.label}</span>
                                  {sortKey === opt.key && (
                                    <span className="popover-check">{sortDirLabel}</span>
                                  )}
                                </button>
                              ))}
                            </div>
                          )}
                        </div>
                      )})}
                    </div>
                  )}
                </div>
                <div className="popover-anchor" ref={displayRef}>
                  <IconButton
                    icon={<IconLayoutGrid size={18} />}
                    label="表示密度"
                    variant="iconLabel"
                    tooltip="表示密度"
                    ariaLabel="表示密度メニューを開く"
                    selected={displayOpen}
                    onClick={() => {
                      setDisplayOpen(!displayOpen);
                      setSortOpen(false);
                      setSettingsOpen(false);
                      setSectorSortOpen(false);
                    }}
                  />
                  {displayOpen && (
                    <div className="popover-panel">
                      <div className="popover-section">
                        <div className="popover-title">表示密度</div>
                        <div className="segmented segmented-grid-preset">
                          {gridPresetOptions.map((preset) => (
                            <button
                              key={preset.value}
                              type="button"
                              className={rows === preset.value && columns === preset.value ? "active" : ""}
                              onClick={() => setRows(preset.value)}
                            >
                              {preset.label}
                            </button>
                          ))}
                        </div>
                        <div className="popover-note">表示本数: {listRangeBars}本</div>
                      </div>
                      <div className="popover-section">
                        <div className="popover-title">表示オプション</div>
                        <button
                          className={`popover-item ${showBoxes ? "active" : ""}`}
                          onClick={() => setShowBoxes(!showBoxes)}
                        >
                          <span className="popover-item-label">ボックス枠を表示</span>
                          {showBoxes && <span className="popover-check">ON</span>}
                        </button>
                        <button
                          className={`popover-item ${showIndicators ? "active" : ""}`}
                          onClick={() => {
                            setShowIndicators(!showIndicators);
                            setDisplayOpen(false);
                          }}
                        >
                          <span className="popover-item-header-label">インジケーター設定</span>
                        </button>
                        <button
                          className={`popover-item ${maSettings[gridTimeframe].some(s => s.visible) ? "active" : ""}`}
                          onClick={() => {
                            if (maSettings[gridTimeframe].some(s => s.visible)) {
                              const newState = maSettings[gridTimeframe].map(s => ({ ...s, visible: false }));
                              newState.forEach((s, i) => updateMaSetting(gridTimeframe, i, { visible: false }));
                            } else {
                              resetMaSettings(gridTimeframe);
                            }
                          }}
                        >
                          <span className="popover-item-label">MA一括表示切替</span>
                          <span className="popover-status">
                            {maSettings[gridTimeframe].some(s => s.visible) ? "ON" : "OFF"}
                          </span>
                        </button>
                      </div>
                    </div>
                  )}
                </div>
                <div className="popover-anchor">
                  <IconButton
                    icon={<IconFilter size={18} />}
                    label="フィルタ"
                    selected={hasActiveFilterChips}
                    variant="iconLabel"
                    onClick={handleOpenTechFilter}
                  />
                </div>
                <div className="txt-update-group">
                  <IconButton
                    icon={<IconRefresh size={18} />}
                    label="日次更新"
                    variant="iconLabel"
                    tooltip="日次更新（TXT/Phase/解析補完）"
                    ariaLabel="日次更新"
                    className="txt-update-button"
                    onClick={handleUpdateTxt}
                    disabled={!backendReady}
                  />
                  {txtUpdateCanCancel && (
                    <IconButton
                      icon={<IconPlayerStop size={18} />}
                      label="停止"
                      variant="iconLabel"
                      tooltip="日次更新を停止"
                      ariaLabel="日次更新を停止"
                      className="txt-update-button"
                      onClick={handleCancelTxtUpdate}
                      disabled={!backendReady || !txtUpdateCanCancel}
                    />
                  )}
                  {txtUpdateStatusLabel && (
                    <div className="txt-update-meta" title={txtUpdateJob?.message ?? undefined}>
                      <div className={`txt-update-status ${txtUpdateStatusTone}`}>
                        <span className="txt-update-dot" />
                        <span>{txtUpdateStatusLabel}</span>
                        {txtUpdateProgressValue != null && (
                          <span className="txt-update-percent">{txtUpdateProgressValue}%</span>
                        )}
                      </div>
                      <div className="txt-update-detail">{txtUpdateStageLabel}</div>
                      {txtUpdateShortDetail && (
                        <div className="txt-update-last">{txtUpdateShortDetail}</div>
                      )}
                      <div
                        className={`txt-update-progress ${txtUpdateStatusTone} ${
                          txtUpdateProgressValue == null ? "is-indeterminate" : ""
                        }`}
                      >
                        <div
                          className="txt-update-progress-bar"
                          style={{ width: `${txtUpdateProgressValue ?? 42}%` }}
                        />
                      </div>
                    </div>
                  )}
                </div>
                <div className="popover-anchor" ref={settingsRef}>
                  <IconButton
                    icon={<IconSettings size={18} />}
                    tooltip="設定"
                    ariaLabel="設定メニューを開く"
                    selected={settingsOpen && settingsPanelMode === "general"}
                    onClick={() => {
                      const alreadyOpen = settingsOpen && settingsPanelMode === "general";
                      setSettingsPanelMode("general");
                      setSettingsDetailsOpen(false);
                      setSettingsOpen(!alreadyOpen);
                      setSortOpen(false);
                      setDisplayOpen(false);
                      setSectorSortOpen(false);
                    }}
                  />
                  {settingsOpen && (
                    <div
                      className="popover-panel popover-right-aligned settings-popover-panel"
                      style={{ right: 0, maxHeight: "calc(100vh - 96px)", overflowY: "auto" }}
                    >
                      {settingsPanelMode === "general" && (
                        <>
                          <div className="popover-section">
                            <div className="popover-title">外観設定</div>
                            <div className="segmented">
                              <button
                                className={currentTheme === "dark" ? "active" : ""}
                                onClick={() => currentTheme !== "dark" && handleThemeToggle()}
                              >
                                <IconMoon size={16} />
                                <span>ダーク</span>
                              </button>
                              <button
                                className={currentTheme === "light" ? "active" : ""}
                                onClick={() => currentTheme !== "light" && handleThemeToggle()}
                              >
                                <IconSun size={16} />
                                <span>ライト</span>
                              </button>
                            </div>
                          </div>
                          <div className="popover-section settings-detail-toggle-section">
                            <div className="popover-title">詳細設定 / 運用</div>
                            <div className="popover-input-row">
                              <button
                                type="button"
                                className={`popover-item ${settingsDetailsOpen ? "active" : ""}`}
                                onClick={() => setSettingsDetailsOpen((current) => !current)}
                              >
                                <span className="popover-item-label">
                                  <IconRefresh size={16} />
                                  <span>{settingsDetailsOpen ? "折りたたむ" : "展開する"}</span>
                                </span>
                                <span className="popover-status">二段目</span>
                              </button>
                              <button
                                type="button"
                                className="popover-item"
                                onClick={() => {
                                  setSettingsPanelMode("walkforward");
                                  setSettingsDetailsOpen(true);
                                }}
                              >
                                <span className="popover-item-label">
                                  <IconFileText size={16} />
                                  <span>ウォークフォワード</span>
                                </span>
                                <span className="popover-status">詳細</span>
                              </button>
                            </div>
                          </div>
                          {settingsDetailsOpen && (
                            <>
                              <div className="popover-section">
                                <div className="popover-title">取引CSV</div>
                                <button
                                  type="button"
                                  className="popover-item"
                                  onClick={handleTradeCsvPick}
                                  disabled={tradeUploadInFlight}
                                >
                                  <span className="popover-item-label">
                                    <IconUpload size={16} />
                                    <span>{tradeUploadInFlight ? "取り込み中..." : "CSV取り込み"}</span>
                                  </span>
                                  <span className="popover-status">手動</span>
                                </button>
                                <button
                                  type="button"
                                  className="popover-item"
                                  onClick={handleForceTradeSync}
                                  disabled={tradeSyncInFlight}
                                >
                                  <span className="popover-item-label">
                                    <IconRefresh size={16} />
                                    <span>{tradeSyncInFlight ? "同期中..." : "強制同期(全件取込)"}</span>
                                  </span>
                                  <span className="popover-status">強制</span>
                                </button>
                                <div className="popover-hint">
                                  保存先: %LOCALAPPDATA%\\MeeMeeScreener\\data\\
                                </div>
                              </div>
                              <div className="popover-section">
                                <div className="popover-title">MM_DATA_DIR</div>
                                <div className="popover-input-row">
                                  <input
                                    type="text"
                                    className="popover-input"
                                    placeholder="%LOCALAPPDATA%\\MeeMeeScreener\\data"
                                    value={dataDirInput}
                                    onChange={(event) => setDataDirInput(event.target.value)}
                                  />
                                  <button
                                    type="button"
                                    className="popover-item"
                                    disabled={dataDirSaving}
                                    onClick={handleDataDirSave}
                                  >
                                    {dataDirSaving ? "保存中..." : "保存"}
                                  </button>
                                </div>
                                <div className="popover-hint">
                                  現在: {dataDir || (dataDirLoading ? "読み込み中..." : "未設定")}
                                </div>
                                {dataDirMessage && (
                                  <div className="popover-hint">{dataDirMessage}</div>
                                )}
                              </div>
                              <div className="popover-section">
                                <div className="popover-title">TXT参照フォルダ</div>
                                <div className="popover-hint">
                                  現在: {health?.pan_out_txt_dir ?? "未取得"}
                                </div>
                              </div>
                              <div className="popover-section">
                                <div className="popover-title">Phase</div>
                                <button
                                  type="button"
                                  className="popover-item"
                                  onClick={handlePhaseRebuild}
                                  disabled={!backendReady}
                                >
                                  <span className="popover-item-label">
                                    <IconFileText size={16} />
                                    <span>{"Phase\u518d\u8a08\u7b97"}</span>
                                  </span>
                                  <span className="popover-status">{"\u624b\u52d5"}</span>
                                </button>
                                <div className="popover-hint">{"\u901a\u5e38\u306f\u300c\u65e5\u6b21\u66f4\u65b0\u300d\u3067\u81ea\u52d5\u5b9f\u884c\u3055\u308c\u307e\u3059\u3002"}</div>
                              </div>
                              <div className="popover-section">
                                <div className="popover-title">売買判定キャッシュ</div>
                                <button
                                  type="button"
                                  className="popover-item"
                                  onClick={handleAnalysisBatchPrewarm}
                                  disabled={!backendReady || analysisBatchSubmitting}
                                >
                                  <span className="popover-item-label">
                                    <IconRefresh size={16} />
                                    <span>{analysisBatchSubmitting ? "起動中..." : "最新判定を一括計算"}</span>
                                  </span>
                                  <span className="popover-status">手動</span>
                                </button>
                                <div className="popover-hint">
                                  最新営業日の ML/売り判定を全銘柄分まとめて再計算し、次回表示を速くします。
                                </div>
                              </div>
                            </>
                          )}
                        </>
                      )}
                      {settingsPanelMode === "walkforward" && (
                        <div className="popover-section">
                          <div className="popover-title">ウォークフォワード検証</div>
                          <div className="popover-title" style={{ marginTop: 6 }}>プリセット</div>
                          <div className="popover-input-row" style={{ marginTop: 6 }}>
                            <input
                              type="text"
                              className="popover-input"
                              placeholder="例: 地合いあり_2026Q1"
                              value={walkforwardPresetName}
                              onChange={(event) => setWalkforwardPresetName(event.target.value)}
                            />
                          </div>
                          <div className="popover-input-row" style={{ marginTop: 6 }}>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleSaveWalkforwardPreset}
                            >
                              保存
                            </button>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleLoadWalkforwardPreset}
                              disabled={!matchedWalkforwardPreset}
                            >
                              読込
                            </button>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleDeleteWalkforwardPreset}
                              disabled={!matchedWalkforwardPreset}
                            >
                              削除
                            </button>
                          </div>
                          <div className="popover-input-row" style={{ marginTop: 6 }}>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleExportWalkforwardPresets}
                            >
                              書き出し
                            </button>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handlePickWalkforwardPresetImport}
                              disabled={walkforwardPresetImporting}
                            >
                              {walkforwardPresetImporting ? "読込中..." : "読み込み"}
                            </button>
                          </div>
                          {walkforwardPresets.length > 0 && (
                            <div className="popover-hint" style={{ marginTop: 6 }}>
                              最近のプリセット:
                              <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                                {walkforwardPresets.slice(0, 8).map((preset) => (
                                  <button
                                    key={preset.name}
                                    type="button"
                                    className={`popover-item ${normalizedWalkforwardPresetName.toLowerCase() ===
                                        preset.name.toLowerCase()
                                        ? "active"
                                        : ""
                                      }`}
                                    style={{ width: "auto", padding: "4px 8px" }}
                                    onClick={() => setWalkforwardPresetName(preset.name)}
                                  >
                                    <span className="popover-item-label">{preset.name}</span>
                                  </button>
                                ))}
                              </div>
                            </div>
                          )}
                          <div className="popover-title" style={{ marginTop: 10 }}>検証期間設定</div>
                          <div className="popover-input-row" style={{ marginTop: 6 }}>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={applyWalkforwardTenYearPreset}
                            >
                              10年単位(120/12/12)
                            </button>
                          </div>
                          <div className="popover-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 6 }}>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>学習期間(月)</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.trainMonths}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    trainMonths: Math.max(1, Number(event.target.value) || 1)
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>検証期間(月)</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.testMonths}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    testMonths: Math.max(1, Number(event.target.value) || 1)
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>ずらし幅(月)</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.stepMonths}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    stepMonths: Math.max(1, Number(event.target.value) || 1)
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>最小検証窓数</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.minWindows}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    minWindows: Math.max(1, Number(event.target.value) || 1)
                                  }))
                                }
                              />
                            </label>
                          </div>
                          <div className="popover-input-row" style={{ marginTop: 8 }}>
                            <label className="popover-hint" style={{ display: "grid", gap: 4, flex: 1 }}>
                              <span>対象銘柄数上限</span>
                              <input
                                type="number"
                                min={20}
                                className="popover-input"
                                value={walkforwardParams.maxCodes}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    maxCodes: Math.max(20, Number(event.target.value) || 20)
                                  }))
                                }
                              />
                            </label>
                          </div>
                          <div className="popover-title" style={{ marginTop: 10 }}>戦略条件</div>
                          <div className="segmented" style={{ marginTop: 6 }}>
                            {(["both", "long", "short"] as const).map((side) => (
                              <button
                                key={side}
                                type="button"
                                className={walkforwardParams.allowedSides === side ? "active" : ""}
                                onClick={() => {
                                  setWalkforwardParams((prev) => ({ ...prev, allowedSides: side }));
                                }}
                              >
                                {side === "both" ? "両建て" : side === "long" ? "買いのみ" : "売りのみ"}
                              </button>
                            ))}
                          </div>
                          <div className="popover-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 6, marginTop: 8 }}>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>買いスコア下限</span>
                              <input
                                type="number"
                                step={0.1}
                                className="popover-input"
                                value={walkforwardParams.minLongScore}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    minLongScore: Number(event.target.value) || 0
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>売りスコア下限</span>
                              <input
                                type="number"
                                step={0.1}
                                className="popover-input"
                                value={walkforwardParams.minShortScore}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    minShortScore: Number(event.target.value) || 0
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>1日新規上限</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.maxNewEntriesPerDay}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    maxNewEntriesPerDay: Math.max(1, Number(event.target.value) || 1)
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>1か月新規上限(任意)</span>
                              <input
                                type="number"
                                min={1}
                                className="popover-input"
                                value={walkforwardParams.maxNewEntriesPerMonth}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    maxNewEntriesPerMonth: event.target.value
                                  }))
                                }
                              />
                            </label>
                            <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                              <span>ML買い確率下限(任意)</span>
                              <input
                                type="number"
                                step={0.01}
                                min={0}
                                max={1}
                                className="popover-input"
                                value={walkforwardParams.minMlPUpLong}
                                onChange={(event) =>
                                  setWalkforwardParams((prev) => ({
                                    ...prev,
                                    minMlPUpLong: event.target.value
                                  }))
                                }
                              />
                            </label>
                          </div>
                          <label
                            className="popover-hint"
                            style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}
                          >
                            <input
                              type="checkbox"
                              checked={walkforwardParams.useRegimeFilter}
                              onChange={(event) =>
                                setWalkforwardParams((prev) => ({
                                  ...prev,
                                  useRegimeFilter: event.target.checked
                                }))
                              }
                            />
                            <span>レジームフィルタを使う</span>
                          </label>
                          {walkforwardParams.useRegimeFilter && (
                            <div className="popover-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 6, marginTop: 8 }}>
                              <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                                <span>地合い参照日数</span>
                                <input
                                  type="number"
                                  min={1}
                                  className="popover-input"
                                  value={walkforwardParams.regimeBreadthLookbackDays}
                                  onChange={(event) =>
                                    setWalkforwardParams((prev) => ({
                                      ...prev,
                                      regimeBreadthLookbackDays: Math.max(1, Number(event.target.value) || 1)
                                    }))
                                  }
                                />
                              </label>
                              <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                                <span>買い許可しきい値(任意)</span>
                                <input
                                  type="number"
                                  step={0.01}
                                  min={0}
                                  max={1}
                                  className="popover-input"
                                  value={walkforwardParams.regimeLongMinBreadthAbove60}
                                  onChange={(event) =>
                                    setWalkforwardParams((prev) => ({
                                      ...prev,
                                      regimeLongMinBreadthAbove60: event.target.value
                                    }))
                                  }
                                />
                              </label>
                              <label className="popover-hint" style={{ display: "grid", gap: 4 }}>
                                <span>売り許可しきい値(任意)</span>
                                <input
                                  type="number"
                                  step={0.01}
                                  min={0}
                                  max={1}
                                  className="popover-input"
                                  value={walkforwardParams.regimeShortMaxBreadthAbove60}
                                  onChange={(event) =>
                                    setWalkforwardParams((prev) => ({
                                      ...prev,
                                      regimeShortMaxBreadthAbove60: event.target.value
                                    }))
                                  }
                                />
                              </label>
                            </div>
                          )}
                          <label className="popover-hint" style={{ display: "grid", gap: 4, marginTop: 8 }}>
                            <span>買いセットアップ制限(任意, カンマ区切り)</span>
                            <input
                              type="text"
                              className="popover-input"
                              placeholder="例: long_pullback_p3,long_breakout_p2"
                              value={walkforwardParams.allowedLongSetups}
                              onChange={(event) =>
                                setWalkforwardParams((prev) => ({
                                  ...prev,
                                  allowedLongSetups: event.target.value
                                }))
                              }
                            />
                          </label>
                          <label className="popover-hint" style={{ display: "grid", gap: 4, marginTop: 8 }}>
                            <span>売りセットアップ制限(任意, カンマ区切り)</span>
                            <input
                              type="text"
                              className="popover-input"
                              placeholder="例: short_downtrend_p4,short_crash_top_p3"
                              value={walkforwardParams.allowedShortSetups}
                              onChange={(event) =>
                                setWalkforwardParams((prev) => ({
                                  ...prev,
                                  allowedShortSetups: event.target.value
                                }))
                              }
                            />
                          </label>
                          <div className="popover-input-row" style={{ marginTop: 8 }}>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleRunWalkforward}
                              disabled={!backendReady || walkforwardSubmitting}
                            >
                              {walkforwardSubmitting ? "起動中..." : "検証を実行"}
                            </button>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={() => {
                                void fetchLatestWalkforward(false);
                              }}
                              disabled={walkforwardLoading}
                            >
                              {walkforwardLoading ? "読込中..." : "最新結果"}
                            </button>
                          </div>
                          {walkforwardLatest && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              最終実行: {walkforwardLatest.run_id ?? "--"}
                              {walkforwardLatest.finished_at ? ` / ${String(walkforwardLatest.finished_at).replace("T", " ").slice(0, 19)}` : ""}
                            </div>
                          )}
                          {walkforwardSummary && (
                            <div className="popover-hint" style={{ marginTop: 8, display: "grid", gap: 2 }}>
                              <div>実行窓: {walkforwardSummary.executed_windows ?? 0}/{walkforwardSummary.windows_total ?? 0}</div>
                              <div>検証期間 勝率: {formatRate(walkforwardSummary.oos_weighted_win_rate)}</div>
                              <div>検証期間 取引数: {walkforwardSummary.oos_trade_events ?? 0}</div>
                              <div>検証期間 実現損益: {formatSigned(walkforwardSummary.oos_total_realized_unit_pnl)}</div>
                              <div>最大ドローダウン: {formatSigned(walkforwardSummary.oos_worst_max_drawdown_unit)}</div>
                              <div>PF平均: {formatSigned(walkforwardSummary.oos_mean_profit_factor)}</div>
                              <div>勝ち窓比率: {formatRate(walkforwardSummary.oos_positive_window_ratio)}</div>
                            </div>
                          )}
                          {walkforwardTopWindows.length > 0 && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              直近窓:
                              {walkforwardTopWindows.map((row) => {
                                const metrics = row.test?.metrics;
                                return (
                                  <div key={`${row.index}:${row.label}`} style={{ marginTop: 2 }}>
                                    {row.label ?? `窓 ${row.index ?? "?"}`} / 取引 {metrics?.trade_events ?? 0} / 勝率 {formatRate(metrics?.win_rate)} / 損益 {formatSigned(metrics?.total_realized_unit_pnl)}
                                  </div>
                                );
                              })}
                            </div>
                          )}
                          {(walkforwardAttributionCode.top.length > 0 || walkforwardAttributionCode.bottom.length > 0) && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              銘柄寄与:
                              {walkforwardAttributionCode.top.map((row) => (
                                <div key={`code-top-${row.key}`} style={{ marginTop: 2 }}>
                                  上位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                              {walkforwardAttributionCode.bottom.map((row) => (
                                <div key={`code-bottom-${row.key}`} style={{ marginTop: 2 }}>
                                  下位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                            </div>
                          )}
                          {(walkforwardAttributionSetup.top.length > 0 || walkforwardAttributionSetup.bottom.length > 0) && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              セットアップ寄与:
                              {walkforwardAttributionSetup.top.map((row) => (
                                <div key={`setup-top-${row.key}`} style={{ marginTop: 2 }}>
                                  上位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                              {walkforwardAttributionSetup.bottom.map((row) => (
                                <div key={`setup-bottom-${row.key}`} style={{ marginTop: 2 }}>
                                  下位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                            </div>
                          )}
                          {(walkforwardAttributionSector.top.length > 0 || walkforwardAttributionSector.bottom.length > 0) && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              業種寄与:
                              {walkforwardAttributionSector.top.map((row) => (
                                <div key={`sector-top-${row.key}`} style={{ marginTop: 2 }}>
                                  上位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                              {walkforwardAttributionSector.bottom.map((row) => (
                                <div key={`sector-bottom-${row.key}`} style={{ marginTop: 2 }}>
                                  下位 {row.key ?? "--"} / 取引 {row.trades ?? 0} / 勝率 {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                            </div>
                          )}
                          {walkforwardResearchLatest && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              研究スナップショット: {formatCompactDate(walkforwardResearchLatest.snapshot_date)} / run{" "}
                              {walkforwardResearchLatest.source_run_id ?? "--"}
                            </div>
                          )}
                          {walkforwardResearchAdoptedSetups.length > 0 && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              採用セットアップ:
                              {walkforwardResearchAdoptedSetups.map((row) => (
                                <div key={`research-setup-${row.setup_id}`} style={{ marginTop: 2 }}>
                                  {row.setup_id ?? "--"} / 取引 {row.trades ?? 0} / 勝率{" "}
                                  {formatRate(row.win_rate)} / 損益 {formatSigned(row.ret_net_sum)}
                                </div>
                              ))}
                            </div>
                          )}
                          {walkforwardResearchRejectedReasons.length > 0 && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              非採用理由:
                              {walkforwardResearchRejectedReasons.map((row) => (
                                <div key={`research-reason-${row.reason}`} style={{ marginTop: 2 }}>
                                  {row.reason ?? "--"} / 件数 {row.count ?? 0}
                                </div>
                              ))}
                            </div>
                          )}
                          {walkforwardResearchHedgeContribution && (
                            <div className="popover-hint" style={{ marginTop: 8 }}>
                              ヘッジ寄与:
                              <div style={{ marginTop: 2 }}>
                                core {formatSigned(walkforwardResearchHedgeContribution.core_ret_net_sum)} / hedge{" "}
                                {formatSigned(walkforwardResearchHedgeContribution.hedge_ret_net_sum)} / total{" "}
                                {formatSigned(walkforwardResearchHedgeContribution.total_ret_net_sum)}
                              </div>
                              <div style={{ marginTop: 2 }}>
                                hedge share {formatRate(walkforwardResearchHedgeContribution.hedge_share)}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                      {settingsPanelMode === "general" && (
                        <>
                          <div className="popover-section">
                            <div className="popover-title">スクショ</div>
                            <div className="popover-hint">
                              保存先: %USERPROFILE%\\Downloads\\MeeMeeScreener
                            </div>
                          </div>
                          <div className="popover-section">
                            <div className="popover-title">銘柄一覧</div>
                            <button
                              type="button"
                              className="popover-item"
                              onClick={handleExportWatchlist}
                              disabled={watchlistExporting}
                            >
                              <span className="popover-item-label">
                                <IconDownload size={16} />
                                <span>{watchlistExporting ? "書き出し中..." : "書き出し"}</span>
                              </span>
                              <span className="popover-status">BK</span>
                            </button>
                            <button type="button" className="popover-item" onClick={handleOpenCodeTxt}>
                              <span className="popover-item-label">
                                <IconFileText size={16} />
                                <span>code.txt</span>
                              </span>
                              <span className="popover-status">編集</span>
                            </button>
                          </div>
                          <div className="popover-section">
                            <div className="popover-title">イベント</div>
                            <button
                              type="button"
                              className="popover-item"
                              disabled={eventsRefreshing}
                              onClick={() => {
                                void refreshEvents();
                                setSettingsOpen(false);
                              }}
                            >
                              <span className="popover-item-label">
                                <IconRefresh size={16} />
                                <span>
                                  {eventsRefreshing ? "更新中..." : "イベント更新"}
                                </span>
                              </span>
                              <span className="popover-status">手動</span>
                            </button>
                            <div className="popover-hint">
                              状態: {eventsRefreshing ? "更新中" : "待機中"}
                            </div>
                            <div className="popover-hint">
                              最終試行: {eventsAttemptLabel ?? "--"}
                            </div>
                            {eventsLastError && (
                              <div className="popover-hint">エラー: {eventsLastError}</div>
                            )}
                          </div>
                        </>
                      )}
                      <div
                        className="popover-hint"
                        style={{
                          borderTop: "1px solid var(--theme-border-subtle)",
                          marginTop: 4,
                          paddingTop: 8,
                          textAlign: "right"
                        }}
                      >
                        {APP_VERSION_LABEL}
                      </div>
                    </div>
                  )}
                  <input
                    ref={tradeCsvInputRef}
                    type="file"
                    accept=".csv"
                    onChange={handleTradeCsvChange}
                    style={{ display: "none" }}
                  />
                  <input
                    ref={walkforwardPresetImportInputRef}
                    type="file"
                    accept=".json,application/json"
                    onChange={handleImportWalkforwardPresetFile}
                    style={{ display: "none" }}
                  />
                </div>
              </div>
            </div>
          </div>
          <div className="header-row-bottom">
            <div className="segmented list-timeframe">
              {(["monthly", "weekly", "daily"] as const).map((frame) => (
                <button
                  key={frame}
                  type="button"
                  className={gridTimeframe === frame ? "active" : ""}
                  onClick={() => setGridTimeframe(frame)}
                >
                  {frame === "daily"
                    ? "日足"
                    : frame === "weekly"
                      ? "週足"
                      : "月足"}
                </button>
              ))}
            </div>
            <div className="grid-preset-summary" aria-label="グリッド表示本数">
              <span className="grid-preset-summary-label">{gridPresetLabel}</span>
              <span className="grid-preset-summary-value">{listRangeBars}本</span>
            </div>
            {activeSectorParam && (
              <div className="sector-filter-chip">
                <span>セクター: {sectorLabel ?? activeSectorParam}</span>
                <button type="button" onClick={clearSectorFilter}>
                  解除
                </button>
              </div>
            )}

            {hasMeaningfulSectorOptions && (
              <div className="sector-sort-button" style={{ marginRight: 8, position: "relative" }} ref={sectorSortRef}>
                <IconButton
                  icon={<IconBuildingArch size={18} />}
                  label={sectorLabel ? sectorLabel : "業種"}
                  variant="iconLabel"
                  tooltip="業種で絞り込み / 並び替え"
                  selected={sectorSortOpen || !!activeSectorParam}
                  onClick={() => {
                    setSectorSortOpen(!sectorSortOpen);
                    setSortOpen(false);
                    setDisplayOpen(false);
                    setSettingsOpen(false);
                  }}
                />
                {sectorSortOpen && (
                  <div className="popover-panel sector-sort-popover-panel">
                    <div className="popover-section">
                      <div className="popover-title sector-sort-title">
                        <span>業種で絞り込み</span>
                        {activeSectorParam && (
                          <button
                            type="button"
                            className="text-button sector-sort-reset"
                            onClick={() => handleSectorSelect(null)}
                          >
                            解除
                          </button>
                        )}
                      </div>
                      <div className="popover-grid sector-sort-grid">
                        <button
                          type="button"
                          className={`popover-item ${!activeSectorParam ? "active" : ""}`}
                          onClick={() => handleSectorSelect(null)}
                        >
                          <span className="popover-item-label">すべて</span>
                        </button>
                        {availableSectors.map((sec) => (
                          <button
                            key={sec.code}
                            type="button"
                            className={`popover-item ${activeSectorParam === sec.code ? "active" : ""}`}
                            onClick={() => handleSectorSelect(sec.code)}
                          >
                            <span className="popover-item-label">{sec.name}</span>
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}

            <div className="search-field list-search">
              <input
                className="search-input"
                type="search"
                placeholder="コード / 銘柄名で検索"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
              {search && (
                <button type="button" className="search-clear" onClick={() => setSearch("")}>
                  クリア
                </button>
              )}
              {canAddWatchlist && (
                <button type="button" onClick={() => addKeep(canAddWatchlist)}>
                  +
                </button>
              )}
            </div>
            <div className="list-events-inline">
              <span className="event-meta-status">
                状態: {eventsRefreshing ? "更新中" : "待機中"}
              </span>
              {eventsLastError && (
                <span className="event-meta-error" title={eventsLastError}>
                  エラー: {eventsLastError}
                </span>
              )}
              <span className="event-meta-last">
                イベント最終更新: {eventsLastSuccessLabel ?? "--"}
              </span>
              {rightsCoverageLabel && (
                <span className="event-meta-rights">{rightsCoverageLabel}</span>
              )}
            </div>
          </div>
        </div>





        {hasActiveFilterChips && (
          <div className="tech-filter-chips-row">
            {techFilterActive.conditions.length > 0 && (
              <span className="tech-filter-chip">
                テクニカル {techFilterActive.conditions.length}条件 ({activeTimeframeLabel})
                <button type="button" onClick={handleOpenTechFilter}>
                  編集
                </button>
              </span>
            )}
            {techFilterActive.boxThisMonth && (
              <span className="tech-filter-chip">今月ボックス</span>
            )}
            {buyStateFilter === "initial" && (
              <span className="tech-filter-chip">
                初動のみ
                <button
                  type="button"
                  onClick={() => {
                    setBuyStateFilter("all");
                    setBuyStateFilterDraft("all");
                  }}
                >
                  ×
                </button>
              </span>
            )}
            {buyStateFilter === "base" && (
              <span className="tech-filter-chip">
                底がためのみ
                <button
                  type="button"
                  onClick={() => {
                    setBuyStateFilter("all");
                    setBuyStateFilterDraft("all");
                  }}
                >
                  ×
                </button>
              </span>
            )}
            {shortTierAbOnly && (
              <span className="tech-filter-chip">
                売りTier A/Bのみ
                <button
                  type="button"
                  onClick={() => {
                    setShortTierAbOnly(false);
                    setShortTierAbOnlyDraft(false);
                  }}
                >
                  ×
                </button>
              </span>
            )}
            {activeAnchorLabel && techFilterActive.conditions.length > 0 && (
              <span className="tech-filter-chip">基準日: {activeAnchorLabel}</span>
            )}
            <button type="button" className="tech-filter-chip-reset" onClick={handleClearAllActiveFilters}>
              すべて解除
            </button>
          </div>
        )}
      </header>
      {health && health.txt_count === 0 && (
        <div className="data-warning">
          TXTが見つかりません。PANROLLINGで出力したTXTめ
          {health.pan_out_txt_dir ? ` ${health.pan_out_txt_dir} ` : ""}
          に配置してください。
        </div>
      )}
      {health && health.code_txt_missing && health.txt_count > 0 && (
        <div className="data-warning subtle">
          code.txt がありません。ファイル名から銘柄コードを推定して表示します。code.txt 推奨です。
        </div>
      )}
      {listSnapshotMeta?.stale && (
        <div className="data-warning subtle">
          一覧は直近の成功スナップショットを表示しています。
          {listSnapshotMeta.updatedAt ? ` 更新時刻: ${listSnapshotMeta.updatedAt}` : ""}
          {listSnapshotMeta.lastError ? ` / 最新更新失敗: ${listSnapshotMeta.lastError}` : ""}
        </div>
      )}
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
        {!showSkeleton && sortedTickers.length === 0 && (
          <div className="grid-empty-state">
            <div className="grid-empty-title">
              {listLoadError ? "一覧の読み込みに失敗しました" : "表示対象の銘柄がありません"}
            </div>
            <div className="grid-empty-message">
              {listLoadError ??
                "条件に一致する銘柄が無いか、一覧データがまだ作成されていません。"}
            </div>
            <button type="button" className="chip" onClick={() => void loadList()}>
              再読み込み
            </button>
          </div>
        )}
        {!showSkeleton && size.width > 0 && sortedTickers.length > 0 && (
          <TradexListSummaryMount
            backendReady={backendReady}
            enabled={true}
            scope="grid-visible"
            items={tradexListSummaryItems}
          >
            {(tradexListSummaryState) => (
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
                        {(() => {
                          const anchorSource =
                            techFilterActive.conditions.length > 0
                              ? filterAnchorInfoByCode
                              : listAnchorInfoByCode;
                          const anchor = anchorSource.get(item.ticker.code);
                          const asofLabel =
                            shouldShowAsof && anchor?.asof ? formatDateYMD(anchor.time) : null;
                          const baseLabel =
                            techFilterActive.conditions.length > 0
                              ? activeAnchorLabel
                              : listAnchorLabel;
                          const asofTooltip = asofLabel
                            ? `基準日 ${baseLabel ?? "最新"} の足が無いので ${asofLabel} を使用`
                            : null;
                          const tradexSummary = tradexListSummaryState.itemsByKey[
                            buildTradexListSummaryKey(item.ticker.code, null)
                          ] ?? null;
                          return (
                            <StockTile
                              ticker={item.ticker}
                              timeframe={gridTimeframe}
                              maxBars={gridMaxBars}
                              active={activeCode === item.ticker.code}
                              kept={keepSet.has(item.ticker.code)}
                              compactHeader={compactTileHeader}
                              asofLabel={asofLabel}
                              asofTooltip={asofTooltip}
                              onActivate={activateByCode}
                              onOpenDetail={handleOpenDetail}
                              onToggleKeep={handleToggleKeep}
                              onExclude={handleExclude}
                              theme={currentTheme}
                              annotation={
                                <TradexListSummary
                                  summary={tradexSummary}
                                  loading={tradexListSummaryState.loading && !tradexSummary}
                                />
                              }
                            />
                          );
                        })()}
                      </div>
                    );
                  }}
                </Grid>
              </div>
            )}
          </TradexListSummaryMount>
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
                <div className="consult-placeholder">建玉情報がありません</div>
              )}
            </div>
          </div>
        )}
      </div>
      <GridIndicatorOverlay
        isOpen={showIndicators}
        maSettings={maSettings}
        onClose={() => setShowIndicators(false)}
        onUpdateSetting={updateSetting}
        onResetSettings={resetSettings}
      />
      <TechnicalFilterDrawer
        open={techFilterOpen}
        timeframe={techFilterDraft.defaultTimeframe}
        anchorLabel={draftAnchorLabel}
        matchCount={draftFilterResult.items.length}
        value={techFilterDraft}
        buyStateFilter={buyStateFilterDraft}
        shortTierAbOnly={shortTierAbOnlyDraft}
        onChange={setTechFilterDraft}
        onBuyStateFilterChange={setBuyStateFilterDraft}
        onShortTierAbOnlyChange={setShortTierAbOnlyDraft}
        onApply={handleApplyTechFilter}
        onCancel={handleCancelTechFilter}
        onReset={handleResetTechFilterDraft}
        onTimeframeChange={(next) => {
          setTechFilterDraft((prev) => ({ ...prev, defaultTimeframe: next }));
        }}
      />
      <Toast
        message={toastMessage?.text ?? null}
        onClose={() => {
          setToastMessage(null);
          setToastAction(null);
        }}
        action={toastAction}
        duration={toastAction ? 8000 : 4000}
      />
    </div>
  );
}





