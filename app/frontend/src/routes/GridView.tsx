import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
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
  describeCondition,
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

const GRID_GAP = 12;
const KP_LIMIT = 24;
type Timeframe = "monthly" | "weekly" | "daily";
type SortOption = { key: SortKey; label: string; fixedDirection?: SortDir };
type SortSection = { title: string; options: SortOption[] };

const rangeOptions = [
  { label: "60本", count: 60 },
  { label: "120本", count: 120 },
  { label: "240本", count: 240 },
  { label: "360本", count: 360 }
];
const gridRowOptions: Array<1 | 2 | 3 | 4 | 5 | 6> = [1, 2, 3, 4, 5, 6];
const gridColumnOptions: Array<1 | 2 | 3 | 4> = [1, 2, 3, 4];

const createDefaultTechFilter = (defaultTimeframe: Timeframe): TechnicalFilterState => ({
  defaultTimeframe,
  anchorMode: "latest",
  anchorDate: null,
  conditions: [],
  boxThisMonth: false
});

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
  pan_out_txt_dir?: string | null;
};

type JobStatusPayload = {
  id?: string;
  type?: string;
  status?: string;
  message?: string;
  error?: string | null;
};

type TxtUpdateJobState = {
  id: string;
  status: string;
  message: string | null;
};

const TERMINAL_JOB_STATUS = new Set(["success", "failed", "canceled"]);

const extractErrorDetail = (err: unknown, fallback = "不明なエラー"): string => {
  if (!err || typeof err !== "object") return fallback;
  const maybeErr = err as {
    message?: unknown;
    response?: {
      data?: {
        error?: unknown;
        detail?: unknown;
        message?: unknown;
      };
    };
  };
  const responseData = maybeErr.response?.data;
  if (typeof responseData?.error === "string" && responseData.error.trim()) return responseData.error;
  if (typeof responseData?.detail === "string" && responseData.detail.trim()) return responseData.detail;
  if (typeof responseData?.message === "string" && responseData.message.trim()) return responseData.message;
  if (typeof maybeErr.message === "string" && maybeErr.message.trim()) return maybeErr.message;
  return fallback;
};

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
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const keepList = useStore((state) => state.keepList);
  const addKeep = useStore((state) => state.addKeep);
  const removeKeep = useStore((state) => state.removeKeep);
  const setColumns = useStore((state) => state.setColumns);
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
  const setPerformancePeriod = useStore((state) => state.setPerformancePeriod);
  const maSettings = useStore((state) => state.maSettings);
  const updateMaSetting = useStore((state) => state.updateMaSetting);
  const resetMaSettings = useStore((state) => state.resetMaSettings);
  const eventsMeta = useStore((state) => state.eventsMeta);
  const refreshEvents = useStore((state) => state.refreshEvents);

  // Sector Sort Settings
  const sectorSortEnabled = useStore((state) => state.settings.sectorSortEnabled);
  const sectorSortInnerKey = useStore((state) => state.settings.sectorSortInnerKey);

  const eventsAttemptLabel = useMemo(
    () => formatEventDateYmd(eventsMeta?.lastAttemptAt),
    [eventsMeta?.lastAttemptAt]
  );
  const eventsLastSuccessLabel = useMemo(() => {
    const earningsMs = parseEventDateMs(eventsMeta?.earningsLastSuccessAt);
    const rightsMs = parseEventDateMs(eventsMeta?.rightsLastSuccessAt);
    const candidates = [
      { value: eventsMeta?.earningsLastSuccessAt ?? null, ms: earningsMs },
      { value: eventsMeta?.rightsLastSuccessAt ?? null, ms: rightsMs }
    ].filter((item) => item.value && item.ms != null) as { value: string; ms: number }[];
    if (!candidates.length) return null;
    const oldest = candidates.reduce((prev, next) => (next.ms < prev.ms ? next : prev));
    return formatEventDateYmd(oldest.value);
  }, [eventsMeta]);
  const rightsCoverageLabel = useMemo(() => {
    const rightsMaxDate = eventsMeta?.dataCoverage?.rightsMaxDate ?? null;
    const maxMs = parseEventDateMs(rightsMaxDate);
    if (!rightsMaxDate || maxMs == null) return null;
    const thresholdMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
    if (maxMs >= thresholdMs) return null;
    const formatted = formatEventDateYmd(rightsMaxDate);
    return formatted ? `権利データ範囲: ～${formatted}` : null;
  }, [eventsMeta]);

  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [showIndicators, setShowIndicators] = useState(false);
  const [sortOpen, setSortOpen] = useState(false);  // Candidate sort menu
  const [basicSortOpen, setBasicSortOpen] = useState(false);  // Basic sort menu
  const [displayOpen, setDisplayOpen] = useState(false);
  const [isSorting, setIsSorting] = useState(false);
  const [toastMessage, setToastMessage] = useState<{ text: string; key: number } | null>(null);
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
  const [dataDir, setDataDir] = useState("");
  const [dataDirInput, setDataDirInput] = useState("");
  const [dataDirLoading, setDataDirLoading] = useState(false);
  const [dataDirSaving, setDataDirSaving] = useState(false);
  const [dataDirMessage, setDataDirMessage] = useState<string | null>(null);
  const [currentTheme, setCurrentTheme] = useState<Theme>(() => getStoredTheme());
  const [tradeUploadInFlight, setTradeUploadInFlight] = useState(false);
  const [tradeSyncInFlight, setTradeSyncInFlight] = useState(false);
  const [txtUpdateJob, setTxtUpdateJob] = useState<TxtUpdateJobState | null>(null);
  const [txtUpdatePolling, setTxtUpdatePolling] = useState(false);
  const [watchlistExporting, setWatchlistExporting] = useState(false);
  const [techFilterOpen, setTechFilterOpen] = useState(false);
  const [techFilterDraft, setTechFilterDraft] = useState<TechnicalFilterState>(() =>
    createDefaultTechFilter(gridTimeframe)
  );
  const [techFilterActive, setTechFilterActive] = useState<TechnicalFilterState>(() =>
    createDefaultTechFilter(gridTimeframe)
  );
  const [sectorSortOpen, setSectorSortOpen] = useState(false); // Popover state for Sector Sort
  const sortRef = useRef<HTMLDivElement | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const settingsRef = useRef<HTMLDivElement | null>(null);
  const sectorSortRef = useRef<HTMLDivElement | null>(null); // Ref for Sector Sort Popover
  const techFilterDropNoticeRef = useRef(false);
  const gridRef = useRef<FixedSizeGrid | null>(null);
  const tradeCsvInputRef = useRef<HTMLInputElement | null>(null);
  const lastVisibleCodesRef = useRef<string[]>([]);
  const lastVisibleRangeRef = useRef<{ start: number; stop: number } | null>(null);
  const undoTimerRef = useRef<number | null>(null);
  const txtUpdateTerminalStatusRef = useRef<string | null>(null);


  const showToast = useCallback((text: string) => {
    toastKeyRef.current += 1;
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
    loadList();
  }, [backendReady, loadList]);

  // Derive available sectors from tickers
  const availableSectors = useMemo(() => {
    const map = new Map<string, string>();
    tickers.forEach((t) => {
      if (t.sector33Code && t.sector33Name) {
        map.set(t.sector33Code, t.sector33Name);
      }
    });
    const list = Array.from(map.entries()).map(([code, name]) => ({ code, name }));
    list.sort((a, b) => a.name.localeCompare(b.name, "ja"));
    return list;
  }, [tickers]);

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

  useEffect(() => {
    setIsSorting(true);
    const timer = window.setTimeout(() => setIsSorting(false), 120);
    return () => window.clearTimeout(timer);
  }, [sortKey, sortDir, sectorSortEnabled, sectorSortInnerKey]);

  // Candidate sort sections (shown only on candidate screens)
  const candidateSortSections = useMemo<SortSection[]>(
    () => [
      {
        title: "買い候補",
        options: [
          { key: "buyCandidate", label: "買い候補(総合)" },
          { key: "buyInitial", label: "買い候補(初動)" },
          { key: "buyBase", label: "買い候補(底がため)" },
        ]
      },
      {
        title: "売り候補",
        options: [
          { key: "shortScore", label: "売り候補(総合)" },
          { key: "aScore", label: "売り候補(反転確実)" },
          { key: "bScore", label: "売り候補(戻り売り)" },
        ]
      }
    ],
    []
  );

  // Basic sort sections (shown on non-candidate screens)
  const basicSortSections = useMemo<SortSection[]>(
    () => [
      {
        title: "基本",
        options: [
          { key: "code", label: "コード" },
          { key: "name", label: "銘柄名" },
          { key: "sector", label: "業種" }
        ]
      },
      {
        title: "テクニカル",
        options: [
          { key: "ma20Dev", label: "乖離率(MA20)" },
          { key: "ma60Dev", label: "乖離率(MA60)" },
          { key: "ma20Slope", label: "MA20傾き" },
          { key: "ma60Slope", label: "MA60傾き" },
        ]
      },
      {
        title: "パフォーマンス",
        options: [
          { key: "performance", label: "騰落率" }  // Period selected via dropdown
        ]
      },
      {
        title: "スコア",
        options: [
          { key: "upScore", label: "上昇スコア" },
          { key: "downScore", label: "下落スコア" },
          { key: "overheatUp", label: "過熱(上)" },
          { key: "overheatDown", label: "過熱(下)" },
        ]
      },
      {
        title: "ボックス",
        options: [{ key: "boxState", label: "ボックス状態" }]
      }
    ],
    []
  );

  // Legacy combined sortSections (for backward compatibility in sorting logic)
  const sortSections = useMemo<SortSection[]>(
    () => [...candidateSortSections, ...basicSortSections],
    [candidateSortSections, basicSortSections]
  );

  const sortOptions = useMemo(
    () => sortSections.flatMap((section) => section.options),
    [sortSections]
  );

  // Determine if current view is a candidate view
  const isCandidateView = useMemo(() => {
    // Check if sortKey is a candidate sort key
    const candidateKeys = ["buyCandidate", "buyInitial", "buyBase", "shortScore", "aScore", "bScore"];
    return candidateKeys.includes(sortKey);
  }, [sortKey]);

  const defaultSortLabel = "コード";
  const sortLabel = useMemo(
    () => sortOptions.find((option) => option.key === sortKey)?.label ?? defaultSortLabel,
    [sortOptions, sortKey]
  );

  const sortDirLabel = sortDir === "desc" ? "降順" : "昇順";
  const gridTimeframeLabel =
    gridTimeframe === "daily" ? "日足" : gridTimeframe === "weekly" ? "週足" : "月足";
  const txtUpdateCanCancel = Boolean(
    txtUpdateJob && txtUpdateJob.id && !TERMINAL_JOB_STATUS.has(txtUpdateJob.status)
  );
  const txtUpdateStatusLabel = useMemo(() => {
    if (!txtUpdateJob) return null;
    return formatTxtUpdateStatusLabel(txtUpdateJob.status);
  }, [txtUpdateJob]);

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
    if (!sectorParam) return searchFiltered;
    return searchFiltered.filter((item) => item.sector33Code === sectorParam);
  }, [searchFiltered, sectorParam]);

  const sectorLabel = useMemo(() => {
    if (!sectorParam) return null;
    const match = tickers.find(
      (item) => item.sector33Code === sectorParam && item.sector33Name
    );
    return match?.sector33Name ?? sectorParam;
  }, [sectorParam, tickers]);

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

    // Determine the active sort configuration
    const activeKey = sectorSortEnabled ? sectorSortInnerKey : sortKey;
    // For inner sort, we might want to respect `sortDir` only if it makes sense?
    // Actually, let's use `sortDir` for the inner sort direction.
    // The Sector Grouping itself is always Sector Code ASC. 
    // (Or should we allow reversing sectors? Standard is usually ASC).

    const isBuyCandidate =
      activeKey === "buyCandidate" || activeKey === "buyInitial" || activeKey === "buyBase";

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
      } else if (activeKey === "sector" && !sectorSortEnabled) { // Avoid using 'sector' as sort value if it's the grouping key, though it doesn't hurt.
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
      } else if (activeKey === "boxState") {
        const state = ticker.boxState ?? "NON";
        sortValue = boxOrder[state] ?? 0;
      } else if (activeKey === "shortScore") {
        sortValue = ticker.shortScore ?? null;
      } else if (activeKey === "aScore") {
        sortValue = ticker.aScore ?? null;
      } else if (activeKey === "bScore") {
        sortValue = ticker.bScore ?? null;
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
      // ... same logic as before, just using activeKey
      const aRank = Number.isFinite(a.ticker.buyStateRank) ? (a.ticker.buyStateRank as number) : 0;
      const bRank = Number.isFinite(b.ticker.buyStateRank) ? (b.ticker.buyStateRank as number) : 0;
      const aScore = Number.isFinite(a.ticker.buyStateScore) ? (a.ticker.buyStateScore as number) : null;
      const bScore = Number.isFinite(b.ticker.buyStateScore) ? (b.ticker.buyStateScore as number) : null;
      const aRisk = Number.isFinite(a.ticker.buyRiskDistance) ? (a.ticker.buyRiskDistance as number) : null;
      const bRisk = Number.isFinite(b.ticker.buyRiskDistance) ? (b.ticker.buyRiskDistance as number) : null;

      if (activeKey === "buyInitial" || activeKey === "buyBase") {
        const target = activeKey === "buyInitial" ? "初動" : "底がため";
        const aligible = aState === target;
        const bligible = bState === target;
        if (aligible !== bligible) return aligible ? -1 : 1;
        if (!aligible && !bligible) return a.ticker.code.localeCompare(b.ticker.code);
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
      // 1. Sector Sort (Grouping) if enabled
      if (sectorSortEnabled) {
        const sA = a.ticker.sector33Code;
        const sB = b.ticker.sector33Code;
        // Put null/empty sectors at the end
        if (sA && !sB) return -1;
        if (!sA && sB) return 1;
        if (sA && sB && sA !== sB) {
          // Compare sector codes (or names if code implies order? codes are usually ordered)
          // `sector33Code` is usually text "0050", "1050" etc.
          return sA.localeCompare(sB);
        }
        // If sectors are same or both missing, fall through to inner sort
      }

      // 2. Inner Sort (using activeKey)
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
  }, [
    scoredTickers,
    sortKey,
    sortDir,
    collator,
    barsCache,
    gridTimeframe,
    listAnchorInfoByCode,
    performancePeriod,
    sectorSortEnabled,
    sectorSortInnerKey
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
    ensureBarsForVisible(gridTimeframe, lastVisibleCodesRef.current, "timeframe-or-range-change");
  }, [backendReady, gridTimeframe, listRangeBars, maSettings, ensureBarsForVisible]);

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
        showToast("ウォッチリスト追加に失敗しました。");
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
        showToast(`${code} を除外しました。`);
      } catch (error) {
        const message = error instanceof Error ? error.message : "ウォッチリスト削除に失敗しました。";
        showToast(message);
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
      if (keepList.length >= KP_LIMIT) {
        showToast(`候補キープは最大 ${KP_LIMIT} 件です。`);
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
    setTechFilterOpen(true);
  };

  const handleApplyTechFilter = () => {
    const normalized = sanitizeTechFilterState(techFilterDraft, gridTimeframe);
    setTechFilterActive(normalized);
    setTechFilterDraft(normalized);
    setTechFilterOpen(false);
  };

  const handleCancelTechFilter = () => {
    setTechFilterDraft(techFilterActive);
    setTechFilterOpen(false);
  };

  const handleResetTechFilterDraft = () => {
    setTechFilterDraft(createDefaultTechFilter(techFilterDraft.defaultTimeframe));
  };

  const handleRemoveActiveCondition = (id: string) => {
    const next = {
      ...techFilterActive,
      conditions: techFilterActive.conditions.filter((item) => item.id !== id)
    };
    setTechFilterActive(next);
    if (!techFilterOpen) {
      setTechFilterDraft(next);
    }
  };

  const handleClearActiveFilters = () => {
    setTechFilterActive(createDefaultTechFilter(techFilterActive.defaultTimeframe));
    if (!techFilterOpen) {
      setTechFilterDraft(createDefaultTechFilter(techFilterDraft.defaultTimeframe));
    }
  };

  const handleUpdateError = useCallback((payload?: TxtUpdateStartPayload) => {
    const error = payload?.error ?? 'unknown';
    if (isTxtUpdateConflictError(error) || payload?.status === "conflict") {
      showToast("TXT更新は既に実行中です。");
      return;
    }
    if (error === 'code_txt_missing') {
      showToast("code.txt が見つかりません。");
      return;
    }
    if (error.startsWith('vbs_not_found')) {
      showToast("TXT更新スクリプトが見つかりません。");
      return;
    }
    showToast("TXT更新の起動に失敗しました。");
  }, [showToast]);

  const applyTxtUpdateStatus = useCallback((payload?: JobStatusPayload | null) => {
    if (!payload || typeof payload.id !== "string" || !payload.id) return;
    const nextStatus = typeof payload.status === "string" ? payload.status : "running";
    const nextMessage = typeof payload.message === "string" ? payload.message : null;
    setTxtUpdateJob({ id: payload.id, status: nextStatus, message: nextMessage });

    if (!TERMINAL_JOB_STATUS.has(nextStatus)) {
      setTxtUpdatePolling(true);
      return;
    }

    setTxtUpdatePolling(false);
    const terminalKey = `${payload.id}:${nextStatus}`;
    if (txtUpdateTerminalStatusRef.current === terminalKey) return;
    txtUpdateTerminalStatusRef.current = terminalKey;

    if (nextStatus === "success") {
      showToast("TXT更新が完了しました。");
      return;
    }
    if (nextStatus === "canceled") {
      showToast("TXT更新をキャンセルしました。");
      return;
    }
    const detail = payload.error || payload.message || "詳細不明";
    showToast(`TXT更新が失敗しました。(${detail})`);
  }, [showToast]);

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
  }, [consultText]);

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = keepList.slice(0, limit);
    const extra = Math.max(0, keepList.length - visible.length);
    return { visible, extra };
  }, [keepList]);

  const handleUpdateTxt = useCallback(async () => {
    if (!backendReady) return;
    showToast("TXT更新を開始しました。");
    try {
      const res = await api.post("/jobs/txt-update");
      const payload = (res.data ?? {}) as TxtUpdateStartPayload;
      if (payload.ok === false) {
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
        txtUpdateTerminalStatusRef.current = null;
        setTxtUpdateJob({
          id: jobId,
          status: "queued",
          message: "Waiting in queue..."
        });
        setTxtUpdatePolling(true);
      }
    } catch (error) {
      let payload: TxtUpdateStartPayload | null = null;
      if (typeof error === "object" && error && "response" in error) {
        const response = (error as { response?: { data?: TxtUpdateStartPayload } }).response;
        payload = response?.data ?? null;
      }
      if (payload) {
        handleUpdateError(payload);
      } else {
        showToast("TXT更新の起動に失敗しました。");
      }
    }
  }, [backendReady, handleUpdateError, applyTxtUpdateStatus]);

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
        showToast("TXT更新のキャンセルを要求しました。");
      } else {
        setTxtUpdatePolling(false);
        showToast("TXT更新は既に終了しています。");
      }
    } catch (err) {
      const detail = extractErrorDetail(err);
      showToast(`TXT更新のキャンセルに失敗しました。(${detail})`);
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
  }, [backendReady]);


  return (
    <div className="app-shell list-view">
      <header className="unified-list-header">
        <div className="list-header-row">
          <div className="header-row-top">
            <div className="header-row-left">
              <div className="app-brand">
                <div className="app-brand-title">MeeMee</div>
                <div className="app-brand-sub">Screener</div>
              </div>
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
                    <div className="popover-panel">
                      {(isCandidateView ? candidateSortSections : sortSections).map((section) => (
                        <div className="popover-section" key={section.title}>
                          <div className="popover-title">{section.title}</div>
                          <div className="popover-grid">
                            {section.options.map((opt) => (
                              <button
                                key={opt.key}
                                type="button"
                                className={`popover-item ${sortKey === opt.key ? "active" : ""}`}
                                onClick={() => {
                                  if (opt.fixedDirection) {
                                    setSortKey(opt.key);
                                    setSortDir(opt.fixedDirection); // Use fixed direction
                                  } else if (sortKey === opt.key) {
                                    setSortDir(sortDir === "asc" ? "desc" : "asc");
                                  } else {
                                    setSortKey(opt.key);
                                    setSortDir("desc");
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
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div className="popover-anchor" ref={displayRef}>
                  <IconButton
                    icon={<IconLayoutGrid size={18} />}
                    label="表示"
                    variant="iconLabel"
                    tooltip="表示設定"
                    ariaLabel="表示設定メニューを開く"
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
                        <div className="popover-title">行数</div>
                        <div className="segmented">
                          {gridRowOptions.map((r) => (
                            <button
                              key={r}
                              className={rows === r ? "active" : ""}
                              onClick={() => setRows(r)}
                            >
                              {r}
                            </button>
                          ))}
                        </div>
                      </div>
                      <div className="popover-section">
                        <div className="popover-title">列数</div>
                        <div className="segmented">
                          {gridColumnOptions.map((c) => (
                            <button
                              key={c}
                              className={columns === c ? "active" : ""}
                              onClick={() => setColumns(c)}
                            >
                              {c}
                            </button>
                          ))}
                        </div>
                      </div>
                      <div className="popover-section">
                        <button
                          className="popover-item"
                          onClick={() => {
                            setRows(3);
                            setColumns(3);
                            setDisplayOpen(false);
                          }}
                        >
                          <span className="popover-item-label">3x3に戻す</span>
                        </button>
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
                    selected={hasActiveFilters}
                    variant="iconLabel"
                    onClick={() => setTechFilterOpen(true)}
                  />
                </div>
                <div className="txt-update-group">
                  <IconButton
                    icon={<IconRefresh size={18} />}
                    label="TXT更新"
                    variant="iconLabel"
                    tooltip="TXT更新"
                    ariaLabel="TXT更新"
                    className="txt-update-button"
                    onClick={handleUpdateTxt}
                    disabled={!backendReady}
                  />
                  {txtUpdateCanCancel && (
                    <IconButton
                      icon={<IconPlayerStop size={18} />}
                      label="停止"
                      variant="iconLabel"
                      tooltip="TXT更新を停止"
                      ariaLabel="TXT更新を停止"
                      className="txt-update-button"
                      onClick={handleCancelTxtUpdate}
                      disabled={!backendReady || !txtUpdateCanCancel}
                    />
                  )}
                  {txtUpdateStatusLabel && (
                    <span
                      style={{
                        marginLeft: 8,
                        fontSize: 12,
                        color: "var(--theme-text-secondary)",
                        whiteSpace: "nowrap"
                      }}
                      title={txtUpdateJob?.message ?? undefined}
                    >
                      {txtUpdateStatusLabel}
                    </span>
                  )}
                </div>
                <div className="popover-anchor" ref={settingsRef}>
                  <IconButton
                    icon={<IconSettings size={18} />}
                    tooltip="設定"
                    ariaLabel="設定メニューを開く"
                    onClick={() => {
                      setSettingsOpen(!settingsOpen);
                      setSortOpen(false);
                      setDisplayOpen(false);
                      setSectorSortOpen(false);
                    }}
                  />
                  {settingsOpen && (
                    <div className="popover-panel popover-right-aligned" style={{ right: 0 }}>
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
                        <div className="popover-hint">{"\u6700\u65b0\u65e5\u306e\u653f\u5c40\u3092\u518d\u8a08\u7b97\u3057\u307e\u3059\u3002"}</div>
                      </div>
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
                            <span>{watchlistExporting ? "エクスポート中..." : "EXPORT"}</span>
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
                          disabled={eventsMeta?.isRefreshing}
                          onClick={() => {
                            void refreshEvents();
                            setSettingsOpen(false);
                          }}
                        >
                          <span className="popover-item-label">
                            <IconRefresh size={16} />
                            <span>
                              {eventsMeta?.isRefreshing ? "更新中..." : "イベント更新"}
                            </span>
                          </span>
                          <span className="popover-status">手動</span>
                        </button>
                        <div className="popover-hint">
                          状態: {eventsMeta?.isRefreshing ? "更新中" : "待機中"}
                        </div>
                        <div className="popover-hint">
                          最終試行: {eventsAttemptLabel ?? "--"}
                        </div>
                        {eventsMeta?.lastError && (
                          <div className="popover-hint">エラー: {eventsMeta.lastError}</div>
                        )}
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
            <div className="segmented segmented-compact list-range">
              {rangeOptions.map((option) => (
                <button
                  key={option.label}
                  className={listRangeBars === option.count ? "active" : ""}
                  onClick={() => setListRangeBars(option.count)}
                >
                  {option.label}
                </button>
              ))}
            </div>
            {sectorParam && (
              <div className="sector-filter-chip">
                <span>セクター: {sectorLabel ?? sectorParam}</span>
                <button type="button" onClick={clearSectorFilter}>
                  解除
                </button>
              </div>
            )}

            {/* Sector Sort/Filter Button */}
            <div className="sector-sort-button" style={{ marginRight: 8, position: "relative" }} ref={sectorSortRef}>
              <IconButton
                icon={<IconBuildingArch size={18} />}
                label={sectorLabel ? sectorLabel : "業種"}
                variant="iconLabel"
                tooltip="業種で絞り込み / 並び替え"
                selected={sectorSortOpen || !!sectorParam || sectorSortEnabled}
                onClick={() => {
                  setSectorSortOpen(!sectorSortOpen);
                  setSortOpen(false);
                  setDisplayOpen(false);
                  setSettingsOpen(false);
                }}
              />
              {sectorSortOpen && (
                <div className="popover-panel" style={{ width: 320, maxHeight: 500, overflowY: "auto" }}>
                  <div className="popover-section">
                    <div className="popover-title" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span>業種で絞り込み</span>
                      {sectorParam && (
                        <button
                          type="button"
                          className="text-button"
                          style={{ fontSize: 11, color: "var(--theme-text-muted)" }}
                          onClick={() => handleSectorSelect(null)}
                        >
                          解除
                        </button>
                      )}
                    </div>
                    <div className="popover-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 4 }}>
                      <button
                        type="button"
                        className={`popover-item ${!sectorParam ? "active" : ""}`}
                        onClick={() => handleSectorSelect(null)}
                        style={{ justifyContent: "center" }}
                      >
                        <span className="popover-item-label">すべて</span>
                      </button>
                      {availableSectors.map((sec) => (
                        <button
                          key={sec.code}
                          type="button"
                          className={`popover-item ${sectorParam === sec.code ? "active" : ""}`}
                          onClick={() => handleSectorSelect(sec.code)}
                          style={{ justifyContent: "flex-start", padding: "6px 8px" }}
                        >
                          <span className="popover-item-label" style={{ fontSize: 11 }}>{sec.name}</span>
                        </button>
                      ))}
                    </div>
                  </div>


                </div>
              )}
            </div>

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
                状態: {eventsMeta?.isRefreshing ? "更新中" : "待機中"}
              </span>
              {eventsMeta?.lastError && (
                <span className="event-meta-error" title={eventsMeta.lastError}>
                  エラー: {eventsMeta.lastError}
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





        {hasActiveFilters && (
          <div className="tech-filter-chips-row">
            {techFilterActive.conditions.length > 0 && (
              <>
                <span className="tech-filter-chip">
                  基準日: 最新 {activeAnchorLabel ? `(${activeAnchorLabel})` : ""}
                </span>
                <span className="tech-filter-chip">
                  条件足種: {activeTimeframeLabel}
                </span>
              </>
            )}
            {techFilterActive.boxThisMonth && (
              <span className="tech-filter-chip">今月ボックス</span>
            )}
            {techFilterActive.conditions.map((condition) => (
              <span key={condition.id} className="tech-filter-chip">
                {(condition.timeframe === "daily"
                  ? "日足"
                  : condition.timeframe === "weekly"
                    ? "週足"
                    : "月足")}: {describeCondition(condition)}
                <button type="button" onClick={() => handleRemoveActiveCondition(condition.id)}>
                  ×
                </button>
              </span>
            ))}
            <button type="button" className="tech-filter-chip-reset" onClick={handleClearActiveFilters}>
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
                      return (
                        <StockTile
                          ticker={item.ticker}
                          timeframe={gridTimeframe}
                          maxBars={gridMaxBars}
                          signals={item.metrics?.signals ?? []}
                          active={activeCode === item.ticker.code}
                          kept={keepSet.has(item.ticker.code)}
                          asofLabel={asofLabel}
                          asofTooltip={asofTooltip}
                          onActivate={activateByCode}
                          onOpenDetail={handleOpenDetail}
                          onToggleKeep={handleToggleKeep}
                          onExclude={handleExclude}
                          theme={currentTheme}
                        />
                      );
                    })()}
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
                <div className="consult-placeholder">建玉情報がありません</div>
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
      <TechnicalFilterDrawer
        open={techFilterOpen}
        timeframe={techFilterDraft.defaultTimeframe}
        anchorLabel={draftAnchorLabel}
        matchCount={draftFilterResult.items.length}
        value={techFilterDraft}
        onChange={setTechFilterDraft}
        onApply={handleApplyTechFilter}
        onCancel={handleCancelTechFilter}
        onReset={handleResetTechFilterDraft}
        onTimeframeChange={(next) => {
          setTechFilterDraft((prev) => ({ ...prev, defaultTimeframe: next }));
        }}
      />
      <Toast message={toastMessage?.text ?? null} onClose={() => setToastMessage(null)} />
    </div>
  );
}





