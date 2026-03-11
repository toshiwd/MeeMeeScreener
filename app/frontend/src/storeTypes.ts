import type { ApiErrorInfo } from "./apiErrors";

export type Ticker = {
  code: string;
  name: string;
  sector33Code?: string | null;
  sector33Name?: string | null;
  stage: string;
  score: number | null;
  reason: string;
  scoreStatus?: string | null;
  missingReasons?: string[] | null;
  scoreBreakdown?: Record<string, number> | null;
  dataStatus?: "missing" | null;
  liquidity20d?: number | null;
  atr14?: number | null;
  lastClose?: number | null;
  chg1D?: number | null;
  chg1W?: number | null;
  chg1M?: number | null;
  chg1Q?: number | null;
  chg1Y?: number | null;
  prevWeekChg?: number | null;
  prevMonthChg?: number | null;
  prevQuarterChg?: number | null;
  prevYearChg?: number | null;
  counts?: {
    up7?: number | null;
    down7?: number | null;
    up20?: number | null;
    down20?: number | null;
    up60?: number | null;
    down60?: number | null;
    up100?: number | null;
    down100?: number | null;
  };
  boxState?: "NONE" | "IN_BOX" | "JUST_BREAKOUT" | "BREAKOUT_UP" | "BREAKOUT_DOWN";
  boxEndMonth?: string | null;
  breakoutMonth?: string | null;
  boxActive?: boolean;
  hasBox?: boolean;
  // Buy Fields
  buyState?: string | null;
  buyPatternName?: string | null;
  buyPatternCode?: string | null;
  entryPriorityScore?: number | null;
  entryPriorityTier?: "A" | "B" | "C" | null;
  entryPriorityLabel?: string | null;
  entryPriorityReasons?: string[] | null;
  buyHardExcluded?: boolean | null;
  buyHardExcludeReasons?: string[] | null;
  buyStateRank?: number | null;
  buyStateScore?: number | null;
  buyCandidateScore?: number | null;
  buyEnvScore?: number | null;
  buyTimingScore?: number | null;
  buyRiskScore?: number | null;
  buyStateReason?: string | null;
  buyEligible?: boolean;
  buyOverextended?: boolean | null;
  buySignalRecencyDays?: number | null;
  buyRiskAtr?: number | null;
  buyUpsideAtr?: number | null;
  buyRiskDistance?: number | null; // legacy
  buyStateDetails?: {
    monthly?: number | null;
    weekly?: number | null;
    daily?: number | null;
  } | null;
  scores?: {
    upScore?: number | null;
    downScore?: number | null;
    overheatUp?: number | null;
    overheatDown?: number | null;
  };
  mlPUp?: number | null;
  mlPUp5?: number | null;
  mlPUp10?: number | null;
  mlPUpShort?: number | null;
  mlPDown?: number | null;
  mlPDownShort?: number | null;
  mlPTurnDown?: number | null;
  mlPTurnDown5?: number | null;
  mlPTurnDown10?: number | null;
  mlPTurnDown20?: number | null;
  mlPTurnDownShort?: number | null;
  mlEv20Net?: number | null;
  mlEv5Net?: number | null;
  mlEv10Net?: number | null;
  mlEvShortNet?: number | null;
  mlModelVersion?: string | null;
  statusLabel?: string;
  reasons?: string[];
  earlyScore?: number | null;
  lateScore?: number | null;
  bodyScore?: number | null;
  phaseN?: number | null;
  phaseReasons?: string[] | null;
  phaseDt?: number | null;
  // Short-selling fields
  shortScore?: number | null; // legacy
  shortCandidateScore?: number | null;
  aScore?: number | null; // legacy
  bScore?: number | null; // legacy
  aCandidateScore?: number | null;
  bCandidateScore?: number | null;
  shortPriorityScore?: number | null;
  shortPriorityTier?: "A" | "B" | "C" | null;
  shortPriorityLabel?: string | null;
  shortPriorityReasons?: string[] | null;
  shortHardExcluded?: boolean | null;
  shortHardExcludeReasons?: string[] | null;
  shortEligible?: boolean;
  shortEnvScore?: number | null;
  shortRiskScore?: number | null;
  shortType?: "A" | "B" | null;
  shortBadges?: string[];
  shortReasons?: string[];
  shortProhibitReason?: string | null;
  sellStop?: number | null;
  sellTarget?: number | null;
  sellRiskAtr?: number | null;
  sellDownsideAtr?: number | null;
  eventEarningsDate?: string | null;
  eventRightsDate?: string | null;
  swingScore?: number | null;
  swingQualified?: boolean | null;
  swingSide?: "long" | "short" | "none" | null;
  swingReasons?: string[] | null;
  swingLongScore?: number | null;
  swingShortScore?: number | null;
};

export type EventsMeta = {
  earningsLastSuccessAt: string | null;
  rightsLastSuccessAt: string | null;
  isRefreshing: boolean;
  refreshJobId: string | null;
  lastError: string | null;
  lastAttemptAt: string | null;
  dataCoverage?: {
    rightsMaxDate?: string | null;
  };
};

export type GridTimeframe = "monthly" | "weekly" | "daily";

export type MaTimeframe = "daily" | "weekly" | "monthly";

export type MaSetting = {
  key: string;
  label: string;
  period: number;
  visible: boolean;
  color: string;
  lineWidth: number;
};

export type Box = {
  startIndex: number;
  endIndex: number;
  startTime: number;
  endTime: number;
  lower: number;
  upper: number;
  breakout: "up" | "down" | null;
};

export type BarsPayload = {
  bars: number[][];
  ma: {
    ma7: number[][];
    ma20: number[][];
    ma60: number[][];
  };
  boxes?: Box[];
};

export type MultiTimeframeBarsPayload = {
  daily?: BarsPayload;
  weekly?: BarsPayload;
  monthly?: BarsPayload;
};

export type BarsCache = {
  monthly: Record<string, BarsPayload>;
  weekly: Record<string, BarsPayload>;
  daily: Record<string, BarsPayload>;
};

export type BoxesCache = {
  monthly: Record<string, Box[]>;
  weekly: Record<string, Box[]>;
  daily: Record<string, Box[]>;
};

export type MaSettings = {
  daily: MaSetting[];
  weekly: MaSetting[];
  monthly: MaSetting[];
};

export type LoadingMap = {
  monthly: Record<string, boolean>;
  weekly: Record<string, boolean>;
  daily: Record<string, boolean>;
};

export type StatusMap = {
  monthly: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
  weekly: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
  daily: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
};

export type Settings = {
  columns: 1 | 2 | 3 | 4;
  rows: 1 | 2 | 3 | 4 | 5 | 6;
  search: string;
  gridScrollTop: number;
  gridTimeframe: GridTimeframe;
  listTimeframe: GridTimeframe;
  listRangeBars: 60 | 120 | 240 | 360;
  listColumns: 1 | 2 | 3 | 4;
  listRows: 1 | 2 | 3 | 4 | 5 | 6;
  showBoxes: boolean;
  showIndicators: boolean;
  // Legacy sort key (for backward compatibility during migration)
  sortKey: SortKey;
  sortDir: SortDir;
  // Separated sort states (new)
  candidateSortKey: CandidateSortKey;
  basicSortKey: BasicSortKey;
  basicSortDir: SortDir;
  performancePeriod: PerformancePeriod;
};

export type StoreState = {
  tickers: Ticker[];
  favorites: string[];
  favoritesLoaded: boolean;
  favoritesLoading: boolean;
  keepList: string[];
  barsCache: BarsCache;
  boxesCache: BoxesCache;
  barsLoading: LoadingMap;
  barsStatus: StatusMap;
  loadingList: boolean;
  backendReady: boolean;
  lastApiError: ApiErrorInfo | null;
  eventsMeta: EventsMeta | null;
  eventsMetaLoading: boolean;
  maSettings: MaSettings;
  compareMaSettings: MaSettings;
  settings: Settings;
  setLastApiError: (info: ApiErrorInfo | null) => void;
  loadList: () => Promise<void>;
  loadFavorites: () => Promise<void>;
  replaceFavorites: (codes: string[]) => void;
  setFavoriteLocal: (code: string, isFavorite: boolean) => void;
  addKeep: (code: string) => void;
  removeKeep: (code: string) => void;
  clearKeep: () => void;
  replaceKeep: (codes: string[]) => void;

  setBackendReady: (ready: boolean) => void;

  setCandidateSortKey: (key: CandidateSortKey) => void;
  setBasicSortKey: (key: BasicSortKey) => void;
  setBasicSortDir: (dir: SortDir) => void;
  setPerformancePeriod: (period: PerformancePeriod) => void;

  updateMaSetting: (
    timeframe: MaTimeframe,
    index: number,
    patch: Partial<MaSetting>
  ) => void;
  updateCompareMaSetting: (timeframe: MaTimeframe, index: number, patch: Partial<MaSetting>) => void;
  resetMaSettings: (timeframe: MaTimeframe) => void;
  resetCompareMaSettings: (timeframe: MaTimeframe) => void;
  resetBarsCache: () => void;
  loadEventsMeta: () => Promise<EventsMeta | null>;
  refreshEventsIfStale: () => Promise<void>;
  refreshEvents: () => Promise<void>;
  loadBarsBatch: (timeframe: GridTimeframe, codes: string[], limitOverride?: number, reason?: string) => Promise<void>;
  loadBoxesBatch: (codes: string[]) => Promise<void>;
  ensureBarsForVisible: (timeframe: GridTimeframe, codes: string[], reason?: string) => Promise<void>;
  setColumns: (value: 1 | 2 | 3 | 4) => void;
  setRows: (value: 1 | 2 | 3 | 4 | 5 | 6) => void;
  setListTimeframe: (value: GridTimeframe) => void;
  setListRangeBars: (value: number) => void;
  setListColumns: (value: 1 | 2 | 3 | 4) => void;
  setListRows: (value: 1 | 2 | 3 | 4 | 5 | 6) => void;
  setSearch: (value: string) => void;
  setGridScrollTop: (value: number) => void;
  setGridTimeframe: (value: GridTimeframe) => void;
  setShowBoxes: (value: boolean) => void;
  setSortKey: (value: SortKey) => void;
  setSortDir: (value: SortDir) => void;
  toggleKeep: (code: string) => void;
};

// Candidate sort presets (for buy/sell candidate screens only)
export type CandidateSortKey =
  | "entryPriority"     // 仕込み優先度
  | "buyCandidate"      // 買い候補（総合）
  | "buyInitial"        // 買い候補（初動）
  | "buyBase"           // 買い候補（底がため）
  | "swingScore"        // スイング候補（総合）
  | "shortPriority"     // 売り精度優先
  | "shortScore"        // 売り候補（総合）
  | "aScore"            // 売り候補（反転確定）
  | "bScore";           // 売り候補（戻り売り）

// Basic sort keys (for non-candidate screens)
export type BasicSortKey =
  | "code"
  | "name"
  | "sector"
  | "ma20Dev"
  | "ma60Dev"
  | "ma20Slope"
  | "ma60Slope"
  | "performance"       // Single performance key with period selector
  | "upScore"
  | "downScore"
  | "overheatUp"
  | "overheatDown"
  | "swingScore"
  | "mlEv20Net"
  | "mlPUpShort"
  | "mlPDownShort"
  | "boxState";

// Performance period for unified performance sorting
export type PerformancePeriod = "1D" | "1W" | "1M" | "1Q" | "1Y";

// Legacy combined type for backward compatibility
export type SortKey =
  | "code"
  | "name"
  | "sector"
  | "entryPriority"
  | "buyCandidate"
  | "buyInitial"
  | "buyBase"
  | "buySignalLatest"
  | "sellSignalLatest"
  | "ma20Dev"
  | "ma60Dev"
  | "ma20Slope"
  | "ma60Slope"
  | "chg1D"
  | "chg1W"
  | "chg1M"
  | "chg1Q"
  | "chg1Y"
  | "prevWeekChg"
  | "prevMonthChg"
  | "prevQuarterChg"
  | "prevYearChg"
  | "upScore"
  | "downScore"
  | "overheatUp"
  | "overheatDown"
  | "swingScore"
  | "mlEv20Net"
  | "mlPUpShort"
  | "mlPDownShort"
  | "boxState"
  | "shortPriority"
  | "shortScore"
  | "aScore"
  | "bScore"
  | "performance";

export type SortDir = "asc" | "desc";
