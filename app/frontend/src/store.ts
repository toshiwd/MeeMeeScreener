import { create } from "zustand";
import { api, setApiErrorReporter } from "./api";
import type {
  BarsPayload,
  Box,
  EventsMeta,
  GridTimeframe,
  MaSetting,
  MultiTimeframeBarsPayload,
  Settings,
  StoreState,
  Ticker
} from "./storeTypes";
import {
  BATCH_REQUEST_TIMEOUT_MS,
  BATCH_RETRY_DELAYS_MS,
  BATCH_TTL_MS,
  COMPARE_MA_STORAGE_PREFIX,
  ENSURE_COALESCE_MS,
  GRID_COLS_KEY,
  GRID_ROWS_KEY,
  KEEP_STORAGE_KEY,
  LIST_COLS_KEY,
  LIST_RANGE_KEY,
  LIST_RANGE_VALUES,
  LIST_ROWS_KEY,
  LIST_TIMEFRAME_KEY,
  MA_STORAGE_PREFIX,
  WATCHLIST_AUTO_REPAIR_COOLDOWN_MS,
  WATCHLIST_AUTO_REPAIR_MIN_MISSING,
  WATCHLIST_AUTO_REPAIR_MIN_RATIO,
  WATCHLIST_AUTO_REPAIR_TS_KEY,
  abortInFlightForTimeframe,
  barsFetchedLimit,
  buildBatchKey,
  counters,
  ensureCoalesceTimers,
  ensurePendingCodes,
  ensurePendingReason,
  ensurePendingWaiters,
  getFetchedLimit,
  getInitialColumns,
  getInitialListColumns,
  getInitialListRangeBars,
  getInitialListRows,
  getInitialRows,
  getInitialSortDir,
  getInitialSortKey,
  getInitialTimeframe,
  getRequiredBars,
  inFlightBatchRequests,
  isAbortError,
  isEventsStale,
  isRetriableBatchError,
  lastEnsureKeyByTimeframe,
  loadKeepList,
  loadSettings,
  makeDefaultSettings,
  markFetchedLimit,
  normalizeColor,
  normalizeEventsMeta,
  normalizeLineWidth,
  persistKeepList,
  persistSettings,
  recentBatchRequests,
  sleepMs,
  startEventsMetaPolling
} from "./storeHelpers";



export const useStore = create<StoreState>((set, get) => ({
  tickers: [],
  favorites: [],
  favoritesLoaded: false,
  favoritesLoading: false,
  keepList: loadKeepList(),
  barsCache: { monthly: {}, weekly: {}, daily: {} },
  boxesCache: { monthly: {}, weekly: {}, daily: {} },
  barsLoading: { monthly: {}, weekly: {}, daily: {} },
  barsStatus: { monthly: {}, weekly: {}, daily: {} },
  loadingList: false,
  backendReady: false,
  lastApiError: null,
  eventsMeta: {
    earningsLastSuccessAt: null,
    rightsLastSuccessAt: null,
    lastAttemptAt: null,
    lastError: null,
    refreshJobId: null,
    isRefreshing: false
  },
  eventsMetaLoading: false,
  maSettings: {
    daily: loadSettings("daily"),
    weekly: loadSettings("weekly"),
    monthly: loadSettings("monthly")
  },
  compareMaSettings: {
    daily: loadSettings("daily", COMPARE_MA_STORAGE_PREFIX),
    weekly: loadSettings("weekly", COMPARE_MA_STORAGE_PREFIX),
    monthly: loadSettings("monthly", COMPARE_MA_STORAGE_PREFIX)
  },
  settings: {
    columns: getInitialColumns(),
    rows: getInitialRows(),
    listColumns: getInitialListColumns(),
    listRows: getInitialListRows(),
    listRangeBars: getInitialListRangeBars(),
    search: "",
    gridScrollTop: 0,
    gridTimeframe: getInitialTimeframe(),
    listTimeframe: "daily",
    showBoxes: true,
    showIndicators: false,
    sortKey: getInitialSortKey(),
    sortDir: getInitialSortDir(),
    candidateSortKey: "entryPriority",
    basicSortKey: "code",
    basicSortDir: "asc",
    performancePeriod: "1M"
  },
  setLastApiError: (info) => set({ lastApiError: info }),
  loadFavorites: async () => {
    if (get().favoritesLoading) return;
    set({ favoritesLoading: true });
    try {
      const res = await api.get("/favorites");
      const payload = res.data as { items?: { code?: string }[] } | { code?: string }[];
      const items = Array.isArray(payload) ? payload : payload.items ?? [];
      const codes = items
        .map((item) => (typeof item.code === "string" ? item.code : ""))
        .filter((code) => code);
      set({ favorites: codes, favoritesLoaded: true });
    } catch (error) {
      const err = error as {
        message?: string;
        response?: { status?: number; data?: unknown };
      };
      console.error("[favorites] load failed", {
        status: err?.response?.status ?? null,
        data: err?.response?.data ?? null,
        message: err?.message ?? null
      });
      set({ favorites: [], favoritesLoaded: true });
    } finally {
      set({ favoritesLoading: false });
    }
  },
  replaceFavorites: (codes) =>
    set({ favorites: [...new Set(codes.filter((code) => code))], favoritesLoaded: true }),
  setFavoriteLocal: (code, isFavorite) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const exists = state.favorites.includes(normalized);
      if (isFavorite && !exists) {
        return { favorites: [...state.favorites, normalized], favoritesLoaded: true };
      }
      if (!isFavorite && exists) {
        return {
          favorites: state.favorites.filter((item) => item !== normalized),
          favoritesLoaded: true
        };
      }
      return state;
    }),
  addKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      if (state.keepList.includes(normalized)) return state;
      const next = [...state.keepList, normalized];
      persistKeepList(next);
      return { keepList: next };
    }),
  removeKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const next = state.keepList.filter((item) => item !== normalized);
      persistKeepList(next);
      return { keepList: next };
    }),
  toggleKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const exists = state.keepList.includes(normalized);
      const next = exists
        ? state.keepList.filter((item) => item !== normalized)
        : [...state.keepList, normalized];
      persistKeepList(next);
      return { keepList: next };
    }),
  clearKeep: () =>
    set((state) => {
      if (!state.keepList.length) return state;
      persistKeepList([]);
      return { keepList: [] };
    }),
  replaceKeep: (codes) => {
    persistKeepList(codes);
    set({ keepList: codes });
  },
  setBackendReady: (ready) => set({ backendReady: ready }),
  loadList: async () => {
    if (get().loadingList) return;
    set({ loadingList: true });
    try {
      const res = await api.get("/grid/screener");
      const payload = res.data as { items?: Ticker[] } | Ticker[];
      const items = Array.isArray(payload) ? payload : payload.items ?? [];
      if (!items.length) {
        throw new Error("Empty screener payload");
      }
      const parseReasons = (value: unknown): string[] => {
        if (Array.isArray(value)) {
          return value.filter((item) => typeof item === "string") as string[];
        }
        if (typeof value === "string" && value.trim()) {
          try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed)) {
              return parsed.filter((item) => typeof item === "string") as string[];
            }
          } catch {
            return value.split(",").map((item) => item.trim()).filter(Boolean);
          }
        }
        return [];
      };
      const tickers: Ticker[] = items.map((rawItem) => {
        const item = rawItem as Record<string, any>;
        const statusLabel = item.statusLabel ?? null;
        const stageRaw = item.stage ?? statusLabel ?? "UNKNOWN";
        const stage =
          typeof stageRaw === "string" && stageRaw.toUpperCase() === "UNKNOWN" && statusLabel
            ? statusLabel
            : stageRaw;
        const nameRaw = typeof item.name === "string" ? item.name.trim() : "";
        return {
          code: item.code,
          name: nameRaw || item.code,
          sector33Code: item.sector33Code ?? item.sector33_code ?? null,
          sector33Name: item.sector33Name ?? item.sector33_name ?? null,
          stage,
          score: Number.isFinite(item.score) ? item.score : null,
          reason: item.reason ?? "",
          scoreStatus:
            item.scoreStatus ??
            item.score_status ??
            (Number.isFinite(item.score) ? "OK" : "INSUFFICIENT_DATA"),
          missingReasons: parseReasons(item.missingReasons ?? item.missing_reasons ?? item.missing_reasons_json),
          scoreBreakdown:
            (item.scoreBreakdown as Record<string, number> | null) ??
            (item.score_breakdown as Record<string, number> | null) ??
            null,
          lastClose: item.lastClose ?? null,
          chg1D: item.chg1D ?? null,
          chg1W: item.chg1W ?? null,
          chg1M: item.chg1M ?? null,
          chg1Q: item.chg1Q ?? null,
          chg1Y: item.chg1Y ?? null,
          prevWeekChg: item.prevWeekChg ?? null,
          prevMonthChg: item.prevMonthChg ?? null,
          prevQuarterChg: item.prevQuarterChg ?? null,
          prevYearChg: item.prevYearChg ?? null,
          counts: item.counts,
          boxState: item.boxState ?? item.box_state ?? "NONE",
          boxEndMonth: item.boxEndMonth ?? item.box_end_month ?? null,
          breakoutMonth: item.breakoutMonth ?? item.breakout_month ?? null,
          boxActive:
            typeof item.boxActive === "boolean"
              ? item.boxActive
              : typeof item.box_active === "boolean"
                ? item.box_active
                : null,
          hasBox:
            typeof item.hasBox === "boolean"
              ? item.hasBox
              : typeof item.boxActive === "boolean"
                ? item.boxActive
                : typeof item.box_active === "boolean"
                  ? item.box_active
                  : (item.boxState ?? item.box_state ?? "NONE") !== "NONE",
          buyState: item.buyState ?? item.buy_state ?? null,
          entryPriorityScore:
            typeof item.entryPriorityScore === "number"
              ? item.entryPriorityScore
              : typeof item.entry_priority_score === "number"
                ? item.entry_priority_score
                : null,
          entryPriorityTier: item.entryPriorityTier ?? item.entry_priority_tier ?? null,
          entryPriorityLabel: item.entryPriorityLabel ?? item.entry_priority_label ?? null,
          entryPriorityReasons: parseReasons(
            item.entryPriorityReasons ?? item.entry_priority_reasons
          ),
          buyHardExcluded:
            typeof item.buyHardExcluded === "boolean"
              ? item.buyHardExcluded
              : typeof item.buy_hard_excluded === "boolean"
                ? item.buy_hard_excluded
                : null,
          buyHardExcludeReasons: parseReasons(
            item.buyHardExcludeReasons ?? item.buy_hard_exclude_reasons
          ),
          buyPatternName: item.buyPatternName ?? item.buy_pattern_name ?? null,
          buyPatternCode: item.buyPatternCode ?? item.buy_pattern_code ?? null,
          buyStateRank:
            typeof item.buyStateRank === "number"
              ? item.buyStateRank
              : typeof item.buy_state_rank === "number"
                ? item.buy_state_rank
                : null,
          buyStateScore:
            typeof item.buyStateScore === "number"
              ? item.buyStateScore
              : typeof item.buy_state_score === "number"
                ? item.buy_state_score
                : null,
          buyStateReason: item.buyStateReason ?? item.buy_state_reason ?? null,
          buyOverextended:
            typeof item.buyOverextended === "boolean"
              ? item.buyOverextended
              : typeof item.buy_overextended === "boolean"
                ? item.buy_overextended
                : null,
          buyRiskDistance:
            typeof item.buyRiskDistance === "number"
              ? item.buyRiskDistance
              : typeof item.buy_risk_distance === "number"
                ? item.buy_risk_distance
                : null,
          buyStateDetails: item.buyStateDetails ?? null,
          scores: item.scores,
          mlPUp: Number.isFinite(item.mlPUp) ? item.mlPUp : Number.isFinite(item.ml_p_up) ? item.ml_p_up : null,
          mlPUp5:
            Number.isFinite(item.mlPUp5)
              ? item.mlPUp5
              : Number.isFinite(item.ml_p_up_5)
                ? item.ml_p_up_5
                : null,
          mlPUp10:
            Number.isFinite(item.mlPUp10)
              ? item.mlPUp10
              : Number.isFinite(item.ml_p_up_10)
                ? item.ml_p_up_10
                : null,
          mlPUpShort:
            Number.isFinite(item.mlPUpShort)
              ? item.mlPUpShort
              : Number.isFinite(item.ml_p_up_short)
                ? item.ml_p_up_short
                : null,
          mlPDown: Number.isFinite(item.mlPDown) ? item.mlPDown : Number.isFinite(item.ml_p_down) ? item.ml_p_down : null,
          mlPDownShort:
            Number.isFinite(item.mlPDownShort)
              ? item.mlPDownShort
              : Number.isFinite(item.ml_p_down_short)
                ? item.ml_p_down_short
                : null,
          mlPTurnDown:
            Number.isFinite(item.mlPTurnDown)
              ? item.mlPTurnDown
              : Number.isFinite(item.ml_p_turn_down)
                ? item.ml_p_turn_down
                : null,
          mlPTurnDown5:
            Number.isFinite(item.mlPTurnDown5)
              ? item.mlPTurnDown5
              : Number.isFinite(item.ml_p_turn_down_5)
                ? item.ml_p_turn_down_5
                : null,
          mlPTurnDown10:
            Number.isFinite(item.mlPTurnDown10)
              ? item.mlPTurnDown10
              : Number.isFinite(item.ml_p_turn_down_10)
                ? item.ml_p_turn_down_10
                : null,
          mlPTurnDown20:
            Number.isFinite(item.mlPTurnDown20)
              ? item.mlPTurnDown20
              : Number.isFinite(item.ml_p_turn_down_20)
                ? item.ml_p_turn_down_20
                : null,
          mlPTurnDownShort:
            Number.isFinite(item.mlPTurnDownShort)
              ? item.mlPTurnDownShort
              : Number.isFinite(item.ml_p_turn_down_short)
                ? item.ml_p_turn_down_short
                : null,
          mlEv20Net:
            Number.isFinite(item.mlEv20Net)
              ? item.mlEv20Net
              : Number.isFinite(item.ml_ev20_net)
                ? item.ml_ev20_net
                : null,
          mlEv5Net:
            Number.isFinite(item.mlEv5Net)
              ? item.mlEv5Net
              : Number.isFinite(item.ml_ev5_net)
                ? item.ml_ev5_net
                : null,
          mlEv10Net:
            Number.isFinite(item.mlEv10Net)
              ? item.mlEv10Net
              : Number.isFinite(item.ml_ev10_net)
                ? item.ml_ev10_net
                : null,
          mlEvShortNet:
            Number.isFinite(item.mlEvShortNet)
              ? item.mlEvShortNet
              : Number.isFinite(item.ml_ev_short_net)
                ? item.ml_ev_short_net
                : null,
          mlModelVersion:
            typeof item.mlModelVersion === "string"
              ? item.mlModelVersion
              : typeof item.ml_model_version === "string"
                ? item.ml_model_version
                : null,
          statusLabel: item.statusLabel,
          reasons: item.reasons,
          earlyScore: Number.isFinite(item.earlyScore) ? item.earlyScore : item.early_score ?? null,
          lateScore: Number.isFinite(item.lateScore) ? item.lateScore : item.late_score ?? null,
          bodyScore: Number.isFinite(item.bodyScore) ? item.bodyScore : item.body_score ?? null,
          phaseN:
            typeof item.phaseN === "number"
              ? item.phaseN
              : typeof item.phase_n === "number"
                ? item.phase_n
                : typeof item.n === "number"
                  ? item.n
                  : null,
          phaseReasons: parseReasons(
            item.phaseReasons ?? item.phase_reasons ?? item.reasons_top3 ?? item.reasonsTop3
          ),
          phaseDt:
            typeof item.phaseDt === "number"
              ? item.phaseDt
              : typeof item.phase_dt === "number"
                ? item.phase_dt
                : null,
          // Short-selling fields
          shortScore:
            typeof item.shortScore === "number"
              ? item.shortScore
              : typeof item.short_score === "number"
                ? item.short_score
                : null,
          shortCandidateScore:
            typeof item.shortCandidateScore === "number"
              ? item.shortCandidateScore
              : typeof item.short_candidate_score === "number"
                ? item.short_candidate_score
                : null,
          aScore:
            typeof item.aScore === "number"
              ? item.aScore
              : typeof item.a_score === "number"
                ? item.a_score
                : null,
          bScore:
            typeof item.bScore === "number"
              ? item.bScore
              : typeof item.b_score === "number"
                ? item.b_score
                : null,
          aCandidateScore:
            typeof item.aCandidateScore === "number"
              ? item.aCandidateScore
              : typeof item.a_candidate_score === "number"
                ? item.a_candidate_score
                : null,
          bCandidateScore:
            typeof item.bCandidateScore === "number"
              ? item.bCandidateScore
              : typeof item.b_candidate_score === "number"
                ? item.b_candidate_score
                : null,
          shortPriorityScore:
            typeof item.shortPriorityScore === "number"
              ? item.shortPriorityScore
              : typeof item.short_priority_score === "number"
                ? item.short_priority_score
                : null,
          shortPriorityTier: item.shortPriorityTier ?? item.short_priority_tier ?? null,
          shortPriorityLabel: item.shortPriorityLabel ?? item.short_priority_label ?? null,
          shortPriorityReasons: parseReasons(
            item.shortPriorityReasons ?? item.short_priority_reasons
          ),
          shortHardExcluded:
            typeof item.shortHardExcluded === "boolean"
              ? item.shortHardExcluded
              : typeof item.short_hard_excluded === "boolean"
                ? item.short_hard_excluded
                : null,
          shortHardExcludeReasons: parseReasons(
            item.shortHardExcludeReasons ?? item.short_hard_exclude_reasons
          ),
          shortEligible:
            typeof item.shortEligible === "boolean"
              ? item.shortEligible
              : typeof item.short_eligible === "boolean"
                ? item.short_eligible
                : null,
          shortEnvScore:
            typeof item.shortEnvScore === "number"
              ? item.shortEnvScore
              : typeof item.short_env_score === "number"
                ? item.short_env_score
                : null,
          shortRiskScore:
            typeof item.shortRiskScore === "number"
              ? item.shortRiskScore
              : typeof item.short_risk_score === "number"
                ? item.short_risk_score
                : null,
          shortType: item.shortType ?? null,
          shortBadges: Array.isArray(item.shortBadges) ? item.shortBadges : [],
          shortReasons: Array.isArray(item.shortReasons) ? item.shortReasons : [],
          shortProhibitReason: item.shortProhibitReason ?? item.short_prohibit_reason ?? null,
          sellStop:
            typeof item.sellStop === "number"
              ? item.sellStop
              : typeof item.sell_stop === "number"
                ? item.sell_stop
                : null,
          sellTarget:
            typeof item.sellTarget === "number"
              ? item.sellTarget
              : typeof item.sell_target === "number"
                ? item.sell_target
                : null,
          sellRiskAtr:
            typeof item.sellRiskAtr === "number"
              ? item.sellRiskAtr
              : typeof item.sell_risk_atr === "number"
                ? item.sell_risk_atr
                : null,
          sellDownsideAtr:
            typeof item.sellDownsideAtr === "number"
              ? item.sellDownsideAtr
              : typeof item.sell_downside_atr === "number"
                ? item.sell_downside_atr
                : null,
          eventEarningsDate: item.eventEarningsDate ?? item.event_earnings_date ?? null,
          eventRightsDate: item.eventRightsDate ?? item.event_rights_date ?? null,
          swingScore:
            typeof item.swingScore === "number"
              ? item.swingScore
              : typeof item.swing_score === "number"
                ? item.swing_score
                : null,
          swingQualified:
            typeof item.swingQualified === "boolean"
              ? item.swingQualified
              : typeof item.swing_qualified === "boolean"
                ? item.swing_qualified
                : null,
          swingSide: (() => {
            const raw = item.swingSide ?? item.swing_side;
            return raw === "long" || raw === "short" || raw === "none" ? raw : null;
          })(),
          swingReasons: parseReasons(item.swingReasons ?? item.swing_reasons),
          swingLongScore:
            typeof item.swingLongScore === "number"
              ? item.swingLongScore
              : typeof item.swing_long_score === "number"
                ? item.swing_long_score
                : null,
          swingShortScore:
            typeof item.swingShortScore === "number"
              ? item.swingShortScore
              : typeof item.swing_short_score === "number"
                ? item.swing_short_score
                : null,
        };
      });
      try {
        const resWatch = await api.get("/watchlist");
        const watchlistCodes = (resWatch.data?.codes || []) as string[];
        if (watchlistCodes.length) {
          const existing = new Set(tickers.map((item) => item.code));
          const missingWatchlistCodes = watchlistCodes.filter((code) => !existing.has(code));
          const missingRatio = missingWatchlistCodes.length / Math.max(1, watchlistCodes.length);
          const shouldAutoRepair =
            missingWatchlistCodes.length >= WATCHLIST_AUTO_REPAIR_MIN_MISSING &&
            missingRatio >= WATCHLIST_AUTO_REPAIR_MIN_RATIO;

          if (shouldAutoRepair && typeof window !== "undefined") {
            const now = Date.now();
            const lastAutoRepairTs = Number(window.localStorage.getItem(WATCHLIST_AUTO_REPAIR_TS_KEY) || "0");
            if (!Number.isFinite(lastAutoRepairTs) || now - lastAutoRepairTs >= WATCHLIST_AUTO_REPAIR_COOLDOWN_MS) {
              window.localStorage.setItem(WATCHLIST_AUTO_REPAIR_TS_KEY, String(now));
              // Auto-repair missing watchlist coverage in the background.
              void api.post("/jobs/force-sync").catch(() => undefined);
            }
          }

          const watchlistOnlyCodes = shouldAutoRepair ? [] : missingWatchlistCodes;
          watchlistOnlyCodes.forEach((code) => {
            if (existing.has(code)) return;
            tickers.push({
              code,
              name: code,
              stage: "",
              score: null,
              reason: "WATCHLIST_ONLY",
              scoreStatus: "INSUFFICIENT_DATA",
              missingReasons: [],
              scoreBreakdown: null,
              dataStatus: "missing"
            } as Ticker);
          });
        }
      } catch {
        // ignore watchlist failures for now
      }
      set({ tickers });
    } catch {
      const res = await api.get("/list");
      const items = (res.data || []) as [string, string, string, number | null, string][];
      const tickers = items.map(([code, name, stage, score, reason]) => ({
        code,
        name,
        stage,
        score: Number.isFinite(score) ? score : null,
        reason,
        scoreStatus: Number.isFinite(score) ? "OK" : "INSUFFICIENT_DATA",
        missingReasons: null,
        scoreBreakdown: null
      }));
      set({ tickers });
    } finally {
      set({ loadingList: false });
    }
  },
  loadBarsBatch: async (timeframe, codes, limitOverride, reason) => {
    const state = get();
    const loadingMap = state.barsLoading[timeframe];
    const uniqueCodes = [...new Set(codes.filter((code) => code))];
    const trimmed = uniqueCodes.filter((code) => !loadingMap[code]);
    if (!trimmed.length) return;

    if (timeframe === "weekly") {
      const weeklyRequired = Math.max(
        limitOverride ?? 0,
        getRequiredBars(get().maSettings.weekly),
        get().settings.listRangeBars
      );
      const requestCodes = [...new Set(trimmed)].sort();
      const requestKey = buildBatchKey(timeframe, weeklyRequired, requestCodes);
      const cachedAt = recentBatchRequests.get(requestKey);
      if (cachedAt && Date.now() - cachedAt < BATCH_TTL_MS) return;

      const inFlight = inFlightBatchRequests.get(requestKey);
      if (inFlight) {
        counters.dedupHitCount += 1;
        return inFlight.promise;
      }

      counters.batchRequestCount += 1;
      counters.v3RequestCount += 1;
      console.debug("[batch_bars_v3]", {
        count: counters.batchRequestCount,
        v3_request_count: counters.v3RequestCount,
        coalesced_request_count: counters.coalescedRequestCount,
        dedup_hit_count: counters.dedupHitCount,
        key: requestKey,
        reason: reason ?? "unknown",
        timeframe,
        limit: weeklyRequired,
        codes: requestCodes.length
      });

      const controller = new AbortController();
      const requestPromise = (async () => {
        set((prev) => {
          const nextLoading = { ...prev.barsLoading.weekly };
          requestCodes.forEach((code) => {
            nextLoading[code] = true;
          });
          return {
            barsLoading: { ...prev.barsLoading, weekly: nextLoading },
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  acc[code] = "loading";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          };
        });

        try {
          const requestPayload = {
            codes: requestCodes,
            timeframes: ["weekly"],
            limit: weeklyRequired,
            includeProvisional: true
          };
          let res: { status: number; data?: any } | null = null;
          let attempt = 0;
          while (true) {
            try {
              res = await api.post("/batch_bars_v3", requestPayload, {
                signal: controller.signal,
                timeout: BATCH_REQUEST_TIMEOUT_MS
              });
              break;
            } catch (error) {
              const canRetry =
                attempt < BATCH_RETRY_DELAYS_MS.length && isRetriableBatchError(error);
              if (!canRetry) throw error;
              const retryDelay =
                BATCH_RETRY_DELAYS_MS[attempt] ??
                BATCH_RETRY_DELAYS_MS[BATCH_RETRY_DELAYS_MS.length - 1] ??
                0;
              attempt += 1;
              await sleepMs(retryDelay);
            }
          }
          if (!res) {
            throw new Error("batch_bars_v3 failed without response");
          }
          if (res.status !== 200) {
            throw new Error(`batch_bars_v3 failed with status ${res.status}`);
          }

          const rawItems = (res.data?.items || {}) as Record<
            string,
            MultiTimeframeBarsPayload | undefined
          >;
          const weeklyItems: Record<string, BarsPayload> = {};
          const weeklyBoxes: Record<string, Box[]> = {};
          requestCodes.forEach((code) => {
            const payload = rawItems[code]?.weekly;
            if (payload && Array.isArray(payload.bars)) {
              weeklyItems[code] = payload;
              weeklyBoxes[code] = payload.boxes ?? [];
            } else {
              weeklyItems[code] = {
                bars: [],
                ma: { ma7: [], ma20: [], ma60: [] },
                boxes: []
              };
              weeklyBoxes[code] = [];
            }
            markFetchedLimit("weekly", code, weeklyRequired);
          });
          recentBatchRequests.set(requestKey, Date.now());
          set((prev) => ({
            barsCache: {
              ...prev.barsCache,
              weekly: { ...prev.barsCache.weekly, ...weeklyItems }
            },
            boxesCache: {
              ...prev.boxesCache,
              weekly: { ...prev.boxesCache.weekly, ...weeklyBoxes }
            },
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  const payload = weeklyItems[code];
                  acc[code] = payload && payload.bars.length ? "success" : "empty";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          }));
        } catch (error) {
          if (isAbortError(error)) return;
          set((prev) => ({
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  const cached = prev.barsCache.weekly[code];
                  acc[code] = cached ? (cached.bars.length ? "success" : "empty") : "error";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          }));
          throw error;
        } finally {
          set((prev) => {
            const cleared = { ...prev.barsLoading.weekly };
            requestCodes.forEach((code) => {
              delete cleared[code];
            });
            return { barsLoading: { ...prev.barsLoading, weekly: cleared } };
          });
        }
      })();

      inFlightBatchRequests.set(requestKey, { promise: requestPromise, controller });
      requestPromise.finally(() => {
        const entry = inFlightBatchRequests.get(requestKey);
        if (entry?.controller === controller) {
          inFlightBatchRequests.delete(requestKey);
        }
      });
      return requestPromise;
    }

    const maSettings =
      timeframe === "daily" ? get().maSettings.daily : get().maSettings.monthly;
    const limit = Math.max(limitOverride ?? 0, getRequiredBars(maSettings));
    const requestCodes = [...new Set(trimmed)].sort();
    const requestKey = buildBatchKey(timeframe, limit, requestCodes);
    const cachedAt = recentBatchRequests.get(requestKey);
    if (cachedAt && Date.now() - cachedAt < BATCH_TTL_MS) return;

    const inFlight = inFlightBatchRequests.get(requestKey);
    if (inFlight) {
      counters.dedupHitCount += 1;
      return inFlight.promise;
    }

    counters.batchRequestCount += 1;
    counters.v3RequestCount += 1;
    console.debug("[batch_bars_v3]", {
      count: counters.batchRequestCount,
      v3_request_count: counters.v3RequestCount,
      coalesced_request_count: counters.coalescedRequestCount,
      dedup_hit_count: counters.dedupHitCount,
      key: requestKey,
      reason: reason ?? "unknown",
      timeframe,
      limit,
      codes: requestCodes.length
    });

    const controller = new AbortController();
    const requestPromise = (async () => {
      set((prev) => {
        const nextLoading = { ...prev.barsLoading[timeframe] };
        requestCodes.forEach((code) => {
          nextLoading[code] = true;
        });
        return {
          barsLoading: { ...prev.barsLoading, [timeframe]: nextLoading },
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                acc[code] = "loading";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        };
      });

      try {
        const requestPayload = {
          timeframes: [timeframe],
          codes: requestCodes,
          limit,
          includeProvisional: true
        };
        let res: { status: number; data?: any } | null = null;
        let attempt = 0;
        while (true) {
          try {
            res = await api.post("/batch_bars_v3", requestPayload, {
              signal: controller.signal,
              timeout: BATCH_REQUEST_TIMEOUT_MS
            });
            break;
          } catch (error) {
            const canRetry =
              attempt < BATCH_RETRY_DELAYS_MS.length && isRetriableBatchError(error);
            if (!canRetry) throw error;
            const retryDelay =
              BATCH_RETRY_DELAYS_MS[attempt] ??
              BATCH_RETRY_DELAYS_MS[BATCH_RETRY_DELAYS_MS.length - 1] ??
              0;
            attempt += 1;
            await sleepMs(retryDelay);
          }
        }
        if (!res) {
          throw new Error("batch_bars_v3 failed without response");
        }
        if (res.status !== 200) {
          throw new Error(`batch_bars_v3 failed with status ${res.status}`);
        }
        const rawItems = (res.data?.items || {}) as Record<string, MultiTimeframeBarsPayload | undefined>;
        const items: Record<string, BarsPayload> = {};
        const boxesMonthly: Record<string, Box[]> = {};
        const boxesDaily: Record<string, Box[]> = {};
        requestCodes.forEach((code) => {
          const framePayload = rawItems[code]?.[timeframe];
          const payload: BarsPayload =
            framePayload && Array.isArray(framePayload.bars)
              ? framePayload
              : {
                  bars: [],
                  ma: { ma7: [], ma20: [], ma60: [] },
                  boxes: []
                };
          items[code] = payload;
          const boxes = payload.boxes ?? [];
          if (timeframe === "monthly") {
            boxesMonthly[code] = boxes;
            boxesDaily[code] = boxes;
          } else if (timeframe === "daily") {
            boxesDaily[code] = boxes;
          }
        });
        requestCodes.forEach((code) => markFetchedLimit(timeframe, code, limit));
        recentBatchRequests.set(requestKey, Date.now());
        set((prev) => ({
          barsCache: {
            ...prev.barsCache,
            [timeframe]: { ...prev.barsCache[timeframe], ...items }
          },
          boxesCache: {
            monthly: { ...prev.boxesCache.monthly, ...boxesMonthly },
            weekly: prev.boxesCache.weekly,
            daily: { ...prev.boxesCache.daily, ...boxesDaily }
          },
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                const payload = items[code];
                acc[code] = payload && payload.bars.length ? "success" : "empty";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        }));
      } catch (error) {
        if (isAbortError(error)) return;
        set((prev) => ({
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                const cached = prev.barsCache[timeframe][code];
                acc[code] = cached ? (cached.bars.length ? "success" : "empty") : "error";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        }));
        throw error;
      } finally {
        set((prev) => {
          const cleared = { ...prev.barsLoading[timeframe] };
          requestCodes.forEach((code) => {
            delete cleared[code];
          });
          return { barsLoading: { ...prev.barsLoading, [timeframe]: cleared } };
        });
      }
    })();

    inFlightBatchRequests.set(requestKey, { promise: requestPromise, controller });
    requestPromise.finally(() => {
      const entry = inFlightBatchRequests.get(requestKey);
      if (entry?.controller === controller) {
        inFlightBatchRequests.delete(requestKey);
      }
    });
    return requestPromise;
  },
  loadBoxesBatch: async (codes) => {
    if (!codes.length) return;
    await get().loadBarsBatch("monthly", codes, undefined, "boxes");
  },
  ensureBarsForVisible: async (timeframe, codes, reason) => {
    const uniqueCodes = [...new Set(codes.filter((code) => code))];
    if (!uniqueCodes.length) return;
    const pending = ensurePendingCodes[timeframe];
    uniqueCodes.forEach((code) => pending.add(code));
    if (reason) {
      ensurePendingReason[timeframe] = reason;
    }

    return new Promise<void>((resolve, reject) => {
      ensurePendingWaiters[timeframe].push({ resolve, reject });
      if (ensureCoalesceTimers[timeframe] !== null) {
        counters.coalescedRequestCount += 1;
        return;
      }
      ensureCoalesceTimers[timeframe] = setTimeout(async () => {
        ensureCoalesceTimers[timeframe] = null;
        const mergedCodes = [...ensurePendingCodes[timeframe]];
        ensurePendingCodes[timeframe].clear();
        const mergedReason = ensurePendingReason[timeframe];
        ensurePendingReason[timeframe] = undefined;
        const waiters = ensurePendingWaiters[timeframe].splice(0);

        try {
          const state = get();
          const cache = state.barsCache[timeframe];
          const maSettings = state.maSettings;
          const requiredBars =
            timeframe === "daily"
              ? getRequiredBars(maSettings.daily)
              : timeframe === "weekly"
                ? getRequiredBars(maSettings.weekly)
                : getRequiredBars(maSettings.monthly);
          const requiredWithRange = Math.max(requiredBars, state.settings.listRangeBars);
          const listKey = buildBatchKey(timeframe, requiredWithRange, mergedCodes);
          if (lastEnsureKeyByTimeframe[timeframe] !== listKey) {
            abortInFlightForTimeframe(timeframe);
            lastEnsureKeyByTimeframe[timeframe] = listKey;
          }
          const missing = mergedCodes.filter((code) => {
            const payload = cache[code];
            const fetchedLimit = getFetchedLimit(timeframe, code);
            if (!payload) return fetchedLimit < requiredWithRange;
            if (payload.bars.length >= requiredWithRange) return false;
            if (fetchedLimit >= requiredWithRange) return false;
            return true;
          });
          if (!missing.length) {
            waiters.forEach((w) => w.resolve());
            return;
          }

          const batchSize = 48;
          for (let i = 0; i < missing.length; i += batchSize) {
            const batch = missing.slice(i, i + batchSize);
            await get().loadBarsBatch(
              timeframe,
              batch,
              requiredWithRange,
              mergedReason
            );
          }
          waiters.forEach((w) => w.resolve());
        } catch (error) {
          waiters.forEach((w) => w.reject(error));
        }
      }, ENSURE_COALESCE_MS);
    });
  },
  setColumns: (columns) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(GRID_COLS_KEY, String(columns));
    }
    set((state) => ({ settings: { ...state.settings, columns } }));
  },
  setRows: (rows) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(GRID_ROWS_KEY, String(rows));
    }
    set((state) => ({ settings: { ...state.settings, rows } }));
  },
  setListTimeframe: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_TIMEFRAME_KEY, value);
    }
    set((state) => ({ settings: { ...state.settings, listTimeframe: value } }));
  },
  setListRangeBars: (value) => {
    const normalized = LIST_RANGE_VALUES.includes(value as Settings["listRangeBars"])
      ? (value as Settings["listRangeBars"])
      : 120;
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_RANGE_KEY, String(normalized));
    }
    set((state) => ({ settings: { ...state.settings, listRangeBars: normalized } }));
  },
  setListColumns: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_COLS_KEY, String(value));
    }
    set((state) => ({ settings: { ...state.settings, listColumns: value } }));
  },
  setListRows: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_ROWS_KEY, String(value));
    }
    set((state) => ({ settings: { ...state.settings, listRows: value } }));
  },
  setSearch: (search) => {
    set((state) => ({ settings: { ...state.settings, search } }));
  },
  setGridScrollTop: (value) => {
    set((state) => ({ settings: { ...state.settings, gridScrollTop: value } }));
  },
  setGridTimeframe: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("gridTimeframe", value);
    }
    set((state) => ({ settings: { ...state.settings, gridTimeframe: value } }));
  },
  setShowBoxes: (value) => {
    set((state) => ({ settings: { ...state.settings, showBoxes: value } }));
  },
  setSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, sortKey: value } }));
  },
  setSortDir: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sortDir", value);
    }
    set((state) => ({ settings: { ...state.settings, sortDir: value } }));
  },
  // New separated sort setters
  setCandidateSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("candidateSortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, candidateSortKey: value } }));
  },
  setBasicSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("basicSortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, basicSortKey: value } }));
  },
  setBasicSortDir: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("basicSortDir", value);
    }
    set((state) => ({ settings: { ...state.settings, basicSortDir: value } }));
  },
  setPerformancePeriod: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("performancePeriod", value);
    }
    set((state) => ({ settings: { ...state.settings, performancePeriod: value } }));
  },
  updateMaSetting: (timeframe, index, patch) => {
    set((state) => {
      const current = state.maSettings[timeframe][index];
      if (!current) return state;
      const next = [...state.maSettings[timeframe]];
      const updated: MaSetting = {
        ...current,
        ...patch,
        period:
          Number.isFinite(Number(patch.period)) && Number(patch.period) > 0
            ? Math.floor(Number(patch.period))
            : current.period,
        color: normalizeColor(patch.color ?? current.color, current.color),
        lineWidth: normalizeLineWidth(patch.lineWidth ?? current.lineWidth, current.lineWidth),
        visible: typeof patch.visible === "boolean" ? patch.visible : current.visible
      };
      next[index] = updated;
      persistSettings(timeframe, next);
      return { maSettings: { ...state.maSettings, [timeframe]: next } };
    });
  },
  updateCompareMaSetting: (timeframe, index, patch) => {
    set((state) => {
      const current = state.compareMaSettings[timeframe][index];
      if (!current) return state;
      const next = [...state.compareMaSettings[timeframe]];
      const updated: MaSetting = {
        ...current,
        ...patch,
        period:
          Number.isFinite(Number(patch.period)) && Number(patch.period) > 0
            ? Math.floor(Number(patch.period))
            : current.period,
        color: normalizeColor(patch.color ?? current.color, current.color),
        lineWidth: normalizeLineWidth(patch.lineWidth ?? current.lineWidth, current.lineWidth),
        visible: typeof patch.visible === "boolean" ? patch.visible : current.visible
      };
      next[index] = updated;
      persistSettings(timeframe, next, COMPARE_MA_STORAGE_PREFIX);
      return { compareMaSettings: { ...state.compareMaSettings, [timeframe]: next } };
    });
  },
  resetMaSettings: (timeframe) => {
    set((state) => {
      const next = makeDefaultSettings(timeframe);
      persistSettings(timeframe, next);
      return { maSettings: { ...state.maSettings, [timeframe]: next } };
    });
  },
  resetCompareMaSettings: (timeframe) => {
    set((state) => {
      const next = makeDefaultSettings(timeframe);
      persistSettings(timeframe, next, COMPARE_MA_STORAGE_PREFIX);
      return { compareMaSettings: { ...state.compareMaSettings, [timeframe]: next } };
    });
  },
  resetBarsCache: () => {
    abortInFlightForTimeframe("daily");
    abortInFlightForTimeframe("weekly");
    abortInFlightForTimeframe("monthly");
    recentBatchRequests.clear();
    barsFetchedLimit.daily = {};
    barsFetchedLimit.weekly = {};
    barsFetchedLimit.monthly = {};
    lastEnsureKeyByTimeframe.daily = null;
    lastEnsureKeyByTimeframe.weekly = null;
    lastEnsureKeyByTimeframe.monthly = null;
    set(() => ({
      barsCache: { monthly: {}, weekly: {}, daily: {} },
      boxesCache: { monthly: {}, weekly: {}, daily: {} },
      barsStatus: { monthly: {}, weekly: {}, daily: {} },
      barsLoading: { monthly: {}, weekly: {}, daily: {} }
    }));
  },
  loadEventsMeta: async () => {
    if (get().eventsMetaLoading) return get().eventsMeta;
    set({ eventsMetaLoading: true });
    try {
      const res = await api.get("/events/meta");
      const meta = normalizeEventsMeta(res.data);
      if (meta) {
        set({ eventsMeta: meta });
      }
      return meta;
    } catch {
      return get().eventsMeta;
    } finally {
      set({ eventsMetaLoading: false });
    }
  },
  refreshEventsIfStale: async () => {
    const meta = await get().loadEventsMeta();
    if (!isEventsStale(meta)) return;
    if (meta?.isRefreshing) return;
    try {
      const res = await api.post("/events/refresh", null, {
        params: { reason: "startup_stale" }
      });
      const jobId =
        (res.data as { jobId?: string; refresh_job_id?: string } | null)?.jobId ??
        (res.data as { refresh_job_id?: string } | null)?.refresh_job_id ??
        null;
      if (jobId) {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: true,
            refreshJobId: jobId
          }
        }));
        void startEventsMetaPolling(get, set);
      } else {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: false,
            lastError: "refresh_job_missing",
            refreshJobId: null
          }
        }));
      }
    } catch {
      // ignore refresh failures
    }
  },
  refreshEvents: async () => {
    if (get().eventsMeta?.isRefreshing) return;
    try {
      const res = await api.post("/events/refresh", null, {
        params: { reason: "manual" }
      });
      const jobId =
        (res.data as { jobId?: string; refresh_job_id?: string } | null)?.jobId ??
        (res.data as { refresh_job_id?: string } | null)?.refresh_job_id ??
        null;
      if (!jobId) {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: false,
            lastError: "refresh_job_missing",
            refreshJobId: null
          }
        }));
        return;
      }
      set((prev) => ({
        eventsMeta: {
          ...(prev.eventsMeta ?? {
            earningsLastSuccessAt: null,
            rightsLastSuccessAt: null,
            lastAttemptAt: null,
            lastError: null,
            isRefreshing: false,
            refreshJobId: null
          }),
          isRefreshing: true,
          refreshJobId: jobId
        }
      }));
      void startEventsMetaPolling(get, set);
    } catch {
      set((prev) => ({
        eventsMeta: {
          ...(prev.eventsMeta ?? {
            earningsLastSuccessAt: null,
            rightsLastSuccessAt: null,
            lastAttemptAt: null,
            lastError: null,
            isRefreshing: false,
            refreshJobId: null
          }),
          isRefreshing: false,
          lastError: "refresh_failed"
        }
      }));
    } finally {
      void get().loadEventsMeta();
    }
  },

}));

setApiErrorReporter((info) => {
  useStore.getState().setLastApiError(info);
});

// Barrel re-exports for backward compatibility
export * from "./storeTypes";
