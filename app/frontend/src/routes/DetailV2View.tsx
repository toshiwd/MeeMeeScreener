import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import DetailChart from "../components/DetailChart";
import DataFreshnessBadges from "../components/DataFreshnessBadges";
import ProductStateNotice, { type ProductStateNoticeKind } from "../components/ProductStateNotice";
import { useBackendReadyState } from "../backendReady";
import { useStore } from "../store";
import type { MaSetting } from "../storeTypes";
import type { MeeMeeDataFreshnessContract } from "../dataFreshnessContract";
import {
  normalizeMeeMeeDataFreshnessContract,
} from "../dataFreshnessContract";
import {
  DEFAULT_LIMITS,
  normalizeTickerName,
} from "./detail/detailHelpers";
import {
  loadDetailChartPrefetch,
  prefetchDetailChartFrames,
  readDetailChartPrefetchSync,
  type ChartPrefetchEntry,
  type ChartPrefetchFrames,
} from "./detail/detailChartPrefetch";
import {
  buildDetailChartLifecycle,
  type DetailChartLifecycleFrame,
} from "./detail/detailChartLifecycle";
import type { FetchState, Timeframe } from "./detail/detailTypes";
import {
  buildDetailChartFrameModel,
  buildDetailChartPanelInput,
  getDetailFrameRows,
} from "./detail/detailChartFrameAdapter";
import {
  readDetailListBackPath,
  readDetailListCodes,
} from "./detail/detailNavigationContext";

const DETAIL_CHROME = {
  daily: { timeframe: "daily" as const },
  weekly: { timeframe: "weekly" as const },
  monthly: { timeframe: "monthly" as const },
};

const EMPTY_FETCH: FetchState = {
  status: "idle",
  responseCount: 0,
  errorMessage: null,
};

const loadingFetch = (count: number): FetchState => ({
  status: "loading",
  responseCount: count,
  errorMessage: null,
});

const successFetch = (count: number): FetchState => ({
  status: "success",
  responseCount: count,
  errorMessage: null,
});

const errorFetch = (message: string, count: number): FetchState => ({
  status: "error",
  responseCount: count,
  errorMessage: message,
});

const createEmptyFrames = (): ChartPrefetchFrames => ({
  daily: null,
  weekly: null,
  monthly: null,
  dataFreshnessContract: null,
});

const mergeFrames = (
  current: ChartPrefetchFrames,
  next: Partial<ChartPrefetchFrames>
): ChartPrefetchFrames => ({
  daily: next.daily ?? current.daily ?? null,
  weekly: next.weekly ?? current.weekly ?? null,
  monthly: next.monthly ?? current.monthly ?? null,
  dataFreshnessContract:
    next.dataFreshnessContract ?? current.dataFreshnessContract ?? null,
});

const noticeKindForStatus = (status: DetailChartLifecycleFrame["status"]): ProductStateNoticeKind => {
  if (status === "loading") return "loading";
  if (status === "error") return "error";
  if (status === "missing") return "missing";
  return "empty";
};

const timeframeTitle: Record<Timeframe, string> = {
  monthly: "Monthly",
  weekly: "Weekly",
  daily: "Daily",
};

const timeframeRole: Record<Timeframe, string> = {
  monthly: "environment",
  weekly: "trend",
  daily: "execution",
};

type FrameMap<T> = Record<Timeframe, T>;

function DetailV2ChartPanel({
  timeframe,
  frame,
  lifecycle,
  maSettings,
  showBoxes,
  contract,
}: {
  timeframe: Timeframe;
  frame: ChartPrefetchEntry | null;
  lifecycle: DetailChartLifecycleFrame;
  maSettings: MaSetting[];
  showBoxes: boolean;
  contract: MeeMeeDataFreshnessContract;
}) {
  const rows = getDetailFrameRows(frame);
  const model = useMemo(
    () => buildDetailChartFrameModel({ timeframe, rows, maSettings }),
    [maSettings, rows, timeframe]
  );
  const shouldRenderChart = model.shouldRenderChart;
  const shouldShowNotice = !shouldRenderChart && lifecycle.status !== "ready";
  const message =
    lifecycle.message ??
    (lifecycle.status === "loading" ? "Loading..." : "Chart data unavailable");

  return (
    <section className={`detail-v2-chart-panel is-${timeframe}`} data-testid={`detail-v2-chart-${timeframe}`}>
      <div className="detail-v2-chart-header">
        <div>
          <h2>{timeframeTitle[timeframe]}</h2>
          <span>{timeframeRole[timeframe]}</span>
        </div>
        <DataFreshnessBadges
          contract={contract}
          scope="chart"
          timeframe={timeframe}
          compact
        />
      </div>
      <div className="detail-v2-chart-body">
        {shouldRenderChart ? (
          <DetailChart
            candles={model.candles}
            volume={model.volume}
            maLines={model.chartMaLines}
            showVolume
            boxes={frame?.boxes ?? []}
            showBoxes={timeframe === "monthly" ? showBoxes : false}
            meeMeeDetailChrome={DETAIL_CHROME[timeframe]}
            meeMeeDetailChromeTerminalDates={null}
            drawingEnabled={false}
          />
        ) : null}
        {shouldShowNotice && (
          <ProductStateNotice
            kind={noticeKindForStatus(lifecycle.status)}
            prefix={timeframeTitle[timeframe]}
            className="detail-chart-empty"
          >
            {message}
          </ProductStateNotice>
        )}
      </div>
    </section>
  );
}

export default function DetailV2View() {
  const { code } = useParams<{ code: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { ready: backendReady } = useBackendReadyState();
  const tickers = useStore((state) => state.tickers);
  const loadingList = useStore((state) => state.loadingList);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const maSettings = useStore((state) => state.maSettings);
  const showBoxes = useStore((state) => state.settings.showBoxes);
  const [frames, setFrames] = useState<ChartPrefetchFrames>(() => createEmptyFrames());
  const [fetches, setFetches] = useState<FrameMap<FetchState>>({
    daily: EMPTY_FETCH,
    weekly: EMPTY_FETCH,
    monthly: EMPTY_FETCH,
  });
  const [errors, setErrors] = useState<FrameMap<string[]>>({
    daily: [],
    weekly: [],
    monthly: [],
  });

  const tickerByCode = useMemo(
    () => new Map(tickers.map((item) => [item.code, item])),
    [tickers]
  );
  const tickerName = normalizeTickerName(code ? tickerByCode.get(code)?.name : null);
  const listBackPath = useMemo(
    () => readDetailListBackPath(location.state as { from?: string } | null),
    [location.state]
  );
  const listCodes = useMemo(() => readDetailListCodes(), []);
  const currentIndex = code ? listCodes.indexOf(code) : -1;
  const previousCode = currentIndex > 0 ? listCodes[currentIndex - 1] : null;
  const nextCode =
    currentIndex >= 0 && currentIndex < listCodes.length - 1
      ? listCodes[currentIndex + 1]
      : null;

  useEffect(() => {
    if (!backendReady) return;
    if (!tickers.length && !loadingList) {
      void ensureListLoaded();
    }
  }, [backendReady, tickers.length, loadingList, ensureListLoaded]);

  useEffect(() => {
    if (!code || !backendReady) return;
    let cancelled = false;
    const request = {
      code,
      dailyLimit: DEFAULT_LIMITS.daily,
      weeklyLimit: DEFAULT_LIMITS.weekly,
      monthlyLimit: DEFAULT_LIMITS.monthly,
      asof: null,
    };
    const seed = readDetailChartPrefetchSync(request);
    setFrames(seed);
    setErrors({ daily: [], weekly: [], monthly: [] });
    setFetches({
      daily: loadingFetch(seed.daily?.rows.length ?? 0),
      weekly: loadingFetch(seed.weekly?.rows.length ?? 0),
      monthly: loadingFetch(seed.monthly?.rows.length ?? 0),
    });

    const applyFrames = (next: ChartPrefetchFrames, loading: boolean) => {
      if (cancelled) return;
      setFrames((current) => mergeFrames(current, next));
      setFetches({
        daily: loading ? loadingFetch(next.daily?.rows.length ?? 0) : successFetch(next.daily?.rows.length ?? 0),
        weekly: loading ? loadingFetch(next.weekly?.rows.length ?? 0) : successFetch(next.weekly?.rows.length ?? 0),
        monthly: loading ? loadingFetch(next.monthly?.rows.length ?? 0) : successFetch(next.monthly?.rows.length ?? 0),
      });
    };

    void loadDetailChartPrefetch(request)
      .then((cached) => {
        applyFrames(cached, true);
      })
      .catch(() => undefined);

    void prefetchDetailChartFrames(request, { forceNetwork: true })
      .then((fresh) => {
        applyFrames(fresh, false);
      })
      .catch((error) => {
        if (cancelled) return;
        const message = error instanceof Error ? error.message : String(error ?? "Chart data request failed");
        setErrors({ daily: [message], weekly: [message], monthly: [message] });
        setFetches((current) => ({
          daily: errorFetch(message, current.daily.responseCount),
          weekly: errorFetch(message, current.weekly.responseCount),
          monthly: errorFetch(message, current.monthly.responseCount),
        }));
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code]);

  const parsed = useMemo(
    () => ({
      daily: buildDetailChartFrameModel({
        timeframe: "daily",
        rows: getDetailFrameRows(frames.daily),
        maSettings: maSettings.daily,
      }),
      weekly: buildDetailChartFrameModel({
        timeframe: "weekly",
        rows: getDetailFrameRows(frames.weekly),
        maSettings: maSettings.weekly,
      }),
      monthly: buildDetailChartFrameModel({
        timeframe: "monthly",
        rows: getDetailFrameRows(frames.monthly),
        maSettings: maSettings.monthly,
      }),
    }),
    [frames.daily, frames.weekly, frames.monthly, maSettings.daily, maSettings.weekly, maSettings.monthly]
  );
  const contract = useMemo(
    () => normalizeMeeMeeDataFreshnessContract(frames.dataFreshnessContract),
    [frames.dataFreshnessContract]
  );
  const lifecycle = useMemo(
    () =>
      buildDetailChartLifecycle({
        daily: buildDetailChartPanelInput({
          fetch: fetches.daily,
          errors: errors.daily,
          candleCount: parsed.daily.candles.length,
          parseStats: parsed.daily.parseStats,
          loading: fetches.daily.status === "loading" && parsed.daily.candles.length === 0,
          pendingSwap: false,
        }),
        weekly: buildDetailChartPanelInput({
          fetch: fetches.weekly,
          errors: errors.weekly,
          candleCount: parsed.weekly.candles.length,
          parseStats: parsed.weekly.parseStats,
          loading: fetches.weekly.status === "loading" && parsed.weekly.candles.length === 0,
          pendingSwap: false,
        }),
        monthly: buildDetailChartPanelInput({
          fetch: fetches.monthly,
          errors: errors.monthly,
          candleCount: parsed.monthly.candles.length,
          parseStats: parsed.monthly.parseStats,
          loading: fetches.monthly.status === "loading" && parsed.monthly.candles.length === 0,
          pendingSwap: false,
        }),
        contract,
      }),
    [contract, errors, fetches, parsed]
  );

  const goToCode = (targetCode: string | null) => {
    if (!targetCode) return;
    navigate(`/detail-v2/${targetCode}`, { state: { from: listBackPath } });
  };

  const openOldDetail = () => {
    if (!code) return;
    navigate(`/detail/${code}`, { state: { from: listBackPath } });
  };

  return (
    <main className="detail-v2-shell" data-testid="detail-v2-shell">
      <header className="detail-v2-header">
        <div className="detail-v2-header-main">
          <button type="button" className="back nav-button nav-primary" onClick={() => navigate(listBackPath)}>
            Back to list
          </button>
          <div>
            <div className="detail-v2-kicker">Detail v2 shell</div>
            <h1>{code ?? "--"}</h1>
            <p>{tickerName || code || "--"}</p>
          </div>
        </div>
        <div className="detail-v2-nav">
          <button type="button" className="back nav-button" disabled={!previousCode} onClick={() => goToCode(previousCode)}>
            Previous
          </button>
          <button type="button" className="back nav-button" disabled={!nextCode} onClick={() => goToCode(nextCode)}>
            Next
          </button>
          <button type="button" className="back nav-button" onClick={openOldDetail}>
            Open old detail
          </button>
        </div>
      </header>

      <section className="detail-v2-status-strip">
        <span>detail</span>
        <DataFreshnessBadges contract={contract} scope="detail" compact />
      </section>

      {!backendReady && (
        <ProductStateNotice kind="loading" className="detail-v2-page-notice">
          Backend is not ready.
        </ProductStateNotice>
      )}

      <div className="detail-v2-chart-grid">
        {(["monthly", "weekly", "daily"] as Timeframe[]).map((timeframe) => (
          <DetailV2ChartPanel
            key={timeframe}
            timeframe={timeframe}
            frame={frames[timeframe]}
            lifecycle={lifecycle[timeframe]}
            maSettings={maSettings[timeframe]}
            showBoxes={showBoxes}
            contract={contract}
          />
        ))}
      </div>
    </main>
  );
}
