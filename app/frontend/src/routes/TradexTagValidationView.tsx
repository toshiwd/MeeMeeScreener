import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import TopNav from "../components/TopNav";

type TagRollupRow = {
  publish_id: string;
  as_of_date: string;
  side: string;
  holding_band: string;
  strategy_tag: string;
  observation_count: number;
  labeled_count: number;
  enter_count: number;
  wait_count: number;
  skip_count: number;
  expectancy_mean: number | null;
  adverse_mean: number | null;
  large_loss_rate: number | null;
  win_rate: number | null;
  teacher_alignment_mean: number | null;
  failure_count: number;
  readiness_hint: string;
  latest_failure_examples: string;
  worst_failure_examples: string;
  summary_json: string;
};

type TagSummaryResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  summary: {
    top_expectancy: TagRollupRow[];
    risk_heavy: TagRollupRow[];
    needs_samples: TagRollupRow[];
  };
};

type TagRowsResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  rows: TagRollupRow[];
};

type PromotionReviewSideRow = {
  side: string;
  compared_count: number;
  champion_enter_count: number;
  challenger_enter_count: number;
  expected_return_mean: number | null;
  adverse_move_mean: number | null;
  teacher_alignment_mean: number | null;
};

type PromotionReviewResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  review: {
    as_of_date: string | null;
    champion_version: string | null;
    challenger_version: string | null;
    sample_count: number;
    expectancy_delta: number | null;
    improved_expectancy: boolean;
    mae_non_worse: boolean;
    adverse_move_non_worse: boolean;
    stable_window: boolean;
    alignment_ok: boolean;
    readiness_pass: boolean;
    reason_codes: string[];
    summary: Record<string, unknown>;
    approval_decision?: {
      decision_id: string;
      decision: string;
      note: string | null;
      actor: string | null;
      created_at: string;
      summary: Record<string, unknown>;
    } | null;
    by_side: PromotionReviewSideRow[];
  } | null;
};

type PromotionReviewSummary = {
  champion_similarity?: number | null;
  challenger_similarity?: number | null;
  champion_tag_prior?: number | null;
  challenger_tag_prior?: number | null;
  champion_combo_prior?: number | null;
  challenger_combo_prior?: number | null;
};

type DailySummaryResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  daily_summary: {
    promotion: PromotionReviewResponse["review"];
    top_strategy: TagRollupRow | null;
    top_strategy_reason?: string | null;
    top_candle: TagRollupRow | null;
    top_candle_reason?: string | null;
    risk_watch: TagRollupRow | null;
    risk_watch_reason?: string | null;
    sample_watch: TagRollupRow | null;
    sample_watch_reason?: string | null;
  };
};

type TrendRow = {
  side: string;
  holding_band: string;
  strategy_tag: string;
  recent_expectancy: number;
  prior_expectancy: number;
  expectancy_delta: number;
  recent_risk: number;
  prior_risk: number;
  risk_delta: number;
  recent_labeled_count: number;
  teacher_signal_mean?: number | null;
  similarity_signal_mean?: number | null;
  last_as_of_date: string;
};

type TrendSummaryResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  trends: {
    improving: TrendRow[];
    weakening: TrendRow[];
    persistent_risk: TrendRow[];
  };
};

type DailySummaryHistoryRow = {
  publish_id: string;
  as_of_date: string;
  side_scope: string;
  top_strategy_tag: string | null;
  top_strategy_expectancy: number | null;
  top_candle_tag: string | null;
  top_candle_expectancy: number | null;
  risk_watch_tag: string | null;
  risk_watch_loss_rate: number | null;
  sample_watch_tag: string | null;
  sample_watch_labeled_count: number | null;
  promotion_ready: boolean | null;
  promotion_sample_count: number | null;
  summary_json: string;
};

type DailySummaryHistoryResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  rows: DailySummaryHistoryRow[];
};

type ActionQueueItem = {
  kind: string;
  priority: number;
  title: string;
  label: string;
  side?: string | null;
  strategy_tag?: string | null;
  holding_band?: string | null;
  metric_label?: string | null;
  metric_value?: number | null;
  note?: string | null;
};

type ActionQueueResponse = {
  degraded: boolean;
  degrade_reason?: string | null;
  publish_id: string | null;
  as_of_date: string | null;
  freshness_state: string | null;
  actions: ActionQueueItem[];
};

type ReplayProgressRun = {
  replay_id: string;
  status: string;
  start_as_of_date: string;
  end_as_of_date: string;
  total_days: number;
  completed_days: number;
  processed_days: number;
  remaining_days: number;
  success_days: number;
  failed_days: number;
  skipped_days: number;
  running_days: number;
  progress_pct: number;
  started_at?: string | null;
  finished_at?: string | null;
  last_completed_as_of_date?: string | null;
  last_heartbeat_at?: string | null;
  current_phase?: string | null;
  current_publish_id?: string | null;
  eta_seconds?: number | null;
  eta_at?: string | null;
  error_class?: string | null;
  current_day?: {
    as_of_date: string;
    publish_id?: string | null;
    started_at?: string | null;
  } | null;
};

type ReplayProgressResponse = {
  running: boolean;
  current_run: ReplayProgressRun | null;
  recent_runs: ReplayProgressRun[];
};

type SideFilter = "all" | "long" | "short";

type FailureExample = {
  code?: string;
  side?: string;
  holding_band?: string;
  strategy_tag?: string;
  as_of_date?: string;
  decision?: string;
  expected_return?: number;
  adverse_move?: number;
  teacher_alignment?: number;
};

type TradexDetailState = {
  from: "/tradex-tags";
  tradexTagContext: {
    side: string;
    holdingBand: string;
    strategyTag: string;
  };
};

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(digits);
};

const formatPct = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(1)}%`;
};

const formatSignedPct = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(1)}%`;
};

const formatHint = (value: string | null | undefined) => {
  switch (value) {
    case "promotable":
      return "昇格候補";
    case "risk_heavy":
      return "リスク高";
    case "needs_samples":
      return "サンプル不足";
    case "negative_expectancy":
      return "期待値マイナス";
    case "needs_labels":
      return "ラベル不足";
    default:
      return value || "--";
  }
};

const formatReplayStatus = (value: string | null | undefined) => {
  switch (value) {
    case "running":
      return "実行中";
    case "success":
      return "完了";
    case "partial_failure":
      return "一部失敗";
    case "failed":
      return "失敗";
    case "cancelled":
      return "停止";
    default:
      return value || "--";
  }
};

const formatReplayPhase = (value: string | null | undefined) => {
  switch (value) {
    case "bootstrap":
      return "初期構築";
    case "bootstrap_export":
      return "初期構築: エクスポート";
    case "bootstrap_export_prepare":
      return "初期構築: 準備";
    case "bootstrap_export_bars":
      return "初期構築: 日足コピー";
    case "bootstrap_export_indicators":
      return "初期構築: 指標コピー";
    case "bootstrap_export_patterns":
      return "初期構築: パターンコピー";
    case "bootstrap_export_commit":
      return "初期構築: 確定";
    case "bootstrap_labels":
      return "初期構築: ラベル";
    case "bootstrap_labels_h20":
      return "初期構築: ラベル20日";
    case "bootstrap_labels_completed":
      return "初期構築: ラベル完了";
    case "candidate":
      return "候補生成";
    case "similarity":
      return "類似チャート";
    case "challenger":
      return "チャレンジャー比較";
    case "day_started":
      return "営業日開始";
    case "day_completed":
      return "営業日完了";
    default:
      return value || "--";
  }
};

const formatDateTime = (value: string | null | undefined) => {
  if (!value) return "--";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString("ja-JP", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

const formatDuration = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return "--";
  const totalMinutes = Math.round(value / 60);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours <= 0) return `${minutes}分`;
  return `${hours}時間${minutes}分`;
};

const formatReplayRange = (run: ReplayProgressRun | null | undefined) => {
  if (!run) return "--";
  return `${run.start_as_of_date} -> ${run.end_as_of_date} / ${run.total_days}日`;
};

const formatSideLabel = (value: SideFilter) => {
  switch (value) {
    case "all":
      return "全体";
    case "long":
      return "買い";
    case "short":
      return "売り";
    default:
      return value;
  }
};

const parseFailureCount = (value: string | null | undefined) => {
  if (!value) return 0;
  try {
    const parsed = JSON.parse(value) as Array<unknown>;
    return Array.isArray(parsed) ? parsed.length : 0;
  } catch {
    return 0;
  }
};

const parseFailureExamples = (value: string | null | undefined): FailureExample[] => {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed) ? (parsed as FailureExample[]) : [];
  } catch {
    return [];
  }
};

const buildDetailState = (item: FailureExample): TradexDetailState => ({
  from: "/tradex-tags",
  tradexTagContext: {
    side: String(item.side || ""),
    holdingBand: String(item.holding_band || ""),
    strategyTag: String(item.strategy_tag || "")
  }
});

export default function TradexTagValidationView() {
  const { ready } = useBackendReadyState();
  const [side, setSide] = useState<SideFilter>("all");
  const [search, setSearch] = useState("");
  const [hintFilter, setHintFilter] = useState("all");
  const [bandFilter, setBandFilter] = useState("all");
  const [selectedTagKey, setSelectedTagKey] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<TagSummaryResponse | null>(null);
  const [detail, setDetail] = useState<TagRowsResponse | null>(null);
  const [promotionReview, setPromotionReview] = useState<PromotionReviewResponse | null>(null);
  const [candleSummary, setCandleSummary] = useState<TagSummaryResponse | null>(null);
  const [candleComboSummary, setCandleComboSummary] = useState<TagSummaryResponse | null>(null);
  const [candleComboTrendSummary, setCandleComboTrendSummary] = useState<TrendSummaryResponse | null>(null);
  const [dailySummary, setDailySummary] = useState<DailySummaryResponse | null>(null);
  const [dailyHistory, setDailyHistory] = useState<DailySummaryHistoryResponse | null>(null);
  const [trendSummary, setTrendSummary] = useState<TrendSummaryResponse | null>(null);
  const [actionQueue, setActionQueue] = useState<ActionQueueResponse | null>(null);
  const [replayProgress, setReplayProgress] = useState<ReplayProgressResponse | null>(null);

  useEffect(() => {
    if (!ready) return;
    let active = true;
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const params = side === "all" ? {} : { side };
        const [summaryRes, detailRes, reviewRes, candleRes, comboRes, dailyRes, historyRes, trendRes, comboTrendRes, actionQueueRes, replayProgressRes] = await Promise.all([
          api.get<TagSummaryResponse>("/analysis-bridge/internal/state-eval-tags/summary", { params }),
          api.get<TagRowsResponse>("/analysis-bridge/internal/state-eval-tags", { params: { ...params, limit: 80 } }),
          api.get<PromotionReviewResponse>("/analysis-bridge/internal/state-eval-promotion-review"),
          api.get<TagSummaryResponse>("/analysis-bridge/internal/state-eval-candles/summary", { params }),
          api.get<TagSummaryResponse>("/analysis-bridge/internal/state-eval-candle-combos/summary", { params }),
          api.get<DailySummaryResponse>("/analysis-bridge/internal/state-eval-daily-summary", { params }),
          api.get<DailySummaryHistoryResponse>("/analysis-bridge/internal/state-eval-daily-summary/history", { params: { ...params, limit: 14 } }),
          api.get<TrendSummaryResponse>("/analysis-bridge/internal/state-eval-trends", { params: { ...params, lookback: 14, limit: 4 } }),
          api.get<TrendSummaryResponse>("/analysis-bridge/internal/state-eval-candle-combo-trends", { params: { ...params, lookback: 14, limit: 4 } }),
          api.get<ActionQueueResponse>("/analysis-bridge/internal/state-eval-action-queue", { params }),
          api.get<ReplayProgressResponse>("/analysis-bridge/internal/replay-progress")
        ]);
        if (!active) return;
        setSummary(summaryRes.data);
        setDetail(detailRes.data);
        setPromotionReview(reviewRes.data);
        setCandleSummary(candleRes.data);
        setCandleComboSummary(comboRes.data);
        setDailySummary(dailyRes.data);
        setDailyHistory(historyRes.data);
        setTrendSummary(trendRes.data);
        setCandleComboTrendSummary(comboTrendRes.data);
        setActionQueue(actionQueueRes.data);
        setReplayProgress(replayProgressRes.data);
        setSelectedTagKey((current) => {
          if (!current) return null;
          const exists = (detailRes.data.rows ?? []).some(
            (row) => `${row.side}:${row.holding_band}:${row.strategy_tag}` === current
          );
          return exists ? current : null;
        });
      } catch (err) {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Failed to load tag validation.");
      } finally {
        if (active) setLoading(false);
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [ready, side]);

  const csvHref = useMemo(() => {
    const params = new URLSearchParams();
    if (side !== "all") params.set("side", side);
    params.set("limit", "200");
    const base = window.MEEMEE_API_BASE || "/api";
    return `${base}/analysis-bridge/internal/state-eval-tags.csv?${params.toString()}`;
  }, [side]);
  const dailyCsvHref = useMemo(() => {
    const params = new URLSearchParams();
    if (side !== "all") params.set("side", side);
    params.set("limit", "60");
    const base = window.MEEMEE_API_BASE || "/api";
    return `${base}/analysis-bridge/internal/state-eval-daily-summary.csv?${params.toString()}`;
  }, [side]);

  const publishedAt = detail?.as_of_date ?? summary?.as_of_date ?? "--";
  const freshness = detail?.freshness_state ?? summary?.freshness_state ?? "--";
  const rows = detail?.rows ?? [];
  const searchedRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    return rows.filter((row) => {
      if (hintFilter !== "all" && row.readiness_hint !== hintFilter) return false;
      if (bandFilter !== "all" && row.holding_band !== bandFilter) return false;
      if (!query) return true;
      return (
        row.strategy_tag.toLowerCase().includes(query) ||
        row.holding_band.toLowerCase().includes(query) ||
        row.side.toLowerCase().includes(query)
      );
    });
  }, [rows, search, hintFilter, bandFilter]);
  const filteredRows = useMemo(() => {
    if (!selectedTagKey) return searchedRows;
    return searchedRows.filter((row) => `${row.side}:${row.holding_band}:${row.strategy_tag}` === selectedTagKey);
  }, [searchedRows, selectedTagKey]);
  const selectedRow = filteredRows[0] ?? null;
  const latestExamples = useMemo(
    () => parseFailureExamples(selectedRow?.latest_failure_examples),
    [selectedRow?.latest_failure_examples]
  );
  const worstExamples = useMemo(
    () => parseFailureExamples(selectedRow?.worst_failure_examples),
    [selectedRow?.worst_failure_examples]
  );

  const summarySections = useMemo(
    () => [
      {
        key: "top_expectancy",
        title: "期待値上位",
        caption: "昇格候補の中で期待値が高いタグです。",
        rows: summary?.summary?.top_expectancy ?? []
      },
      {
        key: "risk_heavy",
        title: "リスク高",
        caption: "大損率が高い、または期待値が弱いタグです。",
        rows: summary?.summary?.risk_heavy ?? []
      },
      {
        key: "needs_samples",
        title: "要サンプル",
        caption: "まだ履歴件数が足りないタグです。",
        rows: summary?.summary?.needs_samples ?? []
      }
    ],
    [summary]
  );
  const candleSections = useMemo(
    () => [
      {
        key: "candle_top_expectancy",
        title: "足形上位",
        caption: "足形タグの中で期待値が高いものです。",
        rows: candleSummary?.summary?.top_expectancy ?? []
      },
      {
        key: "candle_risk_heavy",
        title: "足形リスク",
        caption: "失敗や大損率が高い足形タグです。",
        rows: candleSummary?.summary?.risk_heavy ?? []
      },
      {
        key: "candle_needs_samples",
        title: "足形要サンプル",
        caption: "履歴件数がまだ少ない足形タグです。",
        rows: candleSummary?.summary?.needs_samples ?? []
      }
    ],
    [candleSummary]
  );
  const candleComboSections = useMemo(
    () => [
      {
        key: "combo_top_expectancy",
        title: "コンボ上位",
        caption: "2本足・3本足コンボの中で期待値が高いものです。",
        rows: candleComboSummary?.summary?.top_expectancy ?? []
      },
      {
        key: "combo_risk_heavy",
        title: "コンボリスク",
        caption: "失敗や踏み上げが多いコンボです。",
        rows: candleComboSummary?.summary?.risk_heavy ?? []
      },
      {
        key: "combo_needs_samples",
        title: "コンボ要サンプル",
        caption: "履歴件数がまだ少ないコンボです。",
        rows: candleComboSummary?.summary?.needs_samples ?? []
      }
    ],
    [candleComboSummary]
  );
  const hintOptions = useMemo(() => {
    return ["all", ...new Set(rows.map((row) => row.readiness_hint))];
  }, [rows]);
  const bandOptions = useMemo(() => {
    return ["all", ...new Set(rows.map((row) => row.holding_band))];
  }, [rows]);
  const selectedMetrics = useMemo(() => {
    if (!selectedRow) return null;
    return [
      { label: "期待値", value: formatPct(selectedRow.expectancy_mean) },
      { label: "逆行", value: formatPct(selectedRow.adverse_mean) },
      { label: "大損率", value: formatPct(selectedRow.large_loss_rate) },
      { label: "勝率", value: formatPct(selectedRow.win_rate) },
      { label: "教師", value: formatNumber(selectedRow.teacher_alignment_mean, 2) },
      { label: "件数", value: `${selectedRow.labeled_count}` }
    ];
  }, [selectedRow]);
  const review = promotionReview?.review ?? null;
  const latestApproval = review?.approval_decision ?? null;
  const dailyOverview = dailySummary?.daily_summary ?? null;
  const actions = actionQueue?.actions ?? [];
  const currentReplay = replayProgress?.current_run ?? null;
  const reviewSummary = (review?.summary ?? {}) as PromotionReviewSummary;
  const reviewPriorMetrics = useMemo(
    () => [
      {
        label: "タグ根拠",
        champion: reviewSummary.champion_tag_prior ?? null,
        challenger: reviewSummary.challenger_tag_prior ?? null,
      },
      {
        label: "Combo Prior",
        champion: reviewSummary.champion_combo_prior ?? null,
        challenger: reviewSummary.challenger_combo_prior ?? null,
      },
      {
        label: "Similarity",
        champion: reviewSummary.champion_similarity ?? null,
        challenger: reviewSummary.challenger_similarity ?? null,
      },
    ],
    [
      reviewSummary.champion_combo_prior,
      reviewSummary.champion_similarity,
      reviewSummary.champion_tag_prior,
      reviewSummary.challenger_combo_prior,
      reviewSummary.challenger_similarity,
      reviewSummary.challenger_tag_prior,
    ]
  );
  const reviewGateChecks = useMemo(() => {
    if (!review) return [];
    return [
      { label: "期待値", ok: review.improved_expectancy },
      { label: "MAE", ok: review.mae_non_worse },
      { label: "逆行", ok: review.adverse_move_non_worse },
      { label: "安定性", ok: review.stable_window },
      { label: "教師", ok: review.alignment_ok }
    ];
  }, [review]);

  const handleSelectTag = (row: TagRollupRow) => {
    const key = `${row.side}:${row.holding_band}:${row.strategy_tag}`;
    setSelectedTagKey((current) => (current === key ? null : key));
    if (side !== "all" && row.side !== side) {
      setSide(row.side as SideFilter);
    }
  };

  const handleActionSelect = (item: ActionQueueItem) => {
    const matched = rows.find(
      (row) =>
        (!item.side || row.side === item.side) &&
        (!item.holding_band || row.holding_band === item.holding_band) &&
        (!item.strategy_tag || row.strategy_tag === item.strategy_tag)
    );
    if (matched) {
      handleSelectTag(matched);
    }
  };

  return (
    <div className="app-shell tradex-tag-view">
      <div className="dynamic-header">
        <div className="dynamic-header-row header-row-top">
          <div className="header-row-left">
            <TopNav />
          </div>
        </div>
        <div className="dynamic-header-row header-row-bottom">
          <div className="header-title-group">
            <div className="header-nav-title">
              <span className="header-brand">Tradex 研究状況</span>
            </div>
            <span className="updates-label">タグ検証、進捗、研究結果の確認画面</span>
          </div>
          <div className="tradex-tag-toolbar">
            <div className="tradex-tag-segmented" role="tablist" aria-label="売買フィルター">
              {(["all", "long", "short"] as SideFilter[]).map((value) => (
                <button
                  key={value}
                  type="button"
                  className={side === value ? "is-active" : ""}
                  onClick={() => setSide(value)}
                >
                  {formatSideLabel(value)}
                </button>
              ))}
            </div>
            <a className="tradex-tag-export" href={csvHref}>
              CSV出力
            </a>
            <a className="tradex-tag-export" href={dailyCsvHref}>
              日次CSV出力
            </a>
          </div>
        </div>
      </div>

      <main className="tradex-tag-main">
        <section className="tradex-tag-panel">
          {currentReplay ? (
            <section className="tradex-replay-panel">
              <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">研究進捗</div>
                  <div className="tradex-tag-card-caption">
                    {currentReplay.status === "running"
                      ? "Tradex の replay 実行状況です。現在の進捗と完了予測を表示します。"
                      : "直近の replay 実行結果です。"}
                  </div>
                </div>
                <span className={`tradex-replay-status is-${currentReplay.status}`}>{formatReplayStatus(currentReplay.status)}</span>
              </div>
                <div className="tradex-replay-meta">
                  <strong>{currentReplay.replay_id}</strong>
                  <span>{formatReplayRange(currentReplay)}</span>
                </div>
              <div className="tradex-replay-meta">
                <span>現在処理: {formatReplayPhase(currentReplay.current_phase)}</span>
                <span>最終更新: {formatDateTime(currentReplay.last_heartbeat_at)}</span>
              </div>
              <div className="tradex-replay-meta">
                <span>予測完了: {formatDateTime(currentReplay.eta_at)}</span>
                <span>残り目安: {formatDuration(currentReplay.eta_seconds)}</span>
              </div>
              <div className="tradex-replay-progress-bar" aria-hidden="true">
                <span style={{ width: `${Math.max(0, Math.min(currentReplay.progress_pct, 100))}%` }} />
              </div>
              <div className="tradex-replay-stats">
                <div className="tradex-replay-stat">
                  <span>進捗</span>
                  <strong>{currentReplay.progress_pct.toFixed(1)}%</strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>完了</span>
                  <strong>
                    {currentReplay.completed_days} / {currentReplay.total_days}
                  </strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>成功</span>
                  <strong>{currentReplay.success_days}</strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>失敗</span>
                  <strong>{currentReplay.failed_days}</strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>実行中日数</span>
                  <strong>{currentReplay.running_days}</strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>現在日付</span>
                  <strong>{currentReplay.current_day?.as_of_date ?? currentReplay.last_completed_as_of_date ?? "--"}</strong>
                </div>
                <div className="tradex-replay-stat">
                  <span>公開ID</span>
                  <strong>{currentReplay.current_publish_id ?? currentReplay.current_day?.publish_id ?? "--"}</strong>
                </div>
              </div>
            </section>
          ) : null}
          {!!actions.length && (
            <section className="tradex-action-queue-panel">
              <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">今日の注目項目</div>
                  <div className="tradex-tag-card-caption">
                    今の publish で優先して見るべき項目です。
                  </div>
                </div>
              </div>
              <div className="tradex-action-queue-grid">
                {actions.map((item) => (
                  <button
                    key={`${item.kind}:${item.title}:${item.strategy_tag ?? "none"}`}
                    type="button"
                    className="tradex-action-card"
                    onClick={() => handleActionSelect(item)}
                  >
                    <div className="tradex-action-card-head">
                      <span className={`tradex-action-label is-${item.label.toLowerCase()}`}>{item.label}</span>
                      <span className="tradex-action-priority">P{item.priority}</span>
                    </div>
                    <strong>{item.title}</strong>
                    <span className="tradex-action-tag">{item.strategy_tag ?? "--"}</span>
                    <div className="tradex-action-meta">
                      <span>{item.side ?? "全体"}</span>
                      <span>{item.holding_band ?? "--"}</span>
                    </div>
                    <div className="tradex-action-stat">
                      <span>{item.metric_label ?? "--"}</span>
                      <strong>
                        {item.metric_label?.toLowerCase().includes("sample")
                          ? `${item.metric_value ?? "--"}`
                          : item.metric_label?.toLowerCase().includes("loss")
                            ? formatPct(item.metric_value)
                            : formatSignedPct(item.metric_value)}
                      </strong>
                    </div>
                    {item.note ? <small>{item.note}</small> : null}
                  </button>
                ))}
              </div>
            </section>
          )}

          {dailyOverview && (
            <section className="tradex-daily-overview">
              {[
                {
                  title: "注目タグ",
                  row: dailyOverview.top_strategy,
                  value: dailyOverview.top_strategy ? formatPct(dailyOverview.top_strategy.expectancy_mean) : "--",
                  note: dailyOverview.top_strategy_reason
                },
                {
                  title: "注目足形",
                  row: dailyOverview.top_candle,
                  value: dailyOverview.top_candle ? formatPct(dailyOverview.top_candle.expectancy_mean) : "--",
                  note: dailyOverview.top_candle_reason
                },
                {
                  title: "要注意",
                  row: dailyOverview.risk_watch,
                  value: dailyOverview.risk_watch ? formatPct(dailyOverview.risk_watch.large_loss_rate) : "--",
                  note: dailyOverview.risk_watch_reason
                },
                {
                  title: "要サンプル",
                  row: dailyOverview.sample_watch,
                  value: dailyOverview.sample_watch ? `${dailyOverview.sample_watch.labeled_count}` : "--",
                  note: dailyOverview.sample_watch_reason
                }
              ].map((item) => (
                <button
                  key={item.title}
                  type="button"
                  className="tradex-daily-card"
                  onClick={() => item.row && handleSelectTag(item.row)}
                  disabled={!item.row}
                >
                  <span>{item.title}</span>
                  <strong>{item.row?.strategy_tag ?? "--"}</strong>
                  <em>{item.value}</em>
                  {item.note ? <small>{item.note}</small> : null}
                </button>
              ))}
            </section>
          )}

          {!!trendSummary && (
            <section className="tradex-daily-history-panel">
              <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">変化監視</div>
                  <div className="tradex-tag-card-caption">
                    直近の研究期間で改善中、悪化中、継続リスクのタグを見ます。
                  </div>
                </div>
              </div>
              <div className="tradex-tag-summary-grid">
                {[
                  { title: "改善中", rows: trendSummary.trends.improving, metric: "exp" },
                  { title: "悪化中", rows: trendSummary.trends.weakening, metric: "risk" },
                  { title: "継続リスク", rows: trendSummary.trends.persistent_risk, metric: "risk" }
                ].map((section) => (
                  <article key={section.title} className="tradex-tag-card">
                    <div className="tradex-tag-card-header">
                      <div>
                        <div className="tradex-tag-card-title">{section.title}</div>
                      </div>
                    </div>
                    <div className="tradex-tag-card-list">
                      {section.rows.length ? section.rows.map((row) => (
                        <button
                          key={`${section.title}:${row.side}:${row.holding_band}:${row.strategy_tag}`}
                          type="button"
                          className="tradex-tag-card-row"
                          onClick={() => {
                            const matched = rows.find((item) => `${item.side}:${item.holding_band}:${item.strategy_tag}` === `${row.side}:${row.holding_band}:${row.strategy_tag}`);
                            if (matched) handleSelectTag(matched);
                          }}
                        >
                          <div className="tradex-tag-card-tag">
                            <span>{row.strategy_tag}</span>
                            <span className="tradex-tag-badge">{row.side}</span>
                            <span className="tradex-tag-band">{row.holding_band}</span>
                          </div>
                          <div className="tradex-tag-card-stats">
                            <span>期待値差 {formatSignedPct(row.expectancy_delta)}</span>
                            <span>リスク差 {formatSignedPct(row.risk_delta)}</span>
                            <span>N {row.recent_labeled_count}</span>
                          </div>
                        </button>
                      )) : <div className="tradex-tag-empty">データなし</div>}
                    </div>
                  </article>
                ))}
              </div>
            </section>
          )}

          {!!dailyHistory?.rows?.length && (
            <section className="tradex-daily-history-panel">
              <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">日次履歴</div>
                  <div className="tradex-tag-card-caption">
                    直近の研究スナップショット履歴です。
                  </div>
                </div>
              </div>
              <div className="tradex-daily-history-list">
                {dailyHistory.rows.map((row) => (
                  <button
                    key={`${row.publish_id}:${row.side_scope}`}
                    type="button"
                    className="tradex-daily-history-row"
                    onClick={() => {
                      const tag = row.top_strategy_tag || row.top_candle_tag || row.risk_watch_tag || row.sample_watch_tag;
                      const matched = rows.find((item) => item.strategy_tag === tag);
                      if (matched) handleSelectTag(matched);
                    }}
                  >
                    <span>{row.as_of_date}</span>
                    <strong>{row.top_strategy_tag || "--"}</strong>
                    <span>足形 {row.top_candle_tag || "--"}</span>
                    <span>注意 {row.risk_watch_tag || "--"}</span>
                    <span>{row.promotion_ready ? "候補" : "保留"}</span>
                  </button>
                ))}
              </div>
            </section>
          )}

          <div className="tradex-tag-meta">
            <span>基準日: {publishedAt}</span>
            <span>鮮度: {freshness}</span>
            <span>件数: {filteredRows.length.toLocaleString("ja-JP")} / {searchedRows.length.toLocaleString("ja-JP")} / {rows.length.toLocaleString("ja-JP")}</span>
            {selectedRow && <span>選択中: {selectedRow.strategy_tag}</span>}
            {dailyOverview?.promotion && <span>昇格判定: {dailyOverview.promotion.readiness_pass ? "候補" : "保留"}</span>}
          </div>

          <div className="tradex-tag-filters">
            <label className="tradex-tag-filter">
              <span>検索</span>
              <input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="タグ / 保有帯 / 売買"
              />
            </label>
            <label className="tradex-tag-filter">
              <span>判定</span>
              <select value={hintFilter} onChange={(event) => setHintFilter(event.target.value)}>
                {hintOptions.map((value) => (
                  <option key={value} value={value}>
                    {value === "all" ? "全体" : formatHint(value)}
                  </option>
                ))}
              </select>
            </label>
            <label className="tradex-tag-filter">
              <span>保有帯</span>
              <select value={bandFilter} onChange={(event) => setBandFilter(event.target.value)}>
                {bandOptions.map((value) => (
                  <option key={value} value={value}>
                    {value === "all" ? "全体" : value}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="tradex-tag-reset"
              onClick={() => {
                setSearch("");
                setHintFilter("all");
                setBandFilter("all");
                setSelectedTagKey(null);
              }}
            >
              リセット
            </button>
          </div>

          {!ready && <div className="tradex-tag-status">バックエンド起動待ちです。</div>}
          {ready && loading && <div className="tradex-tag-status">研究データを読み込み中です。</div>}
          {error && <div className="tradex-tag-error">{error}</div>}

          {selectedRow && selectedMetrics && (
            <section className="tradex-tag-focus-panel">
              <div className="tradex-tag-focus-head">
                <div>
                  <div className="tradex-tag-card-title">{selectedRow.strategy_tag}</div>
                  <div className="tradex-tag-card-caption">
                    {selectedRow.side} / {selectedRow.holding_band} / {formatHint(selectedRow.readiness_hint)}
                  </div>
                </div>
                <div className="tradex-tag-focus-stats">
                  {selectedMetrics.map((item) => (
                    <div key={item.label} className="tradex-tag-focus-stat">
                      <span>{item.label}</span>
                      <strong>{item.value}</strong>
                    </div>
                  ))}
                </div>
              </div>
            </section>
          )}

          {review && (
            <section className={review.readiness_pass ? "tradex-review-panel is-pass" : "tradex-review-panel is-hold"}>
              <div className="tradex-review-head">
                <div>
                  <div className="tradex-tag-card-title">昇格レビュー</div>
                  <div className="tradex-tag-card-caption">
                    {review.champion_version || "--"} {"->"} {review.challenger_version || "--"}
                  </div>
                </div>
                <div className={review.readiness_pass ? "tradex-review-badge is-pass" : "tradex-review-badge is-hold"}>
                  {review.readiness_pass ? "確認候補" : "保留"}
                </div>
              </div>

              <div className="tradex-review-metrics">
                <div className="tradex-review-metric">
                  <span>期待値差</span>
                  <strong>{formatSignedPct(review.expectancy_delta)}</strong>
                </div>
                <div className="tradex-review-metric">
                  <span>件数</span>
                  <strong>{review.sample_count}</strong>
                </div>
                <div className="tradex-review-metric">
                  <span>基準日</span>
                  <strong>{review.as_of_date || publishedAt}</strong>
                </div>
              </div>

              <div className="tradex-review-prior-grid">
                {reviewPriorMetrics.map((item) => {
                  const champion = item.champion;
                  const challenger = item.challenger;
                  const delta =
                    typeof champion === "number" && typeof challenger === "number"
                      ? challenger - champion
                      : null;
                  return (
                    <div key={item.label} className="tradex-review-prior-card">
                      <span className="tradex-review-prior-title">{item.label}</span>
                      <div className="tradex-review-prior-values">
                        <span>現行 {formatNumber(champion, 2)}</span>
                        <span>候補 {formatNumber(challenger, 2)}</span>
                      </div>
                      <strong className={delta != null && delta >= 0 ? "tradex-review-prior-delta is-up" : "tradex-review-prior-delta is-down"}>
                        {delta == null ? "--" : `${delta >= 0 ? "+" : ""}${delta.toFixed(2)}`}
                      </strong>
                    </div>
                  );
                })}
              </div>

              <div className="tradex-review-checks">
                {reviewGateChecks.map((item) => (
                  <span key={item.label} className={item.ok ? "tradex-review-check is-pass" : "tradex-review-check is-fail"}>
                    {item.label}: {item.ok ? "通過" : "未通過"}
                  </span>
                ))}
              </div>

              <div className="tradex-review-approval-panel">
                <div className="tradex-review-approval-head">
                  <div>
                    <div className="tradex-tag-card-title">記録済み判断</div>
                    <div className="tradex-tag-card-caption">
                      昇格判断は Codex / CLI で記録します。MeeMee は最新状態だけを表示します。
                    </div>
                  </div>
                  <div className={`tradex-review-approval-badge is-${latestApproval?.decision ?? "hold"}`}>
                    {latestApproval?.decision === "approved"
                      ? "承認"
                      : latestApproval?.decision === "rejected"
                        ? "却下"
                        : latestApproval?.decision === "hold"
                          ? "保留"
                          : "未記録"}
                  </div>
                </div>
                {latestApproval && (
                  <div className="tradex-review-approval-meta">
                    <span>{latestApproval.created_at}</span>
                    <span>{latestApproval.actor ?? "手動"}</span>
                    {latestApproval.note ? <span>{latestApproval.note}</span> : null}
                  </div>
                )}
                <div className="tradex-review-approval-help">
                  `python -m external_analysis promotion-decision-run --decision approved|hold|rejected --note \"...\"` を使います
                </div>
              </div>

              {review.reason_codes.length > 0 && (
                <div className="tradex-review-reasons">
                  {review.reason_codes.map((reason) => (
                    <span key={reason} className="tradex-review-reason">
                      {reason}
                    </span>
                  ))}
                </div>
              )}

              <div className="tradex-review-side-grid">
                {review.by_side.map((item) => (
                  <article key={item.side} className="tradex-review-side-card">
                    <div className="tradex-review-side-head">
                      <span className="tradex-tag-badge">{item.side}</span>
                      <span>{item.compared_count} 件比較</span>
                    </div>
                    <div className="tradex-review-side-stats">
                      <span>現行エントリー {item.champion_enter_count}</span>
                      <span>候補エントリー {item.challenger_enter_count}</span>
                      <span>期待値 {formatPct(item.expected_return_mean)}</span>
                      <span>逆行 {formatPct(item.adverse_move_mean)}</span>
                      <span>教師 {formatNumber(item.teacher_alignment_mean, 2)}</span>
                    </div>
                  </article>
                ))}
              </div>
            </section>
          )}

          <section className="tradex-candle-panel">
            <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">足形研究</div>
                  <div className="tradex-tag-card-caption">
                    足形タグだけを切り出した期待値とリスクの一覧です。
                  </div>
                </div>
              </div>
            <div className="tradex-tag-summary-grid">
              {candleSections.map((section) => (
                <article key={section.key} className="tradex-tag-card">
                  <div className="tradex-tag-card-header">
                    <div>
                      <div className="tradex-tag-card-title">{section.title}</div>
                      <div className="tradex-tag-card-caption">{section.caption}</div>
                    </div>
                  </div>
                  <div className="tradex-tag-card-list">
                    {section.rows.length ? (
                      section.rows.map((row) => (
                        <button
                          key={`${section.key}:${row.side}:${row.strategy_tag}`}
                          type="button"
                          className={
                            selectedTagKey === `${row.side}:${row.holding_band}:${row.strategy_tag}`
                              ? "tradex-tag-card-row is-selected"
                              : "tradex-tag-card-row"
                          }
                          onClick={() => handleSelectTag(row)}
                        >
                          <div className="tradex-tag-card-tag">
                            <span>{row.strategy_tag}</span>
                            <span className="tradex-tag-badge">{row.side}</span>
                            <span className="tradex-tag-band">{row.holding_band}</span>
                          </div>
                          <div className="tradex-tag-card-stats">
                            <span>期待値 {formatPct(row.expectancy_mean)}</span>
                            <span>リスク {formatPct(row.large_loss_rate)}</span>
                            <span>N {row.labeled_count}</span>
                          </div>
                        </button>
                      ))
                    ) : (
                      <div className="tradex-tag-empty">足形データなし</div>
                    )}
                  </div>
                </article>
              ))}
            </div>
          </section>

          <section className="tradex-candle-panel">
            <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">足形コンボ研究</div>
                  <div className="tradex-tag-card-caption">
                    2本足、3本足の組み合わせを同じ集計で見ます。
                  </div>
                </div>
              </div>
            <div className="tradex-tag-summary-grid">
              {candleComboSections.map((section) => (
                <article key={section.key} className="tradex-tag-card">
                  <div className="tradex-tag-card-header">
                    <div>
                      <div className="tradex-tag-card-title">{section.title}</div>
                      <div className="tradex-tag-card-caption">{section.caption}</div>
                    </div>
                  </div>
                  <div className="tradex-tag-card-list">
                    {section.rows.length ? (
                      section.rows.map((row) => (
                        <button
                          key={`${section.key}:${row.side}:${row.strategy_tag}`}
                          type="button"
                          className={
                            selectedTagKey === `${row.side}:${row.holding_band}:${row.strategy_tag}`
                              ? "tradex-tag-card-row is-selected"
                              : "tradex-tag-card-row"
                          }
                          onClick={() => handleSelectTag(row)}
                        >
                          <div className="tradex-tag-card-tag">
                            <span>{row.strategy_tag}</span>
                            <span className="tradex-tag-badge">{row.side}</span>
                            <span className="tradex-tag-band">{row.holding_band}</span>
                          </div>
                          <div className="tradex-tag-card-stats">
                            <span>期待値 {formatPct(row.expectancy_mean)}</span>
                            <span>リスク {formatPct(row.large_loss_rate)}</span>
                            <span>N {row.labeled_count}</span>
                          </div>
                        </button>
                      ))
                    ) : (
                      <div className="tradex-tag-empty">コンボデータなし</div>
                    )}
                  </div>
                </article>
              ))}
            </div>
          </section>

          {!!candleComboTrendSummary && (
            <section className="tradex-daily-history-panel">
              <div className="tradex-tag-card-header">
                <div>
                  <div className="tradex-tag-card-title">コンボ変化監視</div>
                  <div className="tradex-tag-card-caption">
                    2本足、3本足コンボの改善・悪化を追います。
                  </div>
                </div>
              </div>
              <div className="tradex-tag-summary-grid">
                {[
                  { title: "改善中コンボ", rows: candleComboTrendSummary.trends.improving },
                  { title: "悪化中コンボ", rows: candleComboTrendSummary.trends.weakening },
                  { title: "要注意コンボ", rows: candleComboTrendSummary.trends.persistent_risk }
                ].map((section) => (
                  <article key={section.title} className="tradex-tag-card">
                    <div className="tradex-tag-card-header">
                      <div className="tradex-tag-card-title">{section.title}</div>
                    </div>
                    <div className="tradex-tag-card-list">
                      {section.rows.length ? section.rows.map((row) => (
                        <button
                          key={`${section.title}:${row.side}:${row.holding_band}:${row.strategy_tag}`}
                          type="button"
                          className="tradex-tag-card-row"
                          onClick={() => {
                            const matched = rows.find((item) => `${item.side}:${item.holding_band}:${item.strategy_tag}` === `${row.side}:${row.holding_band}:${row.strategy_tag}`);
                            if (matched) handleSelectTag(matched);
                          }}
                        >
                          <div className="tradex-tag-card-tag">
                            <span>{row.strategy_tag}</span>
                            <span className="tradex-tag-badge">{row.side}</span>
                            <span className="tradex-tag-band">{row.holding_band}</span>
                          </div>
                          <div className="tradex-tag-card-stats">
                            <span>期待値差 {formatSignedPct(row.expectancy_delta)}</span>
                            <span>リスク差 {formatSignedPct(row.risk_delta)}</span>
                            <span>N {row.recent_labeled_count}</span>
                          </div>
                        </button>
                      )) : <div className="tradex-tag-empty">コンボ変化データなし</div>}
                    </div>
                  </article>
                ))}
              </div>
            </section>
          )}

          <div className="tradex-tag-summary-grid">
            {summarySections.map((section) => (
              <article key={section.key} className="tradex-tag-card">
                <div className="tradex-tag-card-header">
                  <div>
                    <div className="tradex-tag-card-title">{section.title}</div>
                    <div className="tradex-tag-card-caption">{section.caption}</div>
                  </div>
                </div>
                <div className="tradex-tag-card-list">
                  {section.rows.length ? (
                    section.rows.map((row) => (
                      <button
                        key={`${section.key}:${row.side}:${row.strategy_tag}`}
                        type="button"
                        className={
                          selectedTagKey === `${row.side}:${row.holding_band}:${row.strategy_tag}`
                            ? "tradex-tag-card-row is-selected"
                            : "tradex-tag-card-row"
                        }
                        onClick={() => handleSelectTag(row)}
                      >
                        <div className="tradex-tag-card-tag">
                          <span>{row.strategy_tag}</span>
                          <span className="tradex-tag-badge">{row.side}</span>
                          <span className="tradex-tag-band">{row.holding_band}</span>
                        </div>
                        <div className="tradex-tag-card-stats">
                          <span>期待値 {formatPct(row.expectancy_mean)}</span>
                          <span>リスク {formatPct(row.large_loss_rate)}</span>
                          <span>N {row.labeled_count}</span>
                        </div>
                      </button>
                    ))
                  ) : (
                    <div className="tradex-tag-empty">データなし</div>
                  )}
                </div>
              </article>
            ))}
          </div>

          <div className="tradex-tag-table-wrap">
            <table className="tradex-tag-table">
              <thead>
                <tr>
                  <th>タグ</th>
                  <th>売買</th>
                  <th>保有帯</th>
                  <th>判定</th>
                  <th>件数</th>
                  <th>入る</th>
                  <th>期待値</th>
                  <th>逆行</th>
                  <th>大損率</th>
                  <th>勝率</th>
                  <th>教師</th>
                  <th>失敗例</th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.length ? (
                  filteredRows.map((row) => (
                    <tr
                      key={`${row.publish_id}:${row.side}:${row.holding_band}:${row.strategy_tag}`}
                      className={
                        selectedTagKey === `${row.side}:${row.holding_band}:${row.strategy_tag}`
                          ? "is-selected"
                          : ""
                      }
                      onClick={() => handleSelectTag(row)}
                    >
                      <td>{row.strategy_tag}</td>
                      <td>{row.side}</td>
                      <td>{row.holding_band}</td>
                      <td>{formatHint(row.readiness_hint)}</td>
                      <td>{row.labeled_count}</td>
                      <td>{row.enter_count}</td>
                      <td>{formatPct(row.expectancy_mean)}</td>
                      <td>{formatPct(row.adverse_mean)}</td>
                      <td>{formatPct(row.large_loss_rate)}</td>
                      <td>{formatPct(row.win_rate)}</td>
                      <td>{formatNumber(row.teacher_alignment_mean, 2)}</td>
                      <td>
                        {row.failure_count} / {parseFailureCount(row.worst_failure_examples)}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={12} className="tradex-tag-empty-row">
                      現在のフィルターに該当するデータはありません。
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="tradex-tag-failure-grid">
            <section className="tradex-tag-failure-panel">
              <div className="tradex-tag-failure-header">
                <div className="tradex-tag-card-title">直近の失敗例</div>
                <div className="tradex-tag-card-caption">
                  {selectedRow ? `${selectedRow.strategy_tag} / ${selectedRow.holding_band}` : "行を選ぶと失敗例を確認できます。"}
                </div>
              </div>
              <div className="tradex-tag-failure-list">
                {latestExamples.length ? (
                  latestExamples.map((item, index) => (
                    <article key={`latest:${item.code ?? "na"}:${index}`} className="tradex-tag-failure-item">
                      <div className="tradex-tag-failure-main">
                        {item.code ? (
                          <Link
                            className="tradex-tag-detail-link"
                            to={`/detail/${item.code}`}
                            state={buildDetailState(item)}
                          >
                            {item.code}
                          </Link>
                        ) : (
                          <span>--</span>
                        )}
                        <span>{item.as_of_date ?? "--"}</span>
                        <span>{item.decision ?? "--"}</span>
                      </div>
                      <div className="tradex-tag-card-stats">
                        <span>期待値 {formatPct(item.expected_return)}</span>
                        <span>逆行 {formatPct(item.adverse_move)}</span>
                        <span>教師 {formatNumber(item.teacher_alignment, 2)}</span>
                        <span>{item.strategy_tag ?? "--"}</span>
                      </div>
                    </article>
                  ))
                ) : (
                  <div className="tradex-tag-empty">直近の失敗例はありません。</div>
                )}
              </div>
            </section>

            <section className="tradex-tag-failure-panel">
              <div className="tradex-tag-failure-header">
                <div className="tradex-tag-card-title">大きかった失敗例</div>
                <div className="tradex-tag-card-caption">
                  選択タグで逆行が大きかった事例です。
                </div>
              </div>
              <div className="tradex-tag-failure-list">
                {worstExamples.length ? (
                  worstExamples.map((item, index) => (
                    <article key={`worst:${item.code ?? "na"}:${index}`} className="tradex-tag-failure-item">
                      <div className="tradex-tag-failure-main">
                        {item.code ? (
                          <Link
                            className="tradex-tag-detail-link"
                            to={`/detail/${item.code}`}
                            state={buildDetailState(item)}
                          >
                            {item.code}
                          </Link>
                        ) : (
                          <span>--</span>
                        )}
                        <span>{item.as_of_date ?? "--"}</span>
                        <span>{item.decision ?? "--"}</span>
                      </div>
                      <div className="tradex-tag-card-stats">
                        <span>期待値 {formatPct(item.expected_return)}</span>
                        <span>逆行 {formatPct(item.adverse_move)}</span>
                        <span>教師 {formatNumber(item.teacher_alignment, 2)}</span>
                        <span>{item.strategy_tag ?? "--"}</span>
                      </div>
                    </article>
                  ))
                ) : (
                  <div className="tradex-tag-empty">大きい失敗例はありません。</div>
                )}
              </div>
            </section>
          </div>
        </section>
      </main>
    </div>
  );
}
