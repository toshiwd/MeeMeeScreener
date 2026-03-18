// @ts-nocheck
import type { Box } from "../store";
import type { SignalMetrics } from "./signals";
import { computeSignalMetrics } from "./signals";

export type ConsultationSort = "score" | "code";

export type ConsultationTimeframe = "monthly" | "weekly" | "daily";

export type ConsultationHeader = {
  createdAt: Date;
  timeframe: ConsultationTimeframe;
  barsCount: number;
  filterName?: string | null;
};

export type ConsultationItemInput = {
  code: string;
  name?: string | null;
  market?: string | null;
  sector?: string | null;
  bars?: number[][] | null;
  metrics?: SignalMetrics | null;
  boxes?: Box[] | null;
  boxState?: "NONE" | "IN_BOX" | "JUST_BREAKOUT" | "BREAKOUT_UP" | "BREAKOUT_DOWN" | null;
  hasBox?: boolean | null;
  buyState?: string | null;
  buyStateScore?: number | null;
  buyStateReason?: string | null;
  buyStateDetails?: {
    monthly?: number | null;
    weekly?: number | null;
    daily?: number | null;
  } | null;
};

export type ConsultationPackResult = {
  text: string;
  usedCount: number;
  omittedCount: number;
};

type NormalizedBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
};

const N_A = "N/A";
const MA_PERIODS = [7, 20, 60, 100];

const formatNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return N_A;
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

const formatDate = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return N_A;
  const raw = Number(value);
  if (raw >= 10000000 && raw < 100000000) {
    const year = Math.floor(raw / 10000);
    const month = Math.floor((raw % 10000) / 100);
    const day = raw % 100;
    const mm = String(month).padStart(2, "0");
    const dd = String(day).padStart(2, "0");
    return `${year}-${mm}-${dd}`;
  }
  if (raw >= 100000 && raw < 1000000) {
    const year = Math.floor(raw / 100);
    const month = raw % 100;
    const mm = String(month).padStart(2, "0");
    return `${year}-${mm}-01`;
  }
  const date =
    raw > 1000000000000 ? new Date(raw) : raw > 1000000000 ? new Date(raw * 1000) : null;
  if (!date || Number.isNaN(date.getTime())) return N_A;
  return date.toISOString().slice(0, 10);
};

const parseDate = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return null;
  const raw = Number(value);
  if (raw >= 10000000 && raw < 100000000) {
    const year = Math.floor(raw / 10000);
    const month = Math.floor((raw % 10000) / 100);
    const day = raw % 100;
    return new Date(Date.UTC(year, month - 1, day));
  }
  if (raw >= 100000 && raw < 1000000) {
    const year = Math.floor(raw / 100);
    const month = raw % 100;
    return new Date(Date.UTC(year, month - 1, 1));
  }
  if (raw > 1000000000000) {
    const date = new Date(raw);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  if (raw > 1000000000) {
    const date = new Date(raw * 1000);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  return null;
};

const normalizeBars = (bars?: number[][] | null): NormalizedBar[] => {
  if (!bars?.length) return [];
  const rows = bars
    .map((row) => {
      if (!Array.isArray(row) || row.length < 5) return null;
      const time = Number(row[0]);
      const open = Number(row[1]);
      const high = Number(row[2]);
      const low = Number(row[3]);
      const close = Number(row[4]);
      if (![time, open, high, low, close].every((value) => Number.isFinite(value))) return null;
      const volume = row.length > 5 ? Number(row[5]) : null;
      return { time, open, high, low, close, volume };
    })
    .filter((row): row is NormalizedBar => Boolean(row));
  if (rows.length >= 2 && rows[0].time > rows[rows.length - 1].time) {
    rows.reverse();
  }
  return rows;
};

const computeLastMA = (bars: NormalizedBar[], period: number) => {
  if (bars.length < period) return null;
  let sum = 0;
  let last: number | null = null;
  for (let i = 0; i < bars.length; i += 1) {
    sum += bars[i].close;
    if (i >= period) {
      sum -= bars[i - period].close;
    }
    if (i >= period - 1) {
      last = sum / period;
    }
  }
  return last;
};

const computePrevMA = (bars: NormalizedBar[], period: number) => {
  if (bars.length <= period) return null;
  let sum = 0;
  let prev: number | null = null;
  for (let i = 0; i < bars.length; i += 1) {
    sum += bars[i].close;
    if (i >= period) {
      sum -= bars[i - period].close;
    }
    if (i === bars.length - 2 && i >= period - 1) {
      prev = sum / period;
      break;
    }
  }
  return prev;
};

const computeVolumeRatio = (bars: NormalizedBar[], window = 20) => {
  if (!bars.length) return null;
  const tail = bars.slice(-window);
  const volumes = tail.map((bar) => bar.volume).filter((value) => Number.isFinite(value)) as number[];
  if (!volumes.length) return null;
  const avg = volumes.reduce((acc, value) => acc + value, 0) / volumes.length;
  const latest = bars[bars.length - 1]?.volume;
  if (!Number.isFinite(latest) || avg <= 0) return null;
  return latest / avg;
};

const getLatestBox = (boxes?: Box[] | null) => {
  if (!boxes?.length) return null;
  return boxes.reduce((latest, box) => (box.endTime > latest.endTime ? box : latest), boxes[0]);
};

const computeBoxMonths = (box: Box | null) => {
  if (!box) return null;
  const start = parseDate(box.startTime);
  const end = parseDate(box.endTime);
  if (!start || !end) return null;
  const months =
    (end.getUTCFullYear() - start.getUTCFullYear()) * 12 +
    (end.getUTCMonth() - start.getUTCMonth()) +
    1;
  return months > 0 ? months : null;
};

const formatTimeframeLabel = (timeframe: ConsultationTimeframe) => {
  if (timeframe === "monthly") return "月足";
  if (timeframe === "weekly") return "週足";
  return "日足";
};

const buildScores = (bars: NormalizedBar[], metrics: SignalMetrics | null, box: Box | null) => {
  if (!bars.length) {
    return { scoreBox: null, scoreTrend: null, scoreVolume: null, totalScore: null };
  }
  const close = bars[bars.length - 1].close;
  const ma20 = computeLastMA(bars, 20);
  const ma60 = computeLastMA(bars, 60);
  const prevMa20 = computePrevMA(bars, 20);

  let scoreTrend = 0;
  if (Number.isFinite(ma20) && Number.isFinite(close)) {
    scoreTrend += close > ma20 ? 10 : close < ma20 ? -10 : 0;
  }
  if (Number.isFinite(ma20) && Number.isFinite(ma60)) {
    scoreTrend += ma20 > ma60 ? 10 : ma20 < ma60 ? -10 : 0;
  }
  if (Number.isFinite(ma20) && Number.isFinite(prevMa20)) {
    scoreTrend += ma20 > prevMa20 ? 10 : ma20 < prevMa20 ? -10 : 0;
  }
  if (metrics?.counts?.[7]) {
    const count7 = metrics.counts[7];
    if (count7.upCount >= 7) scoreTrend += 10;
    if (count7.downCount >= 7) scoreTrend -= 10;
  }

  const volumeRatio = computeVolumeRatio(bars, 20);
  let scoreVolume = 0;
  if (Number.isFinite(volumeRatio)) {
    if ((volumeRatio ?? 0) >= 2) scoreVolume = 20;
    else if ((volumeRatio ?? 0) >= 1.2) scoreVolume = 10;
  }

  let scoreBox = 0;
  if (box) {
    const months = computeBoxMonths(box);
    if (months) scoreBox += Math.min(30, months * 2);
    if (box.breakout === "up" || box.breakout === "down") scoreBox += 10;
  }

  const totalScore = scoreBox + scoreTrend + scoreVolume;
  return { scoreBox, scoreTrend, scoreVolume, totalScore };
};

const formatMaPosition = (close: number | null, ma: number | null) => {
  if (!Number.isFinite(close) || !Number.isFinite(ma)) return N_A;
  if (close > ma) return "上";
  if (close < ma) return "下";
  return "同値";
};

export const buildConsultationPack = (
  header: ConsultationHeader,
  items: ConsultationItemInput[],
  sort: ConsultationSort,
  maxItems = 10
): ConsultationPackResult => {
  const createdAt = header.createdAt;
  const createdLabel = `${createdAt.getFullYear()}-${String(createdAt.getMonth() + 1).padStart(
    2,
    "0"
  )}-${String(createdAt.getDate()).padStart(2, "0")} ${String(createdAt.getHours()).padStart(
    2,
    "0"
  )}:${String(createdAt.getMinutes()).padStart(2, "0")}`;

  const computed = items.map((item) => {
    const bars = normalizeBars(item.bars);
    const metrics = item.metrics ?? (bars.length ? computeSignalMetrics(bars.map((bar) => [
      bar.time,
      bar.open,
      bar.high,
      bar.low,
      bar.close,
      bar.volume ?? 0
    ])) : null);
    const lastBar = bars.length ? bars[bars.length - 1] : null;
    const box = getLatestBox(item.boxes);
    const scores = buildScores(bars, metrics, box);
    return {
      ...item,
      bars,
      metrics,
      lastBar,
      box,
      scores
    };
  });

  const sorted = [...computed].sort((a, b) => {
    if (sort === "code") return a.code.localeCompare(b.code);
    const av = a.scores.totalScore;
    const bv = b.scores.totalScore;
    const aMissing = av == null || !Number.isFinite(av);
    const bMissing = bv == null || !Number.isFinite(bv);
    if (aMissing && bMissing) return a.code.localeCompare(b.code);
    if (aMissing) return 1;
    if (bMissing) return -1;
    if (av === bv) return a.code.localeCompare(b.code);
    return (bv ?? 0) - (av ?? 0);
  });

  const limited = sorted.slice(0, maxItems);
  const omittedCount = Math.max(0, sorted.length - limited.length);

  const headerLines = [
    "選定相談パック",
    `- 作成日時: ${createdLabel}`,
    `- 足種: ${formatTimeframeLabel(header.timeframe)}`,
    `- 表示本数: ${header.barsCount}`,
    `- 生成件数: ${limited.length}` + (omittedCount ? `（残り${omittedCount}件）` : ""),
    header.filterName ? `- 条件名: ${header.filterName}` : null
  ].filter(Boolean);

  const bodyLines: string[] = [];
  limited.forEach((item, index) => {
    const lastBar = item.lastBar;
    const metrics = item.metrics;
    const box = item.box;
    const boxMonths = computeBoxMonths(box);
    const close = lastBar?.close ?? null;
    const maValues = new Map<number, number | null>();
    MA_PERIODS.forEach((period) => {
      maValues.set(period, computeLastMA(item.bars, period));
    });

    const counts = metrics?.counts ?? {};
    const countText = (period: number) => {
      const state = counts[period];
      if (!state) return N_A;
      return `${state.upCount}/${state.downCount}`;
    };

    const volumeRatio = computeVolumeRatio(item.bars, 20);
    const boxState =
      item.boxState === "JUST_BREAKOUT"
        ? "直近ブレイク"
        : item.boxState === "IN_BOX"
        ? "箱内"
        : item.boxState === "BREAKOUT_UP"
        ? "上抜け"
        : item.boxState === "BREAKOUT_DOWN"
        ? "下抜け"
        : box?.breakout === "up"
        ? "上抜け"
        : box?.breakout === "down"
        ? "下抜け"
        : "未ブレイク";
    const hasBox =
      typeof item.hasBox === "boolean"
        ? item.hasBox
        : Boolean(box) ||
          item.boxState === "IN_BOX" ||
          item.boxState === "JUST_BREAKOUT" ||
          item.boxState === "BREAKOUT_UP" ||
          item.boxState === "BREAKOUT_DOWN";

    bodyLines.push(
      `---`,
      `[${index + 1}] ${item.code} ${item.name ?? ""}`.trim(),
      "A. 銘柄識別",
      `- コード: ${item.code}`,
      `- 銘柄名: ${item.name ?? N_A}`,
      `- 市場: ${item.market ?? N_A}`,
      `- セクター: ${item.sector ?? N_A}`,
      "B. 最新データ",
      `- 最終日付: ${lastBar ? formatDate(lastBar.time) : N_A}`,
      `- 終値: ${formatNumber(close, 0)}`,
      `- 出来高: ${formatNumber(lastBar?.volume ?? null, 0)}`,
      `- 直近20日出来高平均比: ${
        volumeRatio == null ? N_A : `${(volumeRatio * 100).toFixed(0)}%`
      }`,
      "C. 位置・環境認識",
      `- MA位置: 7MA=${formatMaPosition(close, maValues.get(7) ?? null)}, 20MA=${formatMaPosition(
        close,
        maValues.get(20) ?? null
      )}, 60MA=${formatMaPosition(close, maValues.get(60) ?? null)}, 100MA=${formatMaPosition(
        close,
        maValues.get(100) ?? null
      )}`,
      `- 本数カウント: 7上/7下=${countText(7)}, 20上/20下=${countText(
        20
      )}, 60上/60下=${countText(60)}, 100上/100下=${countText(100)}`,
      `- PPP/ABC: ${N_A}`,
      "D. ボックス判定",
      `- ボックス有無: ${hasBox ? "Yes" : "No"}`,
      `- 期間: ${
        box ? `${formatDate(box.startTime)}〜${formatDate(box.endTime)}` : N_A
      }`,
      `- 月数: ${boxMonths ?? N_A}`,
      `- 実体上限/実体下限: ${
        box ? `${formatNumber(box.upper, 0)} / ${formatNumber(box.lower, 0)}` : N_A
      }`,
      `- 荒れ箱フラグ: ${N_A}`,
      `- ブレイク状況: ${boxState}`,
      "E. 直近シグナル要約",
      `- 窓: ${N_A}`,
      `- 大陽線/大陰線: ${N_A}`,
      `- コマ/十字: ${N_A}`,
      `- 全戻し/包み/はらみ: ${N_A}`,
      "F. スコア",
      `- score_box: ${item.scores.scoreBox ?? N_A}`,
      `- score_trend: ${item.scores.scoreTrend ?? N_A}`,
      `- score_volume: ${item.scores.scoreVolume ?? N_A}`,
      `- total_score: ${item.scores.totalScore ?? N_A}`,
      "G. 買い候補状態",
      `- 状態: ${item.buyState ?? N_A}`,
      `- 状態スコア: ${formatNumber(item.buyStateScore ?? null, 0)}`,
      `- 理由: ${item.buyStateReason ?? N_A}`,
      "H. 買い候補スコア内訳",
      `- 月足: ${formatNumber(item.buyStateDetails?.monthly ?? null, 0)}`,
      `- 週足: ${formatNumber(item.buyStateDetails?.weekly ?? null, 0)}`,
      `- 日足: ${formatNumber(item.buyStateDetails?.daily ?? null, 0)}`
    );
  });

  const text = [...headerLines, "", ...bodyLines].join("\n");
  return { text, usedCount: limited.length, omittedCount };
};
