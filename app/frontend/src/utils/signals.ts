// @ts-nocheck
export type PendingSide = "up" | "down" | null;

export type MaCountState = {
  upCount: number;
  downCount: number;
  pendingSide: PendingSide;
};

export type SignalChip = {
  label: string;
  kind: "achieved" | "warning";
  priority: number;
};

export type SignalMetrics = {
  counts: Record<number, MaCountState>;
  signals: SignalChip[];
  trendStrength: number;
  exhaustionRisk: number;
};

export type SignalDirectionSummary = {
  hasBuySignal: boolean;
  hasSellSignal: boolean;
  hasAnySignal: boolean;
};

const PERIODS = [7, 20, 60, 100];

const THRESHOLDS: Record<number, number> = {
  7: 6,
  20: 16,
  60: 48,
  100: 80
};

const TREND_WEIGHTS: Record<number, number> = {
  7: 0.25,
  20: 0.4,
  60: 0.25,
  100: 0.1
};

const RISK_WEIGHTS: Record<number, number> = {
  7: 0.35,
  20: 0.45,
  60: 0.15,
  100: 0.05
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const updateCounts = (state: MaCountState, side: "up" | "down" | "flat") => {
  if (side === "flat") {
    state.upCount = 0;
    state.downCount = 0;
    state.pendingSide = null;
    return;
  }

  if (side === "up") {
    if (state.upCount > 0) {
      state.upCount += 1;
      state.pendingSide = null;
      return;
    }
    if (state.downCount > 0) {
      if (state.pendingSide === "up") {
        state.upCount = 2;
        state.downCount = 0;
        state.pendingSide = null;
      } else {
        state.pendingSide = "up";
      }
      return;
    }
    state.upCount = 1;
    state.downCount = 0;
    state.pendingSide = null;
    return;
  }

  if (state.downCount > 0) {
    state.downCount += 1;
    state.pendingSide = null;
    return;
  }
  if (state.upCount > 0) {
    if (state.pendingSide === "down") {
      state.downCount = 2;
      state.upCount = 0;
      state.pendingSide = null;
    } else {
      state.pendingSide = "down";
    }
    return;
  }
  state.downCount = 1;
  state.upCount = 0;
  state.pendingSide = null;
};

const initCounts = () =>
  PERIODS.reduce<Record<number, MaCountState>>((acc, period) => {
    acc[period] = { upCount: 0, downCount: 0, pendingSide: null };
    return acc;
  }, {});

const buildSignals = (counts: Record<number, MaCountState>, maxSignals: number) => {
  const signals: SignalChip[] = [];
  const priorityBase = { 100: 400, 60: 300, 20: 200, 7: 100 };

  [100, 60, 20, 7].forEach((period) => {
    const state = counts[period];
    if (!state) return;
    const threshold = THRESHOLDS[period] ?? Math.floor(period * 0.8);
    const upCount = state.upCount;
    const downCount = state.downCount;
    const side = upCount >= downCount ? "up" : "down";
    const count = side === "up" ? upCount : downCount;
    if (count <= 0) return;

    if (count >= period) {
      const label = `${period}${side === "up" ? "上" : "下"}:${count}`;
      signals.push({
        label,
        kind: "achieved",
        priority: priorityBase[period] + 50
      });
      return;
    }

    if (count >= threshold) {
      const label = `${period}${side === "up" ? "上" : "下"}:${count}`;
      signals.push({
        label,
        kind: "warning",
        priority: priorityBase[period]
      });
    }
  });

  signals.sort((a, b) => b.priority - a.priority);
  return signals.slice(0, maxSignals);
};

const finiteNumber = (value: unknown) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const averageLast = (values: number[], period: number) => {
  if (period <= 0 || values.length < period) return null;
  const slice = values.slice(values.length - period);
  return slice.reduce((sum, value) => sum + value, 0) / period;
};

const buildFailedHighRetestSignal = (bars: number[][]): SignalChip | null => {
  const normalized = bars
    .map((row) => {
      const time = finiteNumber(row?.[0]);
      const high = finiteNumber(row?.[2]);
      const close = finiteNumber(row?.[4]);
      return time == null || high == null || close == null ? null : { time, high, close };
    })
    .filter((row): row is { time: number; high: number; close: number } => row != null)
    .sort((a, b) => a.time - b.time);

  if (normalized.length < 80) return null;

  const latest = normalized[normalized.length - 1];
  const previous = normalized[normalized.length - 2];
  if (!latest || !previous) return null;

  const recentWindow = normalized.slice(Math.max(0, normalized.length - 20), normalized.length - 1);
  const priorWindow = normalized.slice(0, Math.max(0, normalized.length - 20));
  if (recentWindow.length < 5 || priorWindow.length < 40) return null;

  const priorHigh = Math.max(...priorWindow.map((row) => row.high));
  const recentRetestHigh = Math.max(...recentWindow.map((row) => row.high));
  const closes = normalized.map((row) => row.close);
  const ma7 = averageLast(closes, 7);
  const ma20 = averageLast(closes, 20);
  if (priorHigh <= 0 || ma7 == null || ma20 == null) return null;

  const attemptedPriorHigh = recentRetestHigh >= priorHigh * 0.88 && recentRetestHigh < priorHigh * 1.01;
  const rolledOverFromRetest = latest.close <= recentRetestHigh * 0.94;
  const belowShortMAs = latest.close < ma7 && latest.close < ma20;
  const stillFalling = latest.close < previous.close;

  if (attemptedPriorHigh && rolledOverFromRetest && belowShortMAs && stillFalling) {
    return {
      label: "売り:高値未達失速",
      kind: "warning",
      priority: 460
    };
  }

  return null;
};

const hasDirectionalThresholdSignal = (
  counts: Record<number, MaCountState>,
  direction: "up" | "down"
) => {
  return [100, 60, 20, 7].some((period) => {
    const state = counts[period];
    if (!state) return false;
    const threshold = THRESHOLDS[period] ?? Math.floor(period * 0.8);
    const count = direction === "up" ? state.upCount : state.downCount;
    return count >= threshold;
  });
};

const computeTrendStrength = (counts: Record<number, MaCountState>) => {
  let total = 0;
  PERIODS.forEach((period) => {
    const state = counts[period];
    if (!state) return;
    const up = state.upCount;
    const down = state.downCount;
    let value = 0;
    if (up > 0) {
      value = Math.min(up, period) / period;
    } else if (down > 0) {
      value = -Math.min(down, period) / period;
    }
    total += (TREND_WEIGHTS[period] ?? 0) * value;
  });
  return total * 100;
};

const computeExhaustionRisk = (counts: Record<number, MaCountState>) => {
  let total = 0;
  PERIODS.forEach((period) => {
    const state = counts[period];
    if (!state) return;
    const threshold = THRESHOLDS[period] ?? Math.floor(period * 0.8);
    const count = state.upCount > 0 ? state.upCount : state.downCount > 0 ? state.downCount : 0;
    const risk = clamp((count - threshold) / (period - threshold), 0, 1);
    total += (RISK_WEIGHTS[period] ?? 0) * risk;
  });
  return total * 100;
};

export const computeSignalMetrics = (bars: number[][], maxSignals = 5): SignalMetrics => {
  const counts = initCounts();
  const sums = new Map<number, number>();
  const validBars = bars.filter((row) => Array.isArray(row) && row.length >= 5);
  if (validBars.length >= 2 && Number(validBars[0][0]) > Number(validBars[validBars.length - 1][0])) {
    validBars.reverse();
  }

  PERIODS.forEach((period) => {
    sums.set(period, 0);
  });

  for (let i = 0; i < validBars.length; i += 1) {
    const close = Number(validBars[i][4]);
    if (!Number.isFinite(close)) continue;

    PERIODS.forEach((period) => {
      let sum = sums.get(period) ?? 0;
      sum += close;
      if (i >= period) {
        sum -= Number(validBars[i - period]?.[4] ?? 0);
      }
      sums.set(period, sum);
      if (i < period - 1) return;

      const ma = sum / period;
      if (!Number.isFinite(ma)) return;
      const side = close >= ma ? "up" : "down";
      updateCounts(counts[period], side);
    });
  }

  const signals = buildSignals(counts, maxSignals);
  const failedHighRetestSignal = buildFailedHighRetestSignal(validBars);
  if (failedHighRetestSignal) {
    signals.push(failedHighRetestSignal);
    signals.sort((a, b) => b.priority - a.priority);
  }

  return {
    counts,
    signals: signals.slice(0, maxSignals),
    trendStrength: computeTrendStrength(counts),
    exhaustionRisk: computeExhaustionRisk(counts)
  };
};

export const getSignalDirectionSummary = (metrics: SignalMetrics): SignalDirectionSummary => {
  const hasBuySignal = hasDirectionalThresholdSignal(metrics.counts, "up");
  const hasSellSignal = hasDirectionalThresholdSignal(metrics.counts, "down");
  return {
    hasBuySignal,
    hasSellSignal,
    hasAnySignal: hasBuySignal || hasSellSignal
  };
};
