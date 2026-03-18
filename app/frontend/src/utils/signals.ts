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

  return {
    counts,
    signals: buildSignals(counts, maxSignals),
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
