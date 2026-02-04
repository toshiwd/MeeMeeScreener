export type Timeframe = "daily" | "weekly" | "monthly";

export type Operand =
  | { type: "field"; field: "C" }
  | { type: "ma"; period: number }
  | { type: "number"; value: number };

export type ConditionOperator =
  | "above"
  | "below"
  | "cross_up"
  | "cross_down";

export type TechnicalCondition = {
  id: string;
  timeframe: Timeframe;
  left: Operand;
  operator: ConditionOperator;
  right: Operand;
};

export type TechnicalFilterState = {
  defaultTimeframe: Timeframe;
  anchorMode: "latest" | "date";
  anchorDate: string | null;
  conditions: TechnicalCondition[];
  boxThisMonth: boolean;
};

export type AnchorInfo = {
  index: number;
  time: number;
  asof: boolean;
  prevIndex: number | null;
};

export type AnchorEvaluation = {
  matches: boolean;
  anchor: AnchorInfo;
};

const OPERATOR_LABELS: Record<ConditionOperator, string> = {
  above: "以上",
  below: "以下",
  cross_up: "上抜け",
  cross_down: "下抜け"
};

export const formatDateYMD = (time: number, separator = "/") => {
  const date = new Date(time * 1000);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getUTCFullYear()}${separator}${pad(date.getUTCMonth() + 1)}${separator}${pad(
    date.getUTCDate()
  )}`;
};

export const getLatestAnchorTime = (
  barsMap: Record<string, { bars?: number[][] } | undefined>
): number | null => {
  let latest: number | null = null;
  for (const payload of Object.values(barsMap)) {
    const bars = payload?.bars;
    if (!bars?.length) continue;
    const lastTime = normalizeBarTime(bars[bars.length - 1]?.[0]);
    if (lastTime == null) continue;
    if (latest == null || lastTime > latest) {
      latest = lastTime;
    }
  }
  return latest;
};

export const resolveAnchorInfo = (bars: number[][], anchorTime: number): AnchorInfo | null => {
  if (!bars.length) return null;
  let idx = -1;
  for (let i = bars.length - 1; i >= 0; i -= 1) {
    const time = normalizeBarTime(bars[i]?.[0]);
    if (time == null) continue;
    if (time <= anchorTime) {
      idx = i;
      break;
    }
  }
  if (idx < 0) return null;
  const time = normalizeBarTime(bars[idx]?.[0]);
  if (time == null) return null;
  return {
    index: idx,
    time,
    asof: time !== anchorTime,
    prevIndex: idx > 0 ? idx - 1 : null
  };
};

const getFieldIndex = (_field: Operand["field"]) => 4;

const resolveFieldValue = (bars: number[][], index: number, field: Operand["field"]) => {
  const row = bars[index];
  if (!row) return null;
  const value = Number(row[getFieldIndex(field)]);
  return Number.isFinite(value) ? value : null;
};

export const computeMAAt = (bars: number[][], index: number, period: number) => {
  const length = Math.max(0, Math.floor(period));
  if (length <= 0) return null;
  if (index - length + 1 < 0) return null;
  let sum = 0;
  for (let i = index - length + 1; i <= index; i += 1) {
    const value = resolveFieldValue(bars, i, "C");
    if (value == null) return null;
    sum += value;
  }
  return sum / length;
};

export const computeEMAAt = (bars: number[][], index: number, period: number) => {
  const length = Math.max(0, Math.floor(period));
  if (length <= 0) return null;
  if (index - length + 1 < 0) return null;
  const k = 2 / (length + 1);
  let ema = 0;
  let initialized = false;
  for (let i = 0; i <= index; i += 1) {
    const close = resolveFieldValue(bars, i, "C");
    if (close == null) return null;
    if (!initialized) {
      if (i === length - 1) {
        const sma = computeMAAt(bars, i, length);
        if (sma == null) return null;
        ema = sma;
        initialized = true;
      }
      continue;
    }
    ema = close * k + ema * (1 - k);
  }
  return initialized ? ema : null;
};

export const resolveOperandValue = (
  bars: number[][],
  index: number,
  operand: Operand
): number | null => {
  switch (operand.type) {
    case "number":
      return Number.isFinite(operand.value) ? operand.value : null;
    case "field":
      return resolveFieldValue(bars, index, operand.field);
    case "ma":
      return computeMAAt(bars, index, operand.period);
    default:
      return null;
  }
};

export const evaluateBuilderCondition = (
  condition: TechnicalCondition,
  bars: number[][],
  anchor: AnchorInfo
): boolean => {
  const { index, prevIndex } = anchor;
  const left = resolveOperandValue(bars, index, condition.left);
  const right = resolveOperandValue(bars, index, condition.right);
  if (left == null || right == null) return false;

  switch (condition.operator) {
    case "above":
      return left >= right;
    case "below":
      return left <= right;
    case "cross_up": {
      if (prevIndex == null) return false;
      const prevLeft = resolveOperandValue(bars, prevIndex, condition.left);
      const prevRight = resolveOperandValue(bars, prevIndex, condition.right);
      if (prevLeft == null || prevRight == null) return false;
      return prevLeft <= prevRight && left > right;
    }
    case "cross_down": {
      if (prevIndex == null) return false;
      const prevLeft = resolveOperandValue(bars, prevIndex, condition.left);
      const prevRight = resolveOperandValue(bars, prevIndex, condition.right);
      if (prevLeft == null || prevRight == null) return false;
      return prevLeft >= prevRight && left < right;
    }
    default:
      return false;
  }
};

export const describeOperand = (operand: Operand): string => {
  if (operand.type === "field") return "価格";
  if (operand.type === "number") return `価格(${operand.value})`;
  if (operand.type === "ma") return `MA(${operand.period})`;
  return "";
};

export const describeCondition = (condition: TechnicalCondition): string => {
  const left = describeOperand(condition.left);
  const right = describeOperand(condition.right);
  const op = OPERATOR_LABELS[condition.operator] ?? condition.operator;
  return `${left} ${op} ${right}`;
};

const normalizeBarTime = (value: unknown) => {
  const time = Number(value);
  if (!Number.isFinite(time)) return null;
  if (time > 10_000_000_000_000) return Math.floor(time / 1000);
  if (time > 10_000_000_000) return Math.floor(time / 10);
  if (time >= 10_000_000 && time < 100_000_000) {
    const year = Math.floor(time / 10000);
    const month = Math.floor((time % 10000) / 100);
    const day = time % 100;
    return normalizeDateParts(year, month, day);
  }
  if (time >= 100_000 && time < 1_000_000) {
    const year = Math.floor(time / 100);
    const month = time % 100;
    return normalizeDateParts(year, month, 1);
  }
  return Math.floor(time);
};

const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

export const sanitizeTechnicalConditions = (
  conditions: Array<unknown>,
  fallbackTimeframe: Timeframe
): { conditions: TechnicalCondition[]; dropped: number } => {
  const sanitized: TechnicalCondition[] = [];
  let dropped = 0;
  const allowedOperators: ConditionOperator[] = ["above", "below", "cross_up", "cross_down"];
  conditions.forEach((raw) => {
    if (!raw || typeof raw !== "object") {
      dropped += 1;
      return;
    }
    const item = raw as TechnicalCondition;
    if (!allowedOperators.includes(item.operator)) {
      dropped += 1;
      return;
    }
    if (!item.left || !item.right) {
      dropped += 1;
      return;
    }
    if (item.left.type !== "field" && item.left.type !== "ma") {
      dropped += 1;
      return;
    }
    if (item.left.type === "field" && item.left.field !== "C") {
      dropped += 1;
      return;
    }
    if (item.right.type !== "ma" && item.right.type !== "number") {
      dropped += 1;
      return;
    }
    const timeframe =
      item.timeframe === "daily" || item.timeframe === "weekly" || item.timeframe === "monthly"
        ? item.timeframe
        : fallbackTimeframe;
    const normalized = normalizeBuilderCondition({
      id: item.id || `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      timeframe,
      left: item.left,
      operator: item.operator,
      right: item.right
    });
    sanitized.push(normalized);
  });
  return { conditions: sanitized, dropped };
};

export const buildDefaultCondition = (timeframe: Timeframe): TechnicalCondition => ({
  id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  timeframe,
  left: { type: "field", field: "C" },
  operator: "above",
  right: { type: "ma", period: 20 }
});

export const normalizeBuilderCondition = (
  condition: TechnicalCondition
): TechnicalCondition => {
  const left = condition.left;
  const right = condition.right;
  const timeframe =
    condition.timeframe === "daily" ||
    condition.timeframe === "weekly" ||
    condition.timeframe === "monthly"
      ? condition.timeframe
      : "monthly";
  const normalizedLeft =
    left.type === "ma"
      ? { type: "ma", period: Math.max(1, Math.floor(left.period || 20)) }
      : { type: "field", field: "C" };
  const normalizedRight =
    right.type === "ma"
      ? { type: "ma", period: Math.max(1, Math.floor(right.period || 20)) }
      : right.type === "number"
        ? { type: "number", value: Number.isFinite(right.value) ? right.value : 0 }
        : { type: "ma", period: 20 };
  return {
    ...condition,
    timeframe,
    left: normalizedLeft,
    right: normalizedRight
  };
};
