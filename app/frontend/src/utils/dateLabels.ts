type DateLikeValue = number | string | null | undefined;

type DateLabelOptions = {
  compact?: boolean;
  separator?: "/" | "-";
};

const WEEKDAY_LABELS = ["日", "月", "火", "水", "木", "金", "土"] as const;

const pad2 = (value: number) => String(Math.trunc(value)).padStart(2, "0");

const formatParts = (
  year: number,
  month: number,
  day: number,
  options: DateLabelOptions = {}
) => {
  const compact = options.compact ?? false;
  const separator = options.separator ?? "/";
  const yearLabel = compact ? pad2(year % 100) : String(year);
  return `${yearLabel}${separator}${pad2(month)}${separator}${pad2(day)}`;
};

const formatWithWeekday = (date: Date, options: DateLabelOptions = {}) => {
  const weekday = WEEKDAY_LABELS[date.getUTCDay()] ?? "";
  const base = formatParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate(), options);
  return `${base} (${weekday})`;
};

const parseNumericDate = (value: number): Date | null => {
  if (!Number.isFinite(value)) return null;
  const raw = Math.trunc(value);
  if (raw >= 10_000_000 && raw < 100_000_000) {
    const year = Math.floor(raw / 10_000);
    const month = Math.floor((raw % 10_000) / 100);
    const day = raw % 100;
    return new Date(Date.UTC(year, month - 1, day));
  }
  if (raw > 1_000_000_000_000) return new Date(raw);
  if (raw > 1_000_000_000) return new Date(raw * 1000);
  return null;
};

const parseStringDate = (value: string): Date | null => {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\d{8}$/.test(trimmed)) {
    const year = Number(trimmed.slice(0, 4));
    const month = Number(trimmed.slice(4, 6));
    const day = Number(trimmed.slice(6, 8));
    return new Date(Date.UTC(year, month - 1, day));
  }
  const match = trimmed.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return new Date(Date.UTC(year, month - 1, day));
};

const parseDateLike = (value: DateLikeValue): Date | null => {
  if (typeof value === "number") return parseNumericDate(value);
  if (typeof value === "string") return parseStringDate(value);
  return null;
};

export const formatCompactDateLabel = (value: DateLikeValue) => {
  const date = parseDateLike(value);
  if (!date || Number.isNaN(date.getTime())) return "--";
  return formatWithWeekday(date, { compact: true, separator: "/" });
};

export const formatLongDateLabel = (value: DateLikeValue, separator: "/" | "-" = "/") => {
  const date = parseDateLike(value);
  if (!date || Number.isNaN(date.getTime())) return "--";
  return formatWithWeekday(date, { compact: false, separator });
};

export const formatIsoDateLabel = (value: DateLikeValue) => formatLongDateLabel(value, "/");

export const formatLedgerDateLabel = (value: string | null | undefined) => formatLongDateLabel(value, "-");

const parseDateTimeLike = (value: DateLikeValue): Date | null => {
  if (typeof value === "number") {
    const raw = Math.trunc(value);
    if (raw > 1_000_000_000_000) return new Date(raw);
    if (raw > 1_000_000_000) return new Date(raw * 1000);
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (!/[T:\s]/.test(trimmed)) return null;
    const date = new Date(trimmed);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  return null;
};

export const formatDateTimeLabel = (value: DateLikeValue) => {
  const date = parseDateTimeLike(value);
  if (!date || Number.isNaN(date.getTime())) return "--";
  const parts = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const partMap = Object.fromEntries(parts.map((part) => [part.type, part.value])) as Record<string, string>;
  const year = partMap.year ?? "";
  const month = partMap.month ?? "";
  const day = partMap.day ?? "";
  const weekday = partMap.weekday ?? "";
  const hour = partMap.hour ?? "00";
  const minute = partMap.minute ?? "00";
  const second = partMap.second ?? "00";
  return `${year}/${month}/${day} (${weekday}) ${hour}:${minute}:${second}`;
};
