export type TradeStrengthBand = "strong" | "medium" | "weak";

const normalizeTradeScore = (value: number | null | undefined): number | null => {
  if (!Number.isFinite(value ?? NaN)) return null;
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) return null;
  return Math.max(0, Math.min(1, normalized));
};

export const resolveTradeStrengthBand = (value: number | null | undefined): TradeStrengthBand | null => {
  const score = normalizeTradeScore(value);
  if (score == null) return null;
  if (score >= 0.8) return "strong";
  if (score >= 0.6) return "medium";
  return "weak";
};

export const formatTradeStrengthLabel = (value: number | null | undefined): string => {
  const band = resolveTradeStrengthBand(value);
  if (!band) return "--";
  switch (band) {
    case "strong":
      return "強";
    case "medium":
      return "中";
    case "weak":
      return "弱";
  }
};

export const formatTradeStrengthPoints = (value: number | null | undefined): string => {
  const score = normalizeTradeScore(value);
  if (score == null) return "--";
  return `${Math.round(score * 100)}点`;
};

export const formatTradeStrengthCaption = (
  directionLabel: string,
  value: number | null | undefined
): string => {
  const scoreLabel = formatTradeStrengthPoints(value);
  if (scoreLabel === "--") return directionLabel;
  return `${directionLabel} ${formatTradeStrengthLabel(value)} ${scoreLabel}`;
};

export const tradeStrengthToneClass = (value: number | null | undefined): string => {
  const band = resolveTradeStrengthBand(value);
  if (band === "strong") return "is-ok";
  if (band === "weak") return "is-warn";
  if (band === "medium") return "is-neutral";
  return "";
};
