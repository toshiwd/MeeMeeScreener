export type DrawingTool = "timeZone" | "priceBand" | "drawBox" | "horizontalLine";

export type DrawingHitKind = "timeZone" | "priceBand" | "drawBox" | "horizontalLine";

export type TimeZoneShape = {
  side: "buy" | "sell";
  startTime: number;
  endTime: number;
  color?: string;
};

export type PriceBandShape = {
  topPrice: number;
  bottomPrice: number;
  opacity: number;
  lineWidth?: number;
};

export type DrawBoxShape = {
  startTime: number;
  endTime: number;
  topPrice: number;
  bottomPrice: number;
  color?: string;
  opacity?: number;
  lineWidth?: number;
};

export const getHitKindsForTool = (tool: DrawingTool | null): DrawingHitKind[] => {
  if (!tool) return ["horizontalLine", "drawBox", "priceBand", "timeZone"];
  if (tool === "horizontalLine") return ["horizontalLine"];
  if (tool === "drawBox") return ["drawBox"];
  if (tool === "priceBand") return ["priceBand"];
  return ["timeZone"];
};

export const buildTimeZoneShape = (
  startTime: number | null | undefined,
  endTime: number | null | undefined,
  side: "buy" | "sell" = "buy",
  color?: string
): TimeZoneShape | null => {
  if (!Number.isFinite(startTime) || !Number.isFinite(endTime)) return null;
  return {
    side,
    startTime: Math.min(startTime, endTime),
    endTime: Math.max(startTime, endTime),
    color
  };
};

export const buildPriceBandShape = (
  startPrice: number | null | undefined,
  endPrice: number | null | undefined,
  opacity = 0.12,
  lineWidth?: number
): PriceBandShape | null => {
  if (!Number.isFinite(startPrice) || !Number.isFinite(endPrice)) return null;
  return {
    topPrice: Math.max(startPrice, endPrice),
    bottomPrice: Math.min(startPrice, endPrice),
    opacity,
    lineWidth
  };
};

export const buildDrawBoxShape = (
  startTime: number | null | undefined,
  endTime: number | null | undefined,
  startPrice: number | null | undefined,
  endPrice: number | null | undefined,
  options?: Pick<DrawBoxShape, "opacity" | "color" | "lineWidth">
): DrawBoxShape | null => {
  if (
    !Number.isFinite(startTime) ||
    !Number.isFinite(endTime) ||
    !Number.isFinite(startPrice) ||
    !Number.isFinite(endPrice)
  ) {
    return null;
  }
  return {
    startTime: Math.min(startTime, endTime),
    endTime: Math.max(startTime, endTime),
    topPrice: Math.max(startPrice, endPrice),
    bottomPrice: Math.min(startPrice, endPrice),
    opacity: options?.opacity,
    color: options?.color,
    lineWidth: options?.lineWidth
  };
};
