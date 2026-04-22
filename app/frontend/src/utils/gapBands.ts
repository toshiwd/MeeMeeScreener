export type GapBand = {
  direction: "up" | "down";
  topPrice: number;
  bottomPrice: number;
  createdAt: number;
  filledAt: number | null;
  remainingTopPrice?: number;
  remainingBottomPrice?: number;
  fillRatio?: number;
};

type CandleLike = {
  time: number;
  high: number;
  low: number;
  close?: number;
};

const byCreatedAtDesc = (a: GapBand, b: GapBand) => b.createdAt - a.createdAt;

export const computeGapBands = (
  candles: CandleLike[],
  asOf: number,
  maxCount: number,
  minGapRatio = 0.004
) => {
  if (!Array.isArray(candles) || candles.length < 2) return [];
  if (!Number.isFinite(maxCount) || maxCount <= 0) return [];
  if (!Number.isFinite(minGapRatio) || minGapRatio < 0) return [];
  const normalizedAsOf = Number.isFinite(asOf)
    ? asOf
    : Math.max(...candles.map((c) => c.time).filter((time) => Number.isFinite(time)));
  if (!Number.isFinite(normalizedAsOf)) return [];

  const sorted = [...candles]
    .filter(
      (c) =>
        Number.isFinite(c.time) &&
        Number.isFinite(c.high) &&
        Number.isFinite(c.low)
    )
    .sort((a, b) => a.time - b.time);
  const limited = sorted.filter((c) => c.time <= normalizedAsOf);
  if (limited.length < 2) return [];

  const gaps: GapBand[] = [];

  for (let i = 1; i < limited.length; i += 1) {
    const prev = limited[i - 1];
    const current = limited[i];

    if (!Number.isFinite(prev.high) || !Number.isFinite(prev.low)) continue;
    if (!Number.isFinite(current.high) || !Number.isFinite(current.low)) continue;

    const gapBase =
      typeof prev.close === "number" && Number.isFinite(prev.close)
        ? prev.close
        : (prev.high + prev.low) / 2;
    if (!Number.isFinite(gapBase) || gapBase <= 0) continue;

    if (current.low > prev.high) {
      const gapSize = current.low - prev.high;
      if (gapSize / gapBase < minGapRatio) continue;
      gaps.push({
        direction: "up",
        topPrice: current.low,
        bottomPrice: prev.high,
        createdAt: current.time,
        filledAt: null,
        remainingTopPrice: current.low,
        remainingBottomPrice: prev.high,
        fillRatio: 0
      });
      continue;
    }

    if (current.high < prev.low) {
      const gapSize = prev.low - current.high;
      if (gapSize / gapBase < minGapRatio) continue;
      gaps.push({
        direction: "down",
        topPrice: prev.low,
        bottomPrice: current.high,
        createdAt: current.time,
        filledAt: null,
        remainingTopPrice: prev.low,
        remainingBottomPrice: current.high,
        fillRatio: 0
      });
    }
  }

  if (gaps.length === 0) return [];

  for (const gap of gaps) {
    for (const candle of limited) {
      if (candle.time <= gap.createdAt) continue;
      if (gap.direction === "up" && candle.low <= gap.bottomPrice) {
        gap.filledAt = candle.time;
        gap.remainingTopPrice = gap.bottomPrice;
        gap.remainingBottomPrice = gap.bottomPrice;
        gap.fillRatio = 1;
        break;
      }
      if (gap.direction === "up" && candle.low < (gap.remainingTopPrice ?? gap.topPrice)) {
        const nextTop = Math.max(
          gap.bottomPrice,
          Math.min(gap.topPrice, candle.low)
        );
        gap.remainingTopPrice = Math.min(gap.remainingTopPrice ?? gap.topPrice, nextTop);
      }
      if (gap.direction === "down" && candle.high >= gap.topPrice) {
        gap.filledAt = candle.time;
        gap.remainingTopPrice = gap.topPrice;
        gap.remainingBottomPrice = gap.topPrice;
        gap.fillRatio = 1;
        break;
      }
      if (
        gap.direction === "down" &&
        candle.high > (gap.remainingBottomPrice ?? gap.bottomPrice)
      ) {
        const nextBottom = Math.min(
          gap.topPrice,
          Math.max(gap.bottomPrice, candle.high)
        );
        gap.remainingBottomPrice = Math.max(gap.remainingBottomPrice ?? gap.bottomPrice, nextBottom);
      }
    }

    const originalHeight = gap.topPrice - gap.bottomPrice;
    const remainingTop =
      typeof gap.remainingTopPrice === "number" && Number.isFinite(gap.remainingTopPrice)
        ? gap.remainingTopPrice
        : gap.topPrice;
    const remainingBottom =
      typeof gap.remainingBottomPrice === "number" && Number.isFinite(gap.remainingBottomPrice)
        ? gap.remainingBottomPrice
        : gap.bottomPrice;
    const remainingHeight = Math.max(0, remainingTop - remainingBottom);
    gap.fillRatio =
      originalHeight > 0
        ? Math.max(0, Math.min(1, 1 - remainingHeight / originalHeight))
        : gap.fillRatio ?? 0;
  }

  return gaps.sort(byCreatedAtDesc).slice(0, maxCount);
};
