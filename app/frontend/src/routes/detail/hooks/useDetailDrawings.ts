import { useEffect, useMemo, useRef, useState } from "react";
import type { DrawBox, HorizontalLine, PriceBand, TimeZone } from "../../../components/DetailChart";

type Timeframe = "daily" | "weekly" | "monthly";

type ChartDrawings = {
  timeZones: TimeZone[];
  priceBands: PriceBand[];
  drawBoxes: DrawBox[];
  horizontalLines: HorizontalLine[];
};

type Params = {
  code: string | null | undefined;
  compareCode: string | null | undefined;
  onResetSelection?: () => void;
};

const DRAWING_STORAGE_PREFIX = "drawings:v1";

const createEmptyDrawings = (): ChartDrawings => ({
  timeZones: [],
  priceBands: [],
  drawBoxes: [],
  horizontalLines: []
});

const normalizeDrawings = (value: any): ChartDrawings => {
  if (!value || typeof value !== "object") return createEmptyDrawings();
  return {
    timeZones: Array.isArray(value.timeZones) ? value.timeZones : [],
    priceBands: Array.isArray(value.priceBands) ? value.priceBands : [],
    drawBoxes: Array.isArray(value.drawBoxes) ? value.drawBoxes : [],
    horizontalLines: Array.isArray(value.horizontalLines) ? value.horizontalLines : []
  };
};

const loadDrawingsFromStorage = (key: string): ChartDrawings => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return createEmptyDrawings();
    return normalizeDrawings(JSON.parse(raw));
  } catch {
    return createEmptyDrawings();
  }
};

const saveDrawingsToStorage = (key: string, drawings: ChartDrawings) => {
  try {
    localStorage.setItem(key, JSON.stringify(drawings));
  } catch {
    // ignore storage errors
  }
};

export function useDetailDrawings({ code, compareCode, onResetSelection }: Params) {
  const [drawingsByKey, setDrawingsByKey] = useState<Record<string, ChartDrawings>>({});
  const emptyDrawingsRef = useRef<ChartDrawings>(createEmptyDrawings());
  const resetSelectionRef = useRef(onResetSelection);

  useEffect(() => {
    resetSelectionRef.current = onResetSelection;
  }, [onResetSelection]);

  const buildDrawingKey = (symbol: string | null | undefined, timeframe: Timeframe) =>
    symbol ? `${DRAWING_STORAGE_PREFIX}:${symbol}:${timeframe}` : null;

  const dailyDrawingKey = useMemo(() => buildDrawingKey(code, "daily"), [code]);
  const weeklyDrawingKey = useMemo(() => buildDrawingKey(code, "weekly"), [code]);
  const monthlyDrawingKey = useMemo(() => buildDrawingKey(code, "monthly"), [code]);
  const compareDailyDrawingKey = useMemo(() => buildDrawingKey(compareCode, "daily"), [compareCode]);
  const compareMonthlyDrawingKey = useMemo(
    () => buildDrawingKey(compareCode, "monthly"),
    [compareCode]
  );

  const updateDrawings = (key: string | null, updater: (prev: ChartDrawings) => ChartDrawings) => {
    if (!key) return;
    setDrawingsByKey((prev) => {
      const current = prev[key] ?? emptyDrawingsRef.current;
      const nextValue = updater(current);
      const next = { ...prev, [key]: nextValue };
      saveDrawingsToStorage(key, nextValue);
      return next;
    });
  };

  const resolveDrawings = (key: string | null) =>
    key ? drawingsByKey[key] ?? emptyDrawingsRef.current : emptyDrawingsRef.current;

  const addTimeZone = (key: string | null) => (zone: TimeZone) =>
    updateDrawings(key, (prev) => ({ ...prev, timeZones: [...prev.timeZones, zone] }));
  const updateTimeZone = (key: string | null) => (index: number, zone: TimeZone) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.timeZones];
      if (!next[index]) return prev;
      next[index] = zone;
      return { ...prev, timeZones: next };
    });

  const addPriceBand = (key: string | null) => (band: PriceBand) =>
    updateDrawings(key, (prev) => ({ ...prev, priceBands: [...prev.priceBands, band] }));
  const updatePriceBand = (key: string | null) => (index: number, band: PriceBand) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.priceBands];
      if (!next[index]) return prev;
      next[index] = band;
      return { ...prev, priceBands: next };
    });

  const addDrawBox = (key: string | null) => (box: DrawBox) =>
    updateDrawings(key, (prev) => ({ ...prev, drawBoxes: [...prev.drawBoxes, box] }));
  const updateDrawBox = (key: string | null) => (index: number, box: DrawBox) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.drawBoxes];
      if (!next[index]) return prev;
      next[index] = box;
      return { ...prev, drawBoxes: next };
    });

  const addHorizontalLine = (key: string | null) => (line: HorizontalLine) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      horizontalLines: [...prev.horizontalLines, line]
    }));
  const updateHorizontalLine = (key: string | null) => (index: number, line: HorizontalLine) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.horizontalLines];
      if (!next[index]) return prev;
      next[index] = line;
      return { ...prev, horizontalLines: next };
    });
  const deleteTimeZone = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      timeZones: prev.timeZones.filter((_, i) => i !== index)
    }));
  const deletePriceBand = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      priceBands: prev.priceBands.filter((_, i) => i !== index)
    }));
  const deleteDrawBox = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      drawBoxes: prev.drawBoxes.filter((_, i) => i !== index)
    }));
  const deleteHorizontalLine = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      horizontalLines: prev.horizontalLines.filter((_, i) => i !== index)
    }));

  const resetAllDrawings = () => {
    const keys = [
      dailyDrawingKey,
      weeklyDrawingKey,
      monthlyDrawingKey,
      compareDailyDrawingKey,
      compareMonthlyDrawingKey
    ].filter(Boolean) as string[];
    if (!keys.length) return;
    setDrawingsByKey((prev) => {
      const next = { ...prev };
      keys.forEach((key) => {
        const empty = createEmptyDrawings();
        next[key] = empty;
        saveDrawingsToStorage(key, empty);
      });
      return next;
    });
    resetSelectionRef.current?.();
  };

  useEffect(() => {
    const keys = [
      dailyDrawingKey,
      weeklyDrawingKey,
      monthlyDrawingKey,
      compareDailyDrawingKey,
      compareMonthlyDrawingKey
    ].filter(Boolean) as string[];
    if (!keys.length) return;
    setDrawingsByKey((prev) => {
      let next = prev;
      let changed = false;
      keys.forEach((key) => {
        if (next[key]) return;
        const loaded = loadDrawingsFromStorage(key);
        if (!changed) {
          next = { ...prev };
          changed = true;
        }
        next[key] = loaded;
      });
      return changed ? next : prev;
    });
  }, [dailyDrawingKey, weeklyDrawingKey, monthlyDrawingKey, compareDailyDrawingKey, compareMonthlyDrawingKey]);

  return {
    dailyDrawingKey,
    weeklyDrawingKey,
    monthlyDrawingKey,
    compareDailyDrawingKey,
    compareMonthlyDrawingKey,
    dailyDrawings: resolveDrawings(dailyDrawingKey),
    weeklyDrawings: resolveDrawings(weeklyDrawingKey),
    monthlyDrawings: resolveDrawings(monthlyDrawingKey),
    compareDailyDrawings: resolveDrawings(compareDailyDrawingKey),
    compareMonthlyDrawings: resolveDrawings(compareMonthlyDrawingKey),
    addTimeZone,
    updateTimeZone,
    addPriceBand,
    updatePriceBand,
    addDrawBox,
    updateDrawBox,
    addHorizontalLine,
    updateHorizontalLine,
    deleteTimeZone,
    deletePriceBand,
    deleteDrawBox,
    deleteHorizontalLine,
    resetAllDrawings,
  };
}
