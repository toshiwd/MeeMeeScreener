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
const LINKED_DAILY_MONTHLY_BOX_SUFFIX = "daily-monthly-boxes";

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

const hasDrawingsInStorage = (key: string) => {
  try {
    return localStorage.getItem(key) != null;
  } catch {
    return false;
  }
};

const drawBoxIdentity = (box: DrawBox) =>
  [
    Number(box.startTime).toFixed(3),
    Number(box.endTime).toFixed(3),
    Number(box.topPrice).toFixed(6),
    Number(box.bottomPrice).toFixed(6),
    box.color ?? "",
    box.opacity ?? "",
    box.lineWidth ?? ""
  ].join("|");

const mergeDrawBoxes = (...boxSets: DrawBox[][]) => {
  const seen = new Set<string>();
  const merged: DrawBox[] = [];
  boxSets.flat().forEach((box) => {
    if (!box || typeof box !== "object") return;
    const identity = drawBoxIdentity(box);
    if (seen.has(identity)) return;
    seen.add(identity);
    merged.push(box);
  });
  return merged;
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
  const buildLinkedBoxKey = (symbol: string | null | undefined) =>
    symbol ? `${DRAWING_STORAGE_PREFIX}:${symbol}:${LINKED_DAILY_MONTHLY_BOX_SUFFIX}` : null;

  const dailyDrawingKey = useMemo(() => buildDrawingKey(code, "daily"), [code]);
  const weeklyDrawingKey = useMemo(() => buildDrawingKey(code, "weekly"), [code]);
  const monthlyDrawingKey = useMemo(() => buildDrawingKey(code, "monthly"), [code]);
  const linkedDailyMonthlyBoxKey = useMemo(() => buildLinkedBoxKey(code), [code]);
  const compareDailyDrawingKey = useMemo(() => buildDrawingKey(compareCode, "daily"), [compareCode]);
  const compareMonthlyDrawingKey = useMemo(
    () => buildDrawingKey(compareCode, "monthly"),
    [compareCode]
  );
  const compareLinkedDailyMonthlyBoxKey = useMemo(
    () => buildLinkedBoxKey(compareCode),
    [compareCode]
  );

  const resolveDrawBoxKey = (key: string | null) => {
    if (!key) return null;
    if (key === dailyDrawingKey || key === monthlyDrawingKey) {
      return linkedDailyMonthlyBoxKey ?? key;
    }
    if (key === compareDailyDrawingKey || key === compareMonthlyDrawingKey) {
      return compareLinkedDailyMonthlyBoxKey ?? key;
    }
    return key;
  };

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
  const resolveFrameDrawings = (key: string | null, linkedBoxKey: string | null) => {
    const drawings = resolveDrawings(key);
    if (!linkedBoxKey) return drawings;
    return {
      ...drawings,
      drawBoxes: resolveDrawings(linkedBoxKey).drawBoxes
    };
  };

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
    updateDrawings(resolveDrawBoxKey(key), (prev) => ({
      ...prev,
      drawBoxes: [...prev.drawBoxes, box]
    }));
  const updateDrawBox = (key: string | null) => (index: number, box: DrawBox) =>
    updateDrawings(resolveDrawBoxKey(key), (prev) => {
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
    updateDrawings(resolveDrawBoxKey(key), (prev) => ({
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
      linkedDailyMonthlyBoxKey,
      compareDailyDrawingKey,
      compareMonthlyDrawingKey,
      compareLinkedDailyMonthlyBoxKey
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
      compareMonthlyDrawingKey,
      linkedDailyMonthlyBoxKey,
      compareLinkedDailyMonthlyBoxKey
    ].filter(Boolean) as string[];
    if (!keys.length) return;
    setDrawingsByKey((prev) => {
      let next = prev;
      let changed = false;
      const ensureNext = () => {
        if (!changed) {
          next = { ...prev };
          changed = true;
        }
      };
      const ensureLoaded = (key: string | null) => {
        if (!key) return emptyDrawingsRef.current;
        if (next[key]) return next[key];
        const loaded = loadDrawingsFromStorage(key);
        ensureNext();
        next[key] = loaded;
        return loaded;
      };
      keys.forEach((key) => {
        ensureLoaded(key);
      });
      [
        { linkedKey: linkedDailyMonthlyBoxKey, sourceKeys: [dailyDrawingKey, monthlyDrawingKey] },
        {
          linkedKey: compareLinkedDailyMonthlyBoxKey,
          sourceKeys: [compareDailyDrawingKey, compareMonthlyDrawingKey]
        }
      ].forEach(({ linkedKey, sourceKeys }) => {
        if (!linkedKey || hasDrawingsInStorage(linkedKey)) return;
        const linked = ensureLoaded(linkedKey);
        const sourceBoxes = sourceKeys.map((sourceKey) => ensureLoaded(sourceKey).drawBoxes);
        const merged = mergeDrawBoxes(linked.drawBoxes, ...sourceBoxes);
        if (merged.length === linked.drawBoxes.length) return;
        const migrated = { ...linked, drawBoxes: merged };
        ensureNext();
        next[linkedKey] = migrated;
        saveDrawingsToStorage(linkedKey, migrated);
      });
      return changed ? next : prev;
    });
  }, [
    dailyDrawingKey,
    weeklyDrawingKey,
    monthlyDrawingKey,
    compareDailyDrawingKey,
    compareMonthlyDrawingKey,
    linkedDailyMonthlyBoxKey,
    compareLinkedDailyMonthlyBoxKey
  ]);

  return {
    dailyDrawingKey,
    weeklyDrawingKey,
    monthlyDrawingKey,
    compareDailyDrawingKey,
    compareMonthlyDrawingKey,
    dailyDrawings: resolveFrameDrawings(dailyDrawingKey, linkedDailyMonthlyBoxKey),
    weeklyDrawings: resolveDrawings(weeklyDrawingKey),
    monthlyDrawings: resolveFrameDrawings(monthlyDrawingKey, linkedDailyMonthlyBoxKey),
    compareDailyDrawings: resolveFrameDrawings(
      compareDailyDrawingKey,
      compareLinkedDailyMonthlyBoxKey
    ),
    compareMonthlyDrawings: resolveFrameDrawings(
      compareMonthlyDrawingKey,
      compareLinkedDailyMonthlyBoxKey
    ),
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
