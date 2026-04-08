import type { Box } from "./storeTypes";
import type { BatchBarsRequestTimeframe } from "./routes/detail/batchBarsRequest";

const CACHE_DB_NAME = "meemee-chart-cache";
const CACHE_DB_VERSION = 1;
const CACHE_STORE_NAME = "frames";
const CACHE_MAX_ENTRIES = 1500;
const CACHE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const ACTIVE_DATA_VERSION_STORAGE_KEY = "meemee.chart-cache.active-version";

type ChartCacheRecord = {
  id: string;
  baseKey: string;
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  asof: string;
  includeBoxes: boolean;
  dataVersion: string;
  bars: number[][];
  boxes: Box[];
  fetchedAt: number;
  touchedAt: number;
};

export type PersistentChartCacheKeyParts = {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  asof?: string | null;
  includeBoxes?: boolean;
  dataVersion?: string | null;
};

export type PersistentChartCacheEntry = {
  bars: number[][];
  boxes: Box[];
  fetchedAt: number;
  dataVersion: string;
};

type VersionListener = (nextDataVersion: string, previousDataVersion: string | null) => void;

const memoryCache = new Map<string, ChartCacheRecord>();
const versionListeners = new Set<VersionListener>();
let openDbPromise: Promise<IDBDatabase | null> | null = null;

const canUseDomStorage = () =>
  typeof window !== "undefined" && typeof window.localStorage !== "undefined";

const canUseIndexedDb = () =>
  typeof indexedDB !== "undefined" && indexedDB != null;

const normalizeCode = (value: string) => value.trim();
const normalizeAsof = (value?: string | null) =>
  typeof value === "string" && value.trim() ? value.trim() : "";
const normalizeIncludeBoxes = (value?: boolean) => value === true;

export const buildPersistentChartCacheBaseKey = ({
  code,
  timeframe,
  limit,
  asof,
  includeBoxes,
}: Omit<PersistentChartCacheKeyParts, "dataVersion">) =>
  `${normalizeCode(code)}|${timeframe}|${Math.max(1, Math.floor(limit))}|${normalizeAsof(asof)}|boxes:${normalizeIncludeBoxes(includeBoxes) ? 1 : 0}`;

export const buildPersistentChartCacheKey = ({
  dataVersion,
  ...rest
}: PersistentChartCacheKeyParts) =>
  `${buildPersistentChartCacheBaseKey(rest)}|version:${(dataVersion ?? getActiveChartDataVersion() ?? "unknown").trim() || "unknown"}`;

const toEntry = (record: ChartCacheRecord | null | undefined): PersistentChartCacheEntry | null =>
  record
    ? {
        bars: record.bars,
        boxes: record.boxes,
        fetchedAt: record.fetchedAt,
        dataVersion: record.dataVersion,
      }
    : null;

const readActiveDataVersionFromStorage = () => {
  if (!canUseDomStorage()) return null;
  const raw = window.localStorage.getItem(ACTIVE_DATA_VERSION_STORAGE_KEY);
  return raw && raw.trim() ? raw.trim() : null;
};

const writeActiveDataVersionToStorage = (dataVersion: string) => {
  if (!canUseDomStorage()) return;
  window.localStorage.setItem(ACTIVE_DATA_VERSION_STORAGE_KEY, dataVersion);
};

export const getActiveChartDataVersion = () => readActiveDataVersionFromStorage();

const touchMemoryRecord = (record: ChartCacheRecord) => {
  const touchedRecord = { ...record, touchedAt: Date.now() };
  memoryCache.set(record.id, touchedRecord);
  return touchedRecord;
};

const upsertMemoryRecord = (record: ChartCacheRecord) => {
  memoryCache.set(record.id, record);
};

const deleteMemoryRecordsForVersion = (dataVersion: string) => {
  for (const [key, record] of memoryCache.entries()) {
    if (record.dataVersion !== dataVersion) {
      memoryCache.delete(key);
    }
  }
};

const deleteMemoryOverflow = () => {
  const cutoff = Date.now() - CACHE_MAX_AGE_MS;
  const records = [...memoryCache.values()]
    .filter((record) => record.touchedAt < cutoff)
    .sort((left, right) => left.touchedAt - right.touchedAt);
  records.forEach((record) => {
    memoryCache.delete(record.id);
  });
  if (memoryCache.size <= CACHE_MAX_ENTRIES) return;
  const overflow = [...memoryCache.values()]
    .sort((left, right) => left.touchedAt - right.touchedAt)
    .slice(0, Math.max(0, memoryCache.size - CACHE_MAX_ENTRIES));
  overflow.forEach((record) => {
    memoryCache.delete(record.id);
  });
};

const openCacheDb = async () => {
  if (!canUseIndexedDb()) return null;
  if (openDbPromise) return openDbPromise;
  openDbPromise = new Promise<IDBDatabase | null>((resolve) => {
    const request = indexedDB.open(CACHE_DB_NAME, CACHE_DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      const store = db.objectStoreNames.contains(CACHE_STORE_NAME)
        ? request.transaction?.objectStore(CACHE_STORE_NAME)
        : db.createObjectStore(CACHE_STORE_NAME, { keyPath: "id" });
      if (!store) return;
      if (!store.indexNames.contains("baseKey")) {
        store.createIndex("baseKey", "baseKey", { unique: false });
      }
      if (!store.indexNames.contains("dataVersion")) {
        store.createIndex("dataVersion", "dataVersion", { unique: false });
      }
      if (!store.indexNames.contains("touchedAt")) {
        store.createIndex("touchedAt", "touchedAt", { unique: false });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
    request.onblocked = () => resolve(null);
  });
  return openDbPromise;
};

const withStore = async <T>(
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => Promise<T>
) => {
  const db = await openCacheDb();
  if (!db) return null;
  const tx = db.transaction(CACHE_STORE_NAME, mode);
  const store = tx.objectStore(CACHE_STORE_NAME);
  return run(store);
};

const requestToPromise = <T>(request: IDBRequest<T>) =>
  new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });

const buildRecord = (
  parts: PersistentChartCacheKeyParts,
  entry: PersistentChartCacheEntry
): ChartCacheRecord => {
  const code = normalizeCode(parts.code);
  const timeframe = parts.timeframe;
  const limit = Math.max(1, Math.floor(parts.limit));
  const asof = normalizeAsof(parts.asof);
  const includeBoxes = normalizeIncludeBoxes(parts.includeBoxes);
  const dataVersion = (parts.dataVersion ?? entry.dataVersion ?? getActiveChartDataVersion() ?? "unknown").trim() || "unknown";
  const baseKey = buildPersistentChartCacheBaseKey({
    code,
    timeframe,
    limit,
    asof,
    includeBoxes,
  });
  const now = Date.now();
  return {
    id: `${baseKey}|version:${dataVersion}`,
    baseKey,
    code,
    timeframe,
    limit,
    asof,
    includeBoxes,
    dataVersion,
    bars: entry.bars,
    boxes: entry.boxes,
    fetchedAt: entry.fetchedAt,
    touchedAt: now,
  };
};

const findLatestMemoryRecord = (baseKey: string, dataVersion?: string | null) => {
  let selected: ChartCacheRecord | null = null;
  for (const record of memoryCache.values()) {
    if (record.baseKey !== baseKey) continue;
    if (dataVersion && record.dataVersion !== dataVersion) continue;
    if (!selected || record.touchedAt > selected.touchedAt) {
      selected = record;
    }
  }
  return selected;
};

export const subscribeToChartDataVersionChange = (listener: VersionListener) => {
  versionListeners.add(listener);
  return () => {
    versionListeners.delete(listener);
  };
};

export const applyChartDataVersion = async (dataVersion?: string | null) => {
  const normalized = typeof dataVersion === "string" ? dataVersion.trim() : "";
  if (!normalized) {
    return {
      changed: false,
      current: getActiveChartDataVersion(),
      previous: getActiveChartDataVersion(),
    };
  }
  const previous = getActiveChartDataVersion();
  if (previous === normalized) {
    return { changed: false, current: normalized, previous };
  }
  writeActiveDataVersionToStorage(normalized);
  deleteMemoryRecordsForVersion(normalized);
  await invalidateChartCacheByVersion(normalized);
  versionListeners.forEach((listener) => listener(normalized, previous));
  return { changed: true, current: normalized, previous };
};

export const invalidateChartCacheByVersion = async (dataVersion: string) => {
  const normalized = dataVersion.trim();
  deleteMemoryRecordsForVersion(normalized);
  if (!canUseIndexedDb()) return;
  await withStore("readwrite", async (store) => {
    const index = store.index("dataVersion");
    const request = index.openCursor();
    await new Promise<void>((resolve) => {
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          resolve();
          return;
        }
        const value = cursor.value as ChartCacheRecord;
        if (value.dataVersion !== normalized) {
          cursor.delete();
        }
        cursor.continue();
      };
      request.onerror = () => resolve();
    });
  });
};

export const getPersistentChartFrame = async (
  parts: Omit<PersistentChartCacheKeyParts, "dataVersion"> & { dataVersion?: string | null }
) => {
  const baseKey = buildPersistentChartCacheBaseKey(parts);
  const requestedVersion = parts.dataVersion ?? getActiveChartDataVersion();
  const memoryMatch = findLatestMemoryRecord(baseKey, requestedVersion);
  if (memoryMatch) {
    const touched = touchMemoryRecord(memoryMatch);
    return toEntry(touched);
  }
  if (!canUseIndexedDb()) return null;
  if (requestedVersion) {
    const directRecord = await withStore("readonly", async (store) => {
      const request = store.get(
        buildPersistentChartCacheKey({ ...parts, dataVersion: requestedVersion })
      );
      try {
        return (await requestToPromise(request)) as ChartCacheRecord | undefined;
      } catch {
        return null;
      }
    });
    if (directRecord) {
      const normalized = touchMemoryRecord(directRecord);
      return toEntry(normalized);
    }
  }
  const latestRecord = await withStore("readonly", async (store) => {
    const index = store.index("baseKey");
    const request = index.getAll(IDBKeyRange.only(baseKey));
    try {
      const values = ((await requestToPromise(request)) as ChartCacheRecord[]) ?? [];
      if (!values.length) return null;
      return values.sort((left, right) => right.touchedAt - left.touchedAt)[0] ?? null;
    } catch {
      return null;
    }
  });
  if (!latestRecord) return null;
  const touched = touchMemoryRecord(latestRecord);
  return toEntry(touched);
};

export const getPersistentChartFrames = async (
  entries: Array<Omit<PersistentChartCacheKeyParts, "dataVersion"> & { dataVersion?: string | null }>
) => {
  const resolved = await Promise.all(
    entries.map(async (entry) => [buildPersistentChartCacheBaseKey(entry), await getPersistentChartFrame(entry)] as const)
  );
  return new Map(resolved);
};

export const setPersistentChartFrame = async (
  parts: PersistentChartCacheKeyParts,
  entry: PersistentChartCacheEntry
) => {
  const record = buildRecord(parts, entry);
  upsertMemoryRecord(record);
  deleteMemoryOverflow();
  if (!canUseIndexedDb()) return;
  await withStore("readwrite", async (store) => {
    try {
      await requestToPromise(store.put(record));
    } catch {
      return;
    }
  });
  await prunePersistentChartCache();
};

export const prunePersistentChartCache = async () => {
  deleteMemoryOverflow();
  if (!canUseIndexedDb()) return;
  const db = await openCacheDb();
  if (!db) return;
  const cutoff = Date.now() - CACHE_MAX_AGE_MS;
  await withStore("readwrite", async (store) => {
    const touchedIndex = store.index("touchedAt");
    const expiredRequest = touchedIndex.openCursor();
    let entryCount = 0;
    const allRecords: ChartCacheRecord[] = [];
    await new Promise<void>((resolve) => {
      expiredRequest.onsuccess = () => {
        const cursor = expiredRequest.result;
        if (!cursor) {
          resolve();
          return;
        }
        const value = cursor.value as ChartCacheRecord;
        entryCount += 1;
        if (value.touchedAt < cutoff) {
          cursor.delete();
        } else {
          allRecords.push(value);
        }
        cursor.continue();
      };
      expiredRequest.onerror = () => resolve();
    });
    if (entryCount <= CACHE_MAX_ENTRIES) return;
    const overflow = allRecords
      .sort((left, right) => left.touchedAt - right.touchedAt)
      .slice(0, Math.max(0, entryCount - CACHE_MAX_ENTRIES));
    await Promise.all(
      overflow.map((record) =>
        requestToPromise(store.delete(record.id)).catch(() => undefined)
      )
    );
  });
};

export const clearPersistentChartCache = async () => {
  memoryCache.clear();
  openDbPromise = null;
  if (!canUseIndexedDb()) return;
  await new Promise<void>((resolve) => {
    const request = indexedDB.deleteDatabase(CACHE_DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
    request.onblocked = () => resolve();
  });
};

export const __resetPersistentChartCacheForTests = async () => {
  if (canUseDomStorage()) {
    window.localStorage.removeItem(ACTIVE_DATA_VERSION_STORAGE_KEY);
  }
  versionListeners.clear();
  await clearPersistentChartCache();
};
