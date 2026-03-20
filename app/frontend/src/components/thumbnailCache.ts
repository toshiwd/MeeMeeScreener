import type { MaSetting } from "../store";

const cache = new Map<string, string>();

export const buildThumbnailSizeKey = (width: number, height: number, ratio: number) =>
  `${width}x${height}@${ratio}`;

export const buildThumbnailSnapshotCacheKey = (cacheKey: string, renderKey: string, sizeKey: string) =>
  `${cacheKey}:${renderKey}:${sizeKey}`;

const buildSettingsKey = (settings: MaSetting[]) =>
  settings
    .map((setting) => `${setting.period}-${setting.visible}-${setting.color}-${setting.lineWidth}`)
    .join("|");

export const buildThumbnailCacheKey = (
  code: string,
  timeframe: "monthly" | "weekly" | "daily",
  showBoxes: boolean,
  maSettings: MaSetting[],
  theme: "dark" | "light"
) => {
  const settingsKey = buildSettingsKey(maSettings);
  return `${code}:${timeframe}:${showBoxes}:${settingsKey}:${theme}`;
};

export const getThumbnailCache = (key: string) => cache.get(key);

export const setThumbnailCache = (key: string, dataUrl: string) => {
  cache.set(key, dataUrl);
};

export const clearThumbnailCache = () => {
  cache.clear();
};
