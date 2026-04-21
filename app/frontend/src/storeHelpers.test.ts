import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  getInitialColumns,
  getInitialListRangeBars,
  getInitialRows,
  getInitialSortDir,
  getInitialSortKey,
  persistGridPreset,
  resolveListThumbnailMaSettings
} from "./storeHelpers";

vi.mock("./api", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn()
  },
  setApiErrorReporter: vi.fn()
}));

const createWindowStub = () => {
  const storage = new Map<string, string>();
  return {
    localStorage: {
      getItem: (key: string) => storage.get(key) ?? null,
      setItem: (key: string, value: string) => {
        storage.set(key, value);
      },
      removeItem: (key: string) => {
        storage.delete(key);
      },
      clear: () => {
        storage.clear();
      }
    }
  };
};

describe("storeHelpers defaults", () => {
  beforeEach(() => {
    vi.stubGlobal("window", createWindowStub() as Window);
  });

  it("defaults to code asc and 3x3 list density when storage is empty", () => {
    expect(getInitialSortKey()).toBe("code");
    expect(getInitialSortDir()).toBe("asc");
    expect(getInitialColumns()).toBe(3);
    expect(getInitialRows()).toBe(3);
    expect(getInitialListRangeBars()).toBe(60);
  });

  it("restores saved values when present", () => {
    const stub = createWindowStub() as Window & {
      localStorage: {
        getItem: (key: string) => string | null;
        setItem: (key: string, value: string) => void;
        removeItem: (key: string) => void;
        clear: () => void;
      };
    };
    stub.localStorage.setItem("sortKey", "ma20Dev");
    stub.localStorage.setItem("sortDir", "desc");
    stub.localStorage.setItem("listRangeBars", "90");
    stub.localStorage.setItem("gridPreset", "5");
    vi.stubGlobal("window", stub);

    expect(getInitialSortKey()).toBe("ma20Dev");
    expect(getInitialSortDir()).toBe("desc");
    expect(getInitialColumns()).toBe(4);
    expect(getInitialRows()).toBe(4);
    expect(getInitialListRangeBars()).toBe(90);
  });

  it("clamps written density presets to 4x4", () => {
    const stub = createWindowStub() as Window & {
      localStorage: {
        getItem: (key: string) => string | null;
        setItem: (key: string, value: string) => void;
        removeItem: (key: string) => void;
        clear: () => void;
      };
    };
    vi.stubGlobal("window", stub);

    persistGridPreset(5 as unknown as 1 | 2 | 3 | 4);

    expect(stub.localStorage.getItem("gridPreset")).toBe("4");
    expect(stub.localStorage.getItem("gridCols")).toBeNull();
    expect(stub.localStorage.getItem("gridRows")).toBeNull();
    expect(stub.localStorage.getItem("listCols")).toBeNull();
    expect(stub.localStorage.getItem("listRows")).toBeNull();
  });

  it("restores weekly list thumbnails to the default five MA lines for legacy two-line state", () => {
    const resolved = resolveListThumbnailMaSettings("weekly", [
      { key: "ma1", label: "MA1", period: 7, visible: true, color: "#ef4444", lineWidth: 1 },
      { key: "ma2", label: "MA2", period: 20, visible: true, color: "#22c55e", lineWidth: 1 },
      { key: "ma3", label: "MA3", period: 60, visible: false, color: "#3b82f6", lineWidth: 1 },
      { key: "ma4", label: "MA4", period: 100, visible: false, color: "#a855f7", lineWidth: 1 },
      { key: "ma5", label: "MA5", period: 200, visible: false, color: "#f59e0b", lineWidth: 1 }
    ]);

    expect(resolved).toHaveLength(5);
    expect(resolved.every((setting) => setting.visible)).toBe(true);
    expect(resolved.map((setting) => setting.period)).toEqual([7, 20, 60, 100, 200]);
  });

  it("preserves customized weekly list settings when they do not match the legacy fallback shape", () => {
    const resolved = resolveListThumbnailMaSettings("weekly", [
      { key: "ma1", label: "MA1", period: 5, visible: true, color: "#ef4444", lineWidth: 1 },
      { key: "ma2", label: "MA2", period: 20, visible: true, color: "#22c55e", lineWidth: 1 },
      { key: "ma3", label: "MA3", period: 60, visible: false, color: "#3b82f6", lineWidth: 1 },
      { key: "ma4", label: "MA4", period: 100, visible: false, color: "#a855f7", lineWidth: 1 },
      { key: "ma5", label: "MA5", period: 200, visible: false, color: "#f59e0b", lineWidth: 1 }
    ]);

    expect(resolved[0]?.period).toBe(5);
    expect(resolved.filter((setting) => setting.visible)).toHaveLength(2);
  });
});
