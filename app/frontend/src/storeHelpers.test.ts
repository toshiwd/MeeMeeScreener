import { beforeEach, describe, expect, it, vi } from "vitest";
import { getInitialListRangeBars, getInitialSortDir, getInitialSortKey } from "./storeHelpers";

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
    vi.stubGlobal("window", stub);

    expect(getInitialSortKey()).toBe("ma20Dev");
    expect(getInitialSortDir()).toBe("desc");
    expect(getInitialListRangeBars()).toBe(90);
  });
});
