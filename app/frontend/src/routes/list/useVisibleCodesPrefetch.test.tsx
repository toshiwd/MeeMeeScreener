// @vitest-environment jsdom
import { act, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useVisibleCodesPrefetch } from "./useVisibleCodesPrefetch";
import type { GridTimeframe } from "../../storeTypes";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

type Item = { code: string };

function Probe({
  items,
  backendReady,
  ensureBarsForVisible
}: {
  items: Item[];
  backendReady: boolean;
  ensureBarsForVisible: (timeframe: GridTimeframe, codes: string[], reason?: string) => Promise<void>;
}) {
  const { handleVisibleItemsChange } = useVisibleCodesPrefetch<Item>({
    backendReady,
    timeframe: "daily",
    reason: "test-visible-range",
    ensureBarsForVisible
  });

  useEffect(() => {
    handleVisibleItemsChange(items);
  }, [handleVisibleItemsChange, items]);

  return null;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useVisibleCodesPrefetch", () => {
  it("dedupes unchanged visible ranges and refetches only when the range changes", async () => {
    const ensureBarsForVisible = vi.fn(async () => {});
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(
        <Probe
          items={[
            { code: "1001" },
            { code: "1002" }
          ]}
          backendReady
          ensureBarsForVisible={ensureBarsForVisible}
        />
      );
    });
    expect(ensureBarsForVisible).toHaveBeenCalledTimes(1);
    expect(ensureBarsForVisible).toHaveBeenLastCalledWith("daily", ["1001", "1002"], "test-visible-range");

    await act(async () => {
      root.render(
        <Probe
          items={[
            { code: "1001" },
            { code: "1002" }
          ]}
          backendReady
          ensureBarsForVisible={ensureBarsForVisible}
        />
      );
    });
    expect(ensureBarsForVisible).toHaveBeenCalledTimes(1);

    await act(async () => {
      root.render(
        <Probe
          items={[
            { code: "1002" },
            { code: "1003" }
          ]}
          backendReady
          ensureBarsForVisible={ensureBarsForVisible}
        />
      );
    });
    expect(ensureBarsForVisible).toHaveBeenCalledTimes(2);
    expect(ensureBarsForVisible).toHaveBeenLastCalledWith("daily", ["1002", "1003"], "test-visible-range");

    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("does not prefetch until backend is ready", async () => {
    const ensureBarsForVisible = vi.fn(async () => {});
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(
        <Probe
          items={[
            { code: "1001" },
            { code: "1002" }
          ]}
          backendReady={false}
          ensureBarsForVisible={ensureBarsForVisible}
        />
      );
    });

    expect(ensureBarsForVisible).not.toHaveBeenCalled();

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
