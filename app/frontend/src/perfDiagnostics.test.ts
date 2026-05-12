// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearPerfDiagnostics,
  getPerfDiagnosticsEvents,
  recordPerfEvent,
} from "./perfDiagnostics";
import { getPrimaryPreloadRoutes, preloadRoute } from "./routePreload";

describe("perfDiagnostics", () => {
  beforeEach(async () => {
    window.localStorage.setItem("meemeePerfDiagnosticsEnabled", "1");
    await clearPerfDiagnostics();
    delete (window as Window & Record<string, unknown>).pywebview;
  });

  it("drops the oldest events when the ring buffer overflows", () => {
    for (let index = 0; index < 4105; index += 1) {
      recordPerfEvent("overflow-test", { index });
    }
    const events = getPerfDiagnosticsEvents();
    expect(events).toHaveLength(4000);
    expect(events[0]?.payload).toMatchObject({ index: 105 });
    expect(events.at(-1)?.payload).toMatchObject({ index: 4104 });
  });

  it("merges extra files into the exported diagnostics payload", async () => {
    const exportPerfDiagnosticsApi = vi.fn().mockResolvedValue({ success: true });
    (window as Window & Record<string, unknown>).pywebview = {
      api: {
        export_perf_diagnostics: exportPerfDiagnosticsApi,
      },
    };

    const { exportPerfDiagnostics } = await import("./perfDiagnostics");
    await exportPerfDiagnostics({
      files: [
        {
          name: "frontend-fatal-test.json",
          content: { ok: true },
        },
      ],
    });

    expect(exportPerfDiagnosticsApi).toHaveBeenCalledTimes(1);
    const payload = exportPerfDiagnosticsApi.mock.calls[0]?.[0] as { files?: Array<{ name?: string }> };
    expect(payload?.files?.some((entry) => entry.name === "frontend-fatal-test.json")).toBe(true);
  });
});

describe("routePreload", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("dedupes repeated preload requests for the same route", async () => {
    const first = preloadRoute("/ranking");
    const second = preloadRoute("/ranking");
    expect(first).toBe(second);
    await expect(first).resolves.toBeTruthy();
  });

  it("keeps detail routes out of idle primary preloading", () => {
    expect(getPrimaryPreloadRoutes()).not.toContain("/detail/:code");
    expect(getPrimaryPreloadRoutes()).not.toContain("/detail-v2/:code");
  });

  it("loads the full detail surface for the detail v2 route instead of the shell route", async () => {
    const detailModule = await preloadRoute("/detail/7203");
    const detailV2Module = await preloadRoute("/detail-v2/7203");

    expect(detailV2Module?.default).toBe(detailModule?.default);
    expect(detailV2Module?.default.name).not.toBe("DetailV2View");
  });
});
