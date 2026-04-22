// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  captureWindowBlob: vi.fn(),
  saveBlobToPerfDiagnostics: vi.fn(),
  exportPerfDiagnostics: vi.fn(),
  recordPerfEvent: vi.fn(),
  updatePerfDiagnosticsSnapshot: vi.fn(),
  getPerfDiagnosticsEvents: vi.fn(() => [{ seq: 1 }]),
}));

vi.mock("./windowScreenshot", () => ({
  captureWindowBlob: mocks.captureWindowBlob,
  saveBlobToPerfDiagnostics: mocks.saveBlobToPerfDiagnostics,
}));

vi.mock("../perfDiagnostics", () => ({
  exportPerfDiagnostics: mocks.exportPerfDiagnostics,
  recordPerfEvent: mocks.recordPerfEvent,
  updatePerfDiagnosticsSnapshot: mocks.updatePerfDiagnosticsSnapshot,
  getPerfDiagnosticsEvents: mocks.getPerfDiagnosticsEvents,
}));

describe("fatalDiagnostics", () => {
  let fatalDiagnostics: typeof import("./fatalDiagnostics");

  beforeEach(async () => {
    vi.resetModules();
    fatalDiagnostics = await import("./fatalDiagnostics");
    mocks.captureWindowBlob.mockReset();
    mocks.saveBlobToPerfDiagnostics.mockReset();
    mocks.exportPerfDiagnostics.mockReset();
    mocks.recordPerfEvent.mockReset();
    mocks.updatePerfDiagnosticsSnapshot.mockReset();
    mocks.getPerfDiagnosticsEvents.mockReset();
    mocks.getPerfDiagnosticsEvents.mockReturnValue([{ seq: 1 }]);
    (window as Window & Record<string, unknown>).__meemeeFatalDiagnosticsQueue = [];
    delete (window as Window & Record<string, unknown>).__meemeeReportFatalDiagnostics;
  });

  it("captures a fatal screenshot and exports a diagnostics bundle", async () => {
    const blob = new Blob(["fatal"], { type: "image/png" });
    mocks.captureWindowBlob.mockResolvedValue({
      success: true,
      blob,
      filename: "MeeMee_Fatal_root_20260422_120000.png",
    });
    mocks.saveBlobToPerfDiagnostics.mockResolvedValue({
      success: true,
      savedPath: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics/frontend-fatal-1.png",
      savedDir: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics",
      fileName: "frontend-fatal-1.png",
    });
    mocks.exportPerfDiagnostics.mockResolvedValue({
      success: true,
      writtenFiles: ["fatal.json"],
    });

    const result = await fatalDiagnostics.reportFrontendFatalDiagnostics({
      source: "error-boundary",
      error: new Error("boom"),
      route: "/ranking",
    });

    expect(result.success).toBe(true);
    expect(result.bundleId).toMatch(/^fatal-/);
    expect(mocks.captureWindowBlob).toHaveBeenCalledTimes(1);
    expect(mocks.saveBlobToPerfDiagnostics).toHaveBeenCalledTimes(1);
    expect(mocks.exportPerfDiagnostics).toHaveBeenCalledTimes(1);
    expect(mocks.updatePerfDiagnosticsSnapshot).toHaveBeenCalledWith(
      expect.objectContaining({
        currentRoute: "/ranking",
      })
    );
    expect(mocks.recordPerfEvent).toHaveBeenCalledWith(
      "frontend_fatal_diagnostics_complete",
      expect.objectContaining({
        route: "/ranking",
        screenshotSaved: true,
      })
    );
  });

  it("dedupes repeated fatal errors with the same signature", async () => {
    const blob = new Blob(["fatal"], { type: "image/png" });
    mocks.captureWindowBlob.mockResolvedValue({
      success: true,
      blob,
      filename: "MeeMee_Fatal_root_20260422_120000.png",
    });
    mocks.saveBlobToPerfDiagnostics.mockResolvedValue({
      success: true,
      savedPath: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics/frontend-fatal-2.png",
      savedDir: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics",
      fileName: "frontend-fatal-2.png",
    });
    mocks.exportPerfDiagnostics.mockResolvedValue({ success: true, writtenFiles: ["fatal.json"] });

    const first = await fatalDiagnostics.reportFrontendFatalDiagnostics({
      source: "error-boundary",
      error: new Error("boom"),
      route: "/ranking",
    });
    const second = await fatalDiagnostics.reportFrontendFatalDiagnostics({
      source: "error-boundary",
      error: new Error("boom"),
      route: "/ranking",
    });

    expect(first.success).toBe(true);
    expect(second.deduped).toBe(true);
    expect(mocks.captureWindowBlob).toHaveBeenCalledTimes(1);
    expect(mocks.exportPerfDiagnostics).toHaveBeenCalledTimes(1);
  });

  it("still exports diagnostics when screenshot capture fails", async () => {
    mocks.captureWindowBlob.mockResolvedValue({
      success: false,
      error: "canvas failed",
    });
    mocks.exportPerfDiagnostics.mockResolvedValue({ success: true, writtenFiles: ["fatal.json"] });

    const result = await fatalDiagnostics.reportFrontendFatalDiagnostics({
      source: "unhandledrejection",
      message: "promise rejected",
      route: "/",
    });

    expect(result.success).toBe(true);
    expect(result.summary?.screenshot?.success).toBe(false);
    expect(result.summary?.screenshot?.error).toBe("canvas failed");
    expect(mocks.saveBlobToPerfDiagnostics).not.toHaveBeenCalled();
    expect(mocks.exportPerfDiagnostics).toHaveBeenCalledTimes(1);
  });

  it("flushes queued boot diagnostics when the bridge installs", async () => {
    const blob = new Blob(["fatal"], { type: "image/png" });
    mocks.captureWindowBlob.mockResolvedValue({
      success: true,
      blob,
      filename: "MeeMee_Fatal_root_20260422_120000.png",
    });
    mocks.saveBlobToPerfDiagnostics.mockResolvedValue({
      success: true,
      savedPath: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics/frontend-fatal-3.png",
      savedDir: "C:/Users/enish/AppData/Local/MeeMeeScreener/logs/perf-diagnostics",
      fileName: "frontend-fatal-3.png",
    });
    mocks.exportPerfDiagnostics.mockResolvedValue({ success: true, writtenFiles: ["fatal.json"] });

    (window as Window & Record<string, unknown>).__meemeeFatalDiagnosticsQueue = [
      {
        source: "boot-window-error",
        message: "bundle exploded",
        route: "/ranking",
        href: window.location.href,
      },
    ];

    fatalDiagnostics.installFrontendFatalDiagnosticsBridge();
    await Promise.resolve();
    await Promise.resolve();

    expect(mocks.captureWindowBlob).toHaveBeenCalledTimes(1);
    expect(mocks.exportPerfDiagnostics).toHaveBeenCalledTimes(1);
  });
});
