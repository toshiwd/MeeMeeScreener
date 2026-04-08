// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearPerfDiagnostics,
  getPerfDiagnosticsEvents,
  recordPerfEvent,
} from "./perfDiagnostics";
import { preloadRoute } from "./routePreload";

describe("perfDiagnostics", () => {
  beforeEach(async () => {
    window.localStorage.setItem("meemeePerfDiagnosticsEnabled", "1");
    await clearPerfDiagnostics();
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
});
