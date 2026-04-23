// @vitest-environment jsdom
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { renderClient } from "../test/renderClient";

const mocks = vi.hoisted(() => ({
  apiGet: vi.fn(),
  backendReadyRef: { value: true }
}));

vi.mock("../api", () => ({
  api: {
    get: mocks.apiGet
  }
}));

vi.mock("../backendReady", () => ({
  useBackendReadyState: () => ({
    backendReady: mocks.backendReadyRef.value,
    ready: mocks.backendReadyRef.value,
    backendAlive: mocks.backendReadyRef.value,
    dbBusy: false,
    phase: "ready",
    message: "ready",
    error: null,
    errorDetails: null,
    attemptCount: 0,
    elapsedMs: 0,
    retry: () => undefined
  })
}));

import GlobalTxtUpdateStatus from "./GlobalTxtUpdateStatus";

describe("GlobalTxtUpdateStatus", () => {
  beforeEach(() => {
    mocks.backendReadyRef.value = true;
    mocks.apiGet.mockReset();
    mocks.apiGet.mockImplementation(async (url: string) => {
      if (url === "/jobs/current" || url.startsWith("/jobs/")) {
        return {
          data: {
            id: "job-1",
            type: "txt_update",
            status: "running",
            progress: 89,
            message: "TXT取込"
          }
        };
      }
      return { data: null };
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("hides the badge on detail routes while keeping polling active", async () => {
    const render = await renderClient(
      <MemoryRouter initialEntries={["/detail/9984"]}>
        <Routes>
          <Route
            path="*"
            element={<GlobalTxtUpdateStatus />}
          />
        </Routes>
      </MemoryRouter>
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(render.container.querySelector(".txt-update-meta")).toBeNull();
    expect(mocks.apiGet).toHaveBeenCalledWith("/jobs/current");

    render.cleanup();
  });

  it("still renders the badge outside detail routes", async () => {
    const render = await renderClient(
      <MemoryRouter initialEntries={["/ranking"]}>
        <Routes>
          <Route
            path="*"
            element={<GlobalTxtUpdateStatus />}
          />
        </Routes>
      </MemoryRouter>
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(render.container.querySelector(".txt-update-meta")).not.toBeNull();
    expect(render.container.textContent).toContain("TXT更新");

    render.cleanup();
  });
});
