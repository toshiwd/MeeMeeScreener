// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import PublishOpsRouteGuard from "./PublishOpsRouteGuard";

// React 18 の act 警告対策。
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("PublishOpsRouteGuard", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("redirects to home when the operator console is disabled", async () => {
    vi.stubEnv("VITE_SHOW_OPERATOR_CONSOLE", "0");

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(
        <MemoryRouter initialEntries={["/tradex/legacy/publish"]}>
          <Routes>
            <Route path="/tradex/" element={<div>home</div>} />
            <Route
              path="/tradex/legacy/publish"
              element={
                <PublishOpsRouteGuard>
                  <div>ops</div>
                </PublishOpsRouteGuard>
              }
            />
          </Routes>
        </MemoryRouter>
      );
    });

    expect(container.textContent).toContain("home");
    expect(container.textContent).not.toContain("ops");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
