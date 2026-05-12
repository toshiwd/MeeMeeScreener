// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderClient } from "../test/renderClient";

const mocks = vi.hoisted(() => ({
  reportFrontendFatalDiagnostics: vi.fn(),
}));

vi.mock("../utils/fatalDiagnostics", () => ({
  reportFrontendFatalDiagnostics: mocks.reportFrontendFatalDiagnostics,
}));

function ThrowingChild() {
  throw new Error("boom");
}

describe("ErrorBoundary", () => {
  beforeEach(() => {
    mocks.reportFrontendFatalDiagnostics.mockReset();
  });

  it("reports fatal render failures to the diagnostics bridge", async () => {
    const ErrorBoundary = (await import("./ErrorBoundary")).default;
    const render = await renderClient(
      <ErrorBoundary>
        <ThrowingChild />
      </ErrorBoundary>
    );

    expect(render.container.textContent).toContain("画面の表示に問題が発生しました");
    expect(render.container.textContent).not.toContain("boom");
    expect(render.container.textContent).not.toContain("componentStack");
    expect(mocks.reportFrontendFatalDiagnostics).toHaveBeenCalledTimes(1);
    expect(mocks.reportFrontendFatalDiagnostics).toHaveBeenCalledWith(
      expect.objectContaining({
        source: "error-boundary",
      })
    );
  });
});
