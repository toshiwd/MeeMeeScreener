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
    await renderClient(
      <ErrorBoundary>
        <ThrowingChild />
      </ErrorBoundary>
    );

    expect(mocks.reportFrontendFatalDiagnostics).toHaveBeenCalledTimes(1);
    expect(mocks.reportFrontendFatalDiagnostics).toHaveBeenCalledWith(
      expect.objectContaining({
        source: "error-boundary",
      })
    );
  });
});
