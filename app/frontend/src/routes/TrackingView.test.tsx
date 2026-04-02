// @vitest-environment jsdom
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import TrackingView from "./TrackingView";

describe("TrackingView", () => {
  it("renders the tracking shell with ranking default mode", () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/ranking/tracking"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(markup).toContain("Ranking / Tracking");
    expect(markup).toContain("解析");
    expect(markup).toContain("Active");
    expect(markup).toContain("Completed");
    expect(markup).toContain("Archive");
    expect(markup).toContain("rank bucket");
    expect(markup).not.toContain("tracking-drawer-backdrop");
  });

  it("accepts signal query params for initial filtering", () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/ranking/tracking?view=signal&q=4444&side=sell&logic_version=logic:test:v2"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(markup).toContain('value="4444"');
    expect(markup).toContain("売り");
    expect(markup).toContain("logic version");
  });

  it("renders the analysis shell when analysis mode is selected", () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter initialEntries={["/ranking/tracking?view=analysis"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(markup).toContain("解析");
    expect(markup).toContain("summary");
    expect(markup).toContain("rolling");
    expect(markup).toContain("sell compare");
    expect(markup).toContain("sell subsets");
    expect(markup).toContain("timing pattern");
    expect(markup).toContain("peak day");
    expect(markup).toContain("regime");
    expect(markup).toContain("failure");
    expect(markup).toContain("buy profit peak median");
    expect(markup).toContain("sell adverse peak median");
  });
});
