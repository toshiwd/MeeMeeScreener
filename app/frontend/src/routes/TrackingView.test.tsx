// @vitest-environment jsdom
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";
import { renderClient, type RenderClientHandle } from "../test/renderClient";
import TrackingView from "./TrackingView";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("TrackingView", () => {
  let render: RenderClientHandle | null = null;

  afterEach(() => {
    render?.cleanup();
    render = null;
  });

  it("renders the tracking shell with ranking default mode", async () => {
    render = await renderClient(
      <MemoryRouter initialEntries={["/ranking/tracking"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(render.html()).toContain("Ranking / Tracking");
    expect(render.html()).toContain("tracking-page-title");
    expect(render.html()).toContain("Active");
    expect(render.html()).toContain("Completed");
    expect(render.html()).toContain("Archive");
    expect(render.html()).toContain("rank bucket");
    expect(render.html()).toContain("結果");
    expect(render.html()).toContain("並び順");
    expect(render.html()).toContain(">2Y<");
    expect(render.html()).not.toContain("tracking-drawer-backdrop");
  });

  it("accepts signal query params for initial filtering", async () => {
    render = await renderClient(
      <MemoryRouter initialEntries={["/ranking/tracking?view=signal&q=4444&side=sell&logic_version=logic:test:v2&outcome=bad&sort=worst"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(render.html()).toContain('value="4444"');
    expect(render.html()).toContain("コード / 銘柄名");
    expect(render.html()).toContain("logic version");
    expect(render.html()).toContain('value="bad"');
    expect(render.html()).toContain('value="worst"');
  });

  it("renders the analysis shell when analysis mode is selected", async () => {
    render = await renderClient(
      <MemoryRouter initialEntries={["/ranking/tracking?view=analysis"]}>
        <TrackingView />
      </MemoryRouter>
    );

    expect(render.html()).toContain("summary");
    expect(render.html()).toContain("rolling");
    expect(render.html()).toContain("sell compare");
    expect(render.html()).toContain("sell subsets");
    expect(render.html()).toContain("timing pattern");
    expect(render.html()).toContain("peak day");
    expect(render.html()).toContain("regime");
    expect(render.html()).toContain("failure");
    expect(render.html()).toContain("buy profit peak median");
    expect(render.html()).toContain("sell adverse peak median");
  });
});
