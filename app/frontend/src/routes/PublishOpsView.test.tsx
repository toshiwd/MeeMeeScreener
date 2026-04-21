// @vitest-environment jsdom
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";
import TopNav from "../components/TopNav";
import { renderClient, type RenderClientHandle } from "../test/renderClient";
import PublishOpsView from "./PublishOpsView";
import { shouldShowOperatorConsole } from "../utils/operatorConsole";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("operator console hardening", () => {
  let render: RenderClientHandle | null = null;

  afterEach(() => {
    render?.cleanup();
    render = null;
  });

  it("keeps the operator nav gate explicit", () => {
    expect(shouldShowOperatorConsole("0")).toBe(false);
    expect(shouldShowOperatorConsole("1")).toBe(true);
  });

  it("renders the operator console shell without expanding heavy detail blocks", async () => {
    render = await renderClient(
      <MemoryRouter>
        <PublishOpsView />
      </MemoryRouter>
    );

    expect(render.html()).toContain("Operator Console");
    expect(render.html()).toContain("Mutation observability");
    expect(render.html()).toContain("last_reason");
    expect(render.html()).toContain("operator_mutation_busy_count");
    expect(render.html()).toContain("Candidate bundles");
    expect(render.html()).toContain("Selected candidate detail");
    expect(render.html()).toContain("MeeMee-safe runtime surface only");
    expect(render.html()).toContain("Raw registry JSON and comparison artifacts are withheld from MeeMee");
    expect(render.html()).not.toContain("<pre>");
    expect(render.html()).not.toContain("published_logic_manifest");
    expect(render.html()).not.toContain("validation_summary");
    expect(render.html()).not.toContain("published_ranking_snapshot");
    expect(render.html()).not.toContain("publish_registry_state");
  });

  it("does not expose the ops nav item in MeeMee TopNav", async () => {
    render = await renderClient(
      <MemoryRouter>
        <TopNav />
      </MemoryRouter>
    );

    expect(render.html()).not.toContain("/ops/publish");
    expect(render.html()).not.toContain("運用");
    expect(render.html()).not.toContain("公開");
    expect(render.html()).toContain("/positions");
    expect(render.html()).toContain("/candidates");
  });
});
