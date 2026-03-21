import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import TopNav from "../components/TopNav";
import PublishOpsView from "./PublishOpsView";
import { shouldShowOperatorConsole } from "../utils/operatorConsole";

describe("operator console hardening", () => {
  it("keeps the operator nav gate explicit", () => {
    expect(shouldShowOperatorConsole("0")).toBe(false);
    expect(shouldShowOperatorConsole("1")).toBe(true);
  });

  it("renders the operator console shell without expanding heavy detail blocks", () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter>
        <PublishOpsView />
      </MemoryRouter>
    );

    expect(markup).toContain("Operator Console");
    expect(markup).toContain("Mutation observability");
    expect(markup).toContain("last_reason");
    expect(markup).toContain("operator_mutation_busy_count");
    expect(markup).toContain("Candidate bundles");
    expect(markup).toContain("Selected candidate detail");
    expect(markup).not.toContain("<pre>");
  });

  it("does not expose the ops nav item in MeeMee TopNav", () => {
    const markup = renderToStaticMarkup(
      <MemoryRouter>
        <TopNav />
      </MemoryRouter>
    );

    expect(markup).not.toContain("/ops/publish");
    expect(markup).not.toContain("研究");
    expect(markup).not.toContain("運用");
    expect(markup).toContain("/candidates");
  });
});
