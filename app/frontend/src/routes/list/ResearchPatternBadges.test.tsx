import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { ResearchPatternBadges } from "./ResearchPatternBadges";

const formatPct = (value?: number | null) => {
  if (!Number.isFinite(value ?? NaN)) return "--";
  return `${((value ?? 0) * 100).toFixed(2)}%`;
};

describe("ResearchPatternBadges", () => {
  it("renders rebound tag and bonus badge without changing setup semantics", () => {
    const markup = renderToStaticMarkup(
      <ResearchPatternBadges
        researchPatternTag="rebound_onset"
        researchPriorBonus={0.015}
        formatPct={formatPct}
      />,
    );

    expect(markup).toContain("反発初動候補");
    expect(markup).toContain("補正 1.50%");
  });

  it("renders nothing when neither tag nor bonus exists", () => {
    const markup = renderToStaticMarkup(
      <ResearchPatternBadges
        researchPatternTag={null}
        researchPriorBonus={0}
        formatPct={formatPct}
      />,
    );

    expect(markup).toBe("");
  });

  it("can coexist with the existing setup label without replacing it", () => {
    const markup = renderToStaticMarkup(
      <div>
        <span>setup-label</span>
        <ResearchPatternBadges
          researchPatternTag="rebound_onset"
          researchPriorBonus={0.015}
          formatPct={formatPct}
        />
      </div>,
    );

    expect(markup).toContain("setup-label");
    expect(markup).toContain("event-badge event-earnings");
    expect(markup).toContain("1.50%");
  });
});
