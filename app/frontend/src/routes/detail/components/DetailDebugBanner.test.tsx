import { describe, expect, it } from "vitest";
import { renderClient } from "../../../test/renderClient";
import DetailDebugBanner from "./DetailDebugBanner";

describe("DetailDebugBanner", () => {
  it("renders inline in the lower tools area when requested", async () => {
    const render = await renderClient(
      <DetailDebugBanner
        hasIssues={true}
        bannerTone="warning"
        bannerTitle="Debug"
        debugSummary={["issue"]}
        debugOpen={false}
        showInfoDetails={false}
        debugLines={["line-1"]}
        copyFallbackText={null}
        inline={true}
        onToggleOpen={() => undefined}
        onCopy={() => undefined}
        onToggleInfoDetails={() => undefined}
        onClose={() => undefined}
      />
    );

    const banner = render.container.querySelector(".detail-debug-banner");
    expect(banner).not.toBeNull();
    expect(banner?.className).toContain("is-inline");
    expect(render.container.textContent).toContain("Debug");

    render.cleanup();
  });
});
