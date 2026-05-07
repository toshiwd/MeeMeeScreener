// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import DataFreshnessBadges from "./DataFreshnessBadges";
import type { MeeMeeDataFreshnessContract } from "../dataFreshnessContract";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("DataFreshnessBadges", () => {
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;

  afterEach(() => {
    if (root) {
      act(() => {
        root?.unmount();
      });
    }
    root = null;
    container?.remove();
    container = null;
  });

  const render = (node: React.ReactNode) => {
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    act(() => {
      root?.render(node);
    });
    return container;
  };

  it("renders mixed chart data as mixed, not confirmed", () => {
    const contract = {
      contract_version: "meemee_data_freshness_v1",
      charts: {
        daily: {
          classification: "mixed",
          freshness_state: "stale",
          status: "ready",
          right_edge_date: "2026-04-16",
        },
      },
    } as unknown as MeeMeeDataFreshnessContract;

    const view = render(<DataFreshnessBadges contract={contract} scope="chart" timeframe="daily" />);

    expect(view.textContent).toContain("mixed / partly provisional");
    expect(view.textContent).toContain("stale");
    expect(view.textContent).not.toContain("confirmed");
  });

  it("renders absent contracts as unavailable/missing without crashing", () => {
    const view = render(<DataFreshnessBadges contract={null} scope="ranking" />);

    expect(view.textContent).toContain("missing");
  });

  it("blocks research-only from normal product labeling", () => {
    const contract = {
      detail: {
        classification: "research-only",
        status: "ready",
      },
    } as unknown as MeeMeeDataFreshnessContract;

    const view = render(<DataFreshnessBadges contract={contract} scope="detail" />);

    expect(view.textContent).toContain("internal source blocked");
  });
});
