// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import ProductStateNotice from "./ProductStateNotice";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("ProductStateNotice", () => {
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

  it("marks the product state explicitly for tests and styling", () => {
    const view = render(
      <ProductStateNotice kind="missing" prefix="Daily" className="detail-chart-empty">
        Chart data unavailable
      </ProductStateNotice>
    );

    const notice = view.querySelector("[data-product-state='missing']");
    expect(notice).not.toBeNull();
    expect(notice?.className).toContain("detail-chart-empty");
    expect(notice?.textContent).toContain("Daily: Chart data unavailable");
  });

  it("keeps db busy distinct from generic error", () => {
    const view = render(
      <ProductStateNotice kind="db_busy">database is temporarily busy</ProductStateNotice>
    );

    expect(view.querySelector("[data-product-state='db_busy']")).not.toBeNull();
    expect(view.querySelector("[data-product-state='error']")).toBeNull();
  });
});
