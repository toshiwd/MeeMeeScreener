// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import AnnotationPanel from "./AnnotationPanel";
import type { ChartAnnotation } from "./annotations";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const baseAnnotation = (objectType: ChartAnnotation["object_type"]): ChartAnnotation => ({
  id: `ann-${objectType}`,
  code: "1001",
  as_of_date: "2026-03-31",
  timeframe: "daily",
  object_type: objectType,
  payload: {
    free_text: "sample",
    importance: 3,
    no_lookahead: true,
  },
  tags: ["support"],
  no_lookahead: true,
});

describe("AnnotationPanel", () => {
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

  it("renders bar-specific fields", () => {
    const view = render(
      <AnnotationPanel annotation={baseAnnotation("bar")} onChange={vi.fn()} onDelete={vi.fn()} />
    );
    expect(view.textContent).toContain("ローソク足の役割");
    expect(view.textContent).not.toContain("範囲の種類");
    expect(view.textContent).not.toContain("ラインの種類");
  });

  it("renders region-specific fields", () => {
    const view = render(
      <AnnotationPanel annotation={baseAnnotation("region")} onChange={vi.fn()} onDelete={vi.fn()} />
    );
    expect(view.textContent).toContain("範囲の種類");
    expect(view.textContent).toContain("有効条件");
    expect(view.textContent).toContain("無効条件");
  });

  it("renders line-specific fields", () => {
    const view = render(
      <AnnotationPanel annotation={baseAnnotation("line")} onChange={vi.fn()} onDelete={vi.fn()} />
    );
    expect(view.textContent).toContain("ラインの種類");
    expect(view.textContent).toContain("ブレイク条件");
    expect(view.textContent).toContain("ブレイク時の行動");
  });

  it("renders callout-specific fields", () => {
    const view = render(
      <AnnotationPanel annotation={baseAnnotation("callout")} onChange={vi.fn()} onDelete={vi.fn()} />
    );
    expect(view.textContent).toContain("指している対象");
    expect(view.textContent).toContain("対象の詳細");
    expect(view.textContent).toContain("コメント種別");
    expect(view.textContent).toContain("引き出し線を表示");
  });
});
