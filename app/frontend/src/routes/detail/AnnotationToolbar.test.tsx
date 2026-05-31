// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import AnnotationToolbar from "./AnnotationToolbar";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("AnnotationToolbar", () => {
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

  it("renders annotation mode and exposes v1 tools when enabled", () => {
    const onToggleEnabled = vi.fn();
    const onSelectTool = vi.fn();
    const onFilterChange = vi.fn();
    const view = render(
      <AnnotationToolbar
        enabled
        activeTool="select"
        filter="all"
        onToggleEnabled={onToggleEnabled}
        onSelectTool={onSelectTool}
        onFilterChange={onFilterChange}
      />
    );

    expect(view.querySelector("[data-testid='annotation-toolbar']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈編集モード']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈 対象選択']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈 ローソク足']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈 ボックス範囲']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈 水平ライン']")).not.toBeNull();
    expect(view.querySelector("[aria-label='注釈 引き出しコメント']")).not.toBeNull();

    act(() => {
      view.querySelector<HTMLButtonElement>("[aria-label='注釈 ローソク足']")?.click();
    });
    expect(onSelectTool).toHaveBeenCalledWith("bar");

    act(() => {
      const filter = view.querySelector<HTMLSelectElement>("[aria-label='注釈表示フィルター']");
      filter!.value = "line";
      filter!.dispatchEvent(new Event("change", { bubbles: true }));
    });
    expect(onFilterChange).toHaveBeenCalledWith("line");
  });
});
