// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import DetailDrawToolbar from "./DetailDrawToolbar";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("DetailDrawToolbar", () => {
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;

  afterEach(() => {
    if (root) {
      act(() => root?.unmount());
    }
    root = null;
    container?.remove();
    container = null;
  });

  const renderToolbar = (overrides: Partial<React.ComponentProps<typeof DetailDrawToolbar>> = {}) => {
    const props: React.ComponentProps<typeof DetailDrawToolbar> = {
      activeTool: null,
      activeDrawColor: "#64748b",
      activeLineOpacity: 0.8,
      activeLineWidth: 2,
      continuousDraw: true,
      onSelectTool: vi.fn(),
      onResetAll: vi.fn(),
      onToggleContinuous: vi.fn(),
      onCycleColor: vi.fn(),
      onLineOpacityChange: vi.fn(),
      onLineWidthChange: vi.fn(),
      ...overrides
    };
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    act(() => root?.render(<DetailDrawToolbar {...props} />));
    return { props, view: container };
  };

  it("renders MOOMOO-style selection, drawing, continuous, and delete controls", () => {
    const { props, view } = renderToolbar();

    expect(view.querySelector("[data-testid='detail-draw-toolbar']")).not.toBeNull();
    expect(view.querySelector("[aria-label='選択 / 描画編集']")).not.toBeNull();
    expect(view.querySelector("[aria-label='ローソク足カウント']")).not.toBeNull();
    expect(view.querySelector("[aria-label='BOX']")).not.toBeNull();
    expect(view.querySelector("[aria-label='価格帯']")).not.toBeNull();
    expect(view.querySelector("[aria-label='水平ライン']")).not.toBeNull();
    expect(view.querySelector("[aria-label='連続描画 ON']")).not.toBeNull();
    expect(view.querySelector("[aria-label='描画を全削除']")).not.toBeNull();

    act(() => view.querySelector<HTMLButtonElement>("[aria-label='選択 / 描画編集']")?.click());
    expect(props.onSelectTool).toHaveBeenCalledWith(null);

    act(() => view.querySelector<HTMLButtonElement>("[aria-label='連続描画 ON']")?.click());
    expect(props.onToggleContinuous).toHaveBeenCalledTimes(1);
  });

  it("shows style controls only while a drawing tool is active", () => {
    const initial = renderToolbar();
    expect(initial.view.querySelector("[aria-label='描画スタイル']")).toBeNull();

    act(() => {
      root?.render(<DetailDrawToolbar {...initial.props} activeTool="drawBox" />);
    });

    expect(initial.view.querySelector("[aria-label='描画スタイル']")).not.toBeNull();
    expect(initial.view.querySelector("[aria-label='透明度']")).not.toBeNull();
    expect(initial.view.querySelector("[aria-label='太さ']")).not.toBeNull();
  });
});
