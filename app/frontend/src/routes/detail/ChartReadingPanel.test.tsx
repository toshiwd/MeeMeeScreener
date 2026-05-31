// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import ChartReadingPanel from "./ChartReadingPanel";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

describe("ChartReadingPanel", () => {
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

  const renderPanel = (overrides = {}) => {
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    act(() => {
      root?.render(
        <ChartReadingPanel
          timeframe="daily"
          targetType="bar"
          commentType="review"
          noteText=""
          tagsText=""
          selectedDrawing={null}
          selectedAnnotation={null}
          selectedBar={null}
          noteDate="2026-05-27"
          onTimeframeChange={vi.fn()}
          onTargetTypeChange={vi.fn()}
          onCommentTypeChange={vi.fn()}
          onNoteTextChange={vi.fn()}
          onTagsTextChange={vi.fn()}
          onClearNote={vi.fn()}
          onAnnotateDrawing={vi.fn()}
          onAnnotationChange={vi.fn()}
          onAnnotationDelete={vi.fn()}
          {...overrides}
        />
      );
    });
    return container;
  };

  it("renders timeframe, target, and comment type controls", () => {
    const view = renderPanel();
    expect(view.querySelector("[data-testid='chart-reading-panel']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-reading-timeframe']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-reading-target-type']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-reading-comment-type']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-reading-note-date']")?.textContent).toContain("2026-05-27");
  });

  it("allows annotating selected box or line drawings", () => {
    const onAnnotateDrawing = vi.fn();
    const view = renderPanel({
      selectedDrawing: { kind: "drawBox", startTime: 1, endTime: 2, topPrice: 120, bottomPrice: 100 },
      onAnnotateDrawing,
    });
    const button = view.querySelector<HTMLButtonElement>("[data-testid='chart-reading-annotate-drawing']");
    expect(button?.disabled).toBe(false);
    act(() => {
      button?.click();
    });
    expect(onAnnotateDrawing).toHaveBeenCalledTimes(1);
  });

  it("uses autosave status and exposes clear action instead of save", () => {
    const onClearNote = vi.fn();
    const view = renderPanel({ onClearNote });
    expect(view.querySelector("[data-testid='chart-reading-autosave-status']")?.textContent).toContain("自動保存");
    expect(view.textContent).not.toContain("メモを保存");
    const button = view.querySelector<HTMLButtonElement>("[data-testid='chart-reading-clear']");
    expect(button?.textContent).toContain("クリア");
    act(() => {
      button?.click();
    });
    expect(onClearNote).toHaveBeenCalledTimes(1);
  });

  it("shows the selected daily candle when the target is a bar", () => {
    const view = renderPanel({
      selectedBar: {
        date: "2026-05-27",
        time: 20260527,
        open: 2097,
        high: 2175,
        low: 2079,
        close: 2174,
        volume: 364,
      },
    });
    expect(view.querySelector("[data-testid='chart-reading-selected-bar']")?.textContent).toContain("2026-05-27");
    expect(view.querySelector("[data-testid='chart-reading-selected-bar']")?.textContent).toContain("C 2,174");
  });
});
