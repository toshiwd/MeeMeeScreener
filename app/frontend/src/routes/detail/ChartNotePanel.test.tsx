// @vitest-environment jsdom
import React, { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import ChartNotePanel, { type ChartNoteParagraph } from "./ChartNotePanel";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const paragraph: ChartNoteParagraph = {
  paragraph_id: "p1",
  order: 1,
  text: "",
  comment_type: "daily_bar_reading",
  linked_objects: [],
  reason_tags: [],
  no_lookahead: true,
};

describe("ChartNotePanel", () => {
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
        <ChartNotePanel
          title=""
          timeframe="mixed"
          paragraphs={[paragraph]}
          selectedAnnotation={null}
          onTitleChange={vi.fn()}
          onTimeframeChange={vi.fn()}
          onAddParagraph={vi.fn()}
          onParagraphChange={vi.fn()}
          onLinkSelectedAnnotation={vi.fn()}
          onLinkMa20={vi.fn()}
          onSave={vi.fn()}
          {...overrides}
        />
      );
    });
    return container;
  };

  it("renders note mode controls and paragraph editor", () => {
    const view = renderPanel();
    expect(view.querySelector("[data-testid='chart-note-panel']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-note-add-paragraph']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-note-paragraph-text']")).not.toBeNull();
    expect(view.querySelector("[data-testid='chart-note-paragraph-comment-type']")).not.toBeNull();
  });

  it("emits paragraph text edits and MA20 link action", () => {
    const onParagraphChange = vi.fn();
    const onLinkMa20 = vi.fn();
    const view = renderPanel({ onParagraphChange, onLinkMa20 });
    const text = view.querySelector<HTMLTextAreaElement>("[data-testid='chart-note-paragraph-text']");
    act(() => {
      if (text) {
        const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value")?.set;
        setter?.call(text, "20下20で前のボックス下限で下げ止まった");
        text.dispatchEvent(new Event("input", { bubbles: true }));
      }
    });
    expect(onParagraphChange).toHaveBeenCalled();
    act(() => {
      view.querySelector<HTMLButtonElement>("[data-testid='chart-note-link-ma20']")?.click();
    });
    expect(onLinkMa20).toHaveBeenCalledWith("p1");
  });
});
