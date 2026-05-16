// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { captureWindowBlob } from "./windowScreenshot";

const mocks = vi.hoisted(() => ({
  html2canvas: vi.fn(),
}));

vi.mock("html2canvas", () => ({
  default: mocks.html2canvas,
}));

describe("windowScreenshot", () => {
  let originalGetComputedStyle: typeof window.getComputedStyle;

  beforeEach(() => {
    document.body.innerHTML = '<div id="root"><canvas></canvas></div>';
    originalGetComputedStyle = window.getComputedStyle.bind(window);
    mocks.html2canvas.mockReset();
    vi.spyOn(HTMLCanvasElement.prototype, "toDataURL").mockReturnValue(
      "data:image/png;base64,canvas-snapshot"
    );
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      callback(0);
      return 0;
    });
    vi.stubGlobal("cancelAnimationFrame", () => undefined);
    vi.spyOn(window, "getComputedStyle").mockImplementation((element: Element) => {
      if (element === document.documentElement) {
        return {
          backgroundImage: "none",
          backgroundColor: "color(srgb 0.95 0.94 0.93)",
          getPropertyValue: (name: string) => (name === "--bg-app" ? "#f7f3eb" : ""),
        } as unknown as CSSStyleDeclaration;
      }

      if (element === document.body || (element instanceof HTMLElement && element.id === "root")) {
        return {
          backgroundImage: "none",
          backgroundColor: "color(srgb 0.95 0.94 0.93)",
          getPropertyValue: (name: string) => (name === "--bg-app" ? "#f7f3eb" : ""),
        } as unknown as CSSStyleDeclaration;
      }

      return originalGetComputedStyle(element);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("normalizes the screenshot background color and enables foreign object rendering", async () => {
    const canvas = document.createElement("canvas");
    Object.defineProperty(canvas, "toBlob", {
      configurable: true,
      value: (callback: BlobCallback) => callback(new Blob(["png"], { type: "image/png" })),
    });
    mocks.html2canvas.mockResolvedValue(canvas);

    const result = await captureWindowBlob({ screenType: "Detail", code: "7203" });

    expect(result.success).toBe(true);
    expect(result.blob).toBeInstanceOf(Blob);
    expect(mocks.html2canvas).toHaveBeenCalledTimes(1);
    expect(mocks.html2canvas).toHaveBeenCalledWith(
      expect.any(HTMLElement),
      expect.objectContaining({
        backgroundColor: "#f7f3eb",
        foreignObjectRendering: true,
      })
    );
  });

  it("renders chart canvas snapshots into the html2canvas clone", async () => {
    const sourceCanvas = document.querySelector("canvas") as HTMLCanvasElement;
    sourceCanvas.width = 320;
    sourceCanvas.height = 180;
    sourceCanvas.style.width = "320px";
    sourceCanvas.style.height = "180px";
    Object.defineProperty(sourceCanvas, "toDataURL", {
      configurable: true,
      value: () => "data:image/png;base64,chart-snapshot",
    });
    const outputCanvas = document.createElement("canvas");
    Object.defineProperty(outputCanvas, "toBlob", {
      configurable: true,
      value: (callback: BlobCallback) => callback(new Blob(["png"], { type: "image/png" })),
    });
    mocks.html2canvas.mockImplementation(async (_element: HTMLElement, options: any) => {
      const clonedDocument = document.implementation.createHTMLDocument("clone");
      clonedDocument.body.innerHTML = document.body.innerHTML;
      options.onclone(clonedDocument);
      const image = clonedDocument.querySelector("img");
      expect(image?.getAttribute("src")).toBe("data:image/png;base64,chart-snapshot");
      expect(image?.getAttribute("width")).toBe("320");
      expect(image?.getAttribute("height")).toBe("180");
      return outputCanvas;
    });

    const result = await captureWindowBlob({ screenType: "Detail", code: "7203" });

    expect(result.success).toBe(true);
    expect(sourceCanvas.hasAttribute("data-meemee-screenshot-canvas-id")).toBe(false);
  });
});
