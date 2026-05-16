import { vi } from "vitest";

export type CanvasContextStub = {
  clearRect: ReturnType<typeof vi.fn>;
  beginPath: ReturnType<typeof vi.fn>;
  moveTo: ReturnType<typeof vi.fn>;
  lineTo: ReturnType<typeof vi.fn>;
  closePath: ReturnType<typeof vi.fn>;
  arc: ReturnType<typeof vi.fn>;
  stroke: ReturnType<typeof vi.fn>;
  fill: ReturnType<typeof vi.fn>;
  fillRect: ReturnType<typeof vi.fn>;
  strokeRect: ReturnType<typeof vi.fn>;
  fillText: ReturnType<typeof vi.fn>;
  setLineDash: ReturnType<typeof vi.fn>;
  rect: ReturnType<typeof vi.fn>;
  clip: ReturnType<typeof vi.fn>;
  save: ReturnType<typeof vi.fn>;
  restore: ReturnType<typeof vi.fn>;
  setTransform: ReturnType<typeof vi.fn>;
  lineWidth: number;
  font: string;
  textAlign: CanvasTextAlign;
  textBaseline: CanvasTextBaseline;
  strokeStyle: string;
  fillStyle: string;
};

export type CanvasMockHandle = {
  ctx: CanvasContextStub;
  getContextSpy: ReturnType<typeof vi.spyOn>;
  toDataURLSpy: ReturnType<typeof vi.spyOn>;
  restore: () => void;
};

export function createCanvasContextStub(): CanvasContextStub {
  return {
    clearRect: vi.fn(),
    beginPath: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    closePath: vi.fn(),
    arc: vi.fn(),
    stroke: vi.fn(),
    fill: vi.fn(),
    fillRect: vi.fn(),
    strokeRect: vi.fn(),
    fillText: vi.fn(),
    setLineDash: vi.fn(),
    rect: vi.fn(),
    clip: vi.fn(),
    save: vi.fn(),
    restore: vi.fn(),
    setTransform: vi.fn(),
    lineWidth: 1,
    font: "",
    textAlign: "left",
    textBaseline: "alphabetic",
    strokeStyle: "#000",
    fillStyle: "#000"
  };
}

export function installCanvasMock(): CanvasMockHandle {
  const ctx = createCanvasContextStub();
  const getContextSpy = vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(() => ctx as never);
  const toDataURLSpy = vi.spyOn(HTMLCanvasElement.prototype, "toDataURL").mockImplementation(function (this: HTMLCanvasElement) {
    return `data:image/png;base64,${this.width}x${this.height}`;
  });
  let restored = false;

  return {
    ctx,
    getContextSpy,
    toDataURLSpy,
    restore: () => {
      if (restored) return;
      restored = true;
      getContextSpy.mockRestore();
      toDataURLSpy.mockRestore();
    }
  };
}
