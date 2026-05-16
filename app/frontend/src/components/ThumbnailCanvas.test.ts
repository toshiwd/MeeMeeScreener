import { describe, expect, it, vi } from "vitest";
import { drawChart } from "./ThumbnailCanvas";
import type { BarsPayload, Box, MaSetting } from "../storeTypes";

type CanvasContextStub = {
  clearRect: ReturnType<typeof vi.fn>;
  beginPath: ReturnType<typeof vi.fn>;
  moveTo: ReturnType<typeof vi.fn>;
  lineTo: ReturnType<typeof vi.fn>;
  stroke: ReturnType<typeof vi.fn>;
  fillRect: ReturnType<typeof vi.fn>;
  strokeRect: ReturnType<typeof vi.fn>;
  fillText: ReturnType<typeof vi.fn>;
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

const createCanvasStub = () => {
  const calls: Array<{ x: number; y: number; w: number; h: number }> = [];
  const ctx: CanvasContextStub = {
    clearRect: vi.fn(),
    beginPath: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    stroke: vi.fn(),
    fillRect: vi.fn((x: number, y: number, w: number, h: number) => {
      calls.push({ x, y, w, h });
    }),
    strokeRect: vi.fn(),
    fillText: vi.fn(),
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

  const canvas = {
    getContext: vi.fn(() => ctx),
    style: {},
    width: 0,
    height: 0
  } as unknown as HTMLCanvasElement;

  return { canvas, ctx, calls };
};

describe("drawChart", () => {
  it("draws volume bars from the sixth bar column in a fixed lower band", () => {
    const payload: BarsPayload = {
      bars: [
        [20260318, 100, 102, 99, 101, 10],
        [20260319, 101, 112, 100, 110, 100]
      ],
      ma: { ma7: [], ma20: [], ma60: [] },
      provenance: null
    };
    const boxes: Box[] = [];
    const maSettings: MaSetting[] = [];
    const { canvas, calls } = createCanvasStub();

    drawChart(canvas, payload, boxes, false, maSettings, 200, 120, 2, false, "dark");

    const volumeBars = calls.filter((call) => call.y > 90);
    expect(volumeBars).toHaveLength(2);
    expect(volumeBars[0].h).toBeLessThan(volumeBars[1].h);
    expect(volumeBars[0].y).toBeGreaterThanOrEqual(90);
    expect(volumeBars[1].y).toBeGreaterThanOrEqual(90);
  });

  it("draws boxes with the detected monthly range", () => {
    const payload: BarsPayload = {
      bars: [
        [10, 100, 105, 95, 101, 10],
        [20, 101, 110, 97, 104, 10],
        [30, 104, 111, 100, 106, 10]
      ],
      ma: { ma7: [], ma20: [], ma60: [] },
      provenance: null
    };
    const boxes: Box[] = [
      {
        startIndex: 0,
        endIndex: 1,
        startTime: 0,
        endTime: 20,
        lower: 90,
        upper: 120,
        breakout: null
      }
    ];
    const maSettings: MaSetting[] = [];
    const { canvas, ctx } = createCanvasStub();

    drawChart(canvas, payload, boxes, true, maSettings, 240, 160, 3, false, "dark");

    expect(ctx.strokeRect).toHaveBeenCalledTimes(1);
    const [x, , width] = ctx.strokeRect.mock.calls[0];
    expect(x).toBe(0);
    expect(width).toBeGreaterThan(100);
    expect(ctx.lineWidth).toBe(2);
  });
});
