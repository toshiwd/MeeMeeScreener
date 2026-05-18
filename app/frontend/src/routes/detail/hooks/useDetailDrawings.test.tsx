import { useEffect } from "react";
import { act } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { renderClient } from "../../../test/renderClient";
import { useDetailDrawings } from "./useDetailDrawings";

function Probe({
  code = "7203",
  compareCode = null,
  onSnapshot
}: {
  code?: string | null;
  compareCode?: string | null;
  onSnapshot: (value: ReturnType<typeof useDetailDrawings>) => void;
}) {
  const drawings = useDetailDrawings({ code, compareCode });
  useEffect(() => {
    onSnapshot(drawings);
  });
  return null;
}

const emptyDrawings = {
  timeZones: [],
  priceBands: [],
  drawBoxes: [],
  horizontalLines: []
};

describe("useDetailDrawings", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it("shares hand-drawn boxes between daily and monthly charts", async () => {
    let latest: ReturnType<typeof useDetailDrawings> | null = null;
    const render = await renderClient(
      <Probe onSnapshot={(value) => {
        latest = value;
      }} />
    );

    const box = {
      startTime: 1711929600,
      endTime: 1714521600,
      topPrice: 1200,
      bottomPrice: 980,
      color: "#64748b",
      opacity: 0.12,
      lineWidth: 2
    };

    await act(async () => {
      if (!latest) throw new Error("drawings hook did not render");
      latest.addDrawBox(latest.dailyDrawingKey)(box);
      await Promise.resolve();
    });

    expect(latest?.dailyDrawings.drawBoxes).toEqual([box]);
    expect(latest?.monthlyDrawings.drawBoxes).toEqual([box]);
    expect(latest?.weeklyDrawings.drawBoxes).toEqual([]);

    render.cleanup();
  });

  it("migrates existing daily and monthly boxes into the linked box store", async () => {
    const dailyBox = {
      startTime: 1711929600,
      endTime: 1712016000,
      topPrice: 1100,
      bottomPrice: 1000
    };
    const monthlyBox = {
      startTime: 1704067200,
      endTime: 1711929600,
      topPrice: 1300,
      bottomPrice: 900,
      color: "#0ea5e9"
    };
    window.localStorage.setItem(
      "drawings:v1:7203:daily",
      JSON.stringify({ ...emptyDrawings, drawBoxes: [dailyBox] })
    );
    window.localStorage.setItem(
      "drawings:v1:7203:monthly",
      JSON.stringify({ ...emptyDrawings, drawBoxes: [monthlyBox] })
    );

    let latest: ReturnType<typeof useDetailDrawings> | null = null;
    const render = await renderClient(
      <Probe onSnapshot={(value) => {
        latest = value;
      }} />
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(latest?.dailyDrawings.drawBoxes).toEqual([dailyBox, monthlyBox]);
    expect(latest?.monthlyDrawings.drawBoxes).toEqual([dailyBox, monthlyBox]);
    expect(
      JSON.parse(window.localStorage.getItem("drawings:v1:7203:daily-monthly-boxes") ?? "{}")
        .drawBoxes
    ).toEqual([dailyBox, monthlyBox]);

    render.cleanup();
  });
});
