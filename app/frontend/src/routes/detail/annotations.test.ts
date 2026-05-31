import { describe, expect, it } from "vitest";
import { annotationToCallout, type ChartAnnotation } from "./annotations";

describe("annotationToCallout", () => {
  it("maps structured callout payload to chart overlay coordinates", () => {
    const annotation: ChartAnnotation = {
      id: "callout-1",
      code: "1001",
      as_of_date: "2026-03-31",
      timeframe: "daily",
      object_type: "callout",
      payload: {
        anchor_type: "bar",
        anchor_target: "candle",
        anchor_date: "2026-03-31",
        anchor_price: 51060,
        label_position: { date: "2026-04-08", price: 53500 },
        free_text: "e2e candle callout",
      },
      tags: ["callout"],
      no_lookahead: true,
    };

    expect(annotationToCallout(annotation, true)).toEqual({
      id: "callout-1",
      anchorTime: 20260331,
      anchorPrice: 51060,
      labelTime: 20260408,
      labelPrice: 53500,
      text: "e2e candle callout",
      tags: ["callout"],
      selected: true,
    });
  });

  it("keeps indicator target and selected state renderable through the same overlay shape", () => {
    const annotation: ChartAnnotation = {
      id: "callout-ma20",
      code: "1001",
      as_of_date: "2026-03-31",
      timeframe: "daily",
      object_type: "callout",
      payload: {
        anchor_type: "indicator",
        anchor_target: "ma20",
        anchor_date: "2026-03-31",
        anchor_price: 53760.5,
        label_position: { date: "2026-04-08", price: 53500 },
        free_text: "ma20 callout",
      },
      tags: ["ma20"],
      no_lookahead: true,
    };

    const callout = annotationToCallout(annotation, false);
    expect(callout?.anchorPrice).toBe(53760.5);
    expect(callout?.tags).toEqual(["ma20"]);
  });
});
