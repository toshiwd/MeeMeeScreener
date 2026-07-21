import { describe, expect, it } from "vitest";
import { forecastReadinessLabel, formatForecastPercent } from "./ForecastView";

describe("ForecastView helpers", () => {
  it("formats model ratios as user-facing percentages", () => {
    expect(formatForecastPercent(0.634)).toBe("63.4%");
    expect(formatForecastPercent(0.021, true)).toBe("+2.1%");
    expect(formatForecastPercent(null)).toBe("--");
  });
  it("keeps an unready forecast explicitly display-only", () => {
    expect(forecastReadinessLabel(null)).toBe("評価データなし");
    expect(forecastReadinessLabel({ readiness_pass: false })).toBe("表示のみ・判断未採用");
    expect(forecastReadinessLabel({ readiness_pass: true })).toBe("判断利用可");
  });
});
