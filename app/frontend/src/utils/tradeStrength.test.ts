import { describe, expect, it } from "vitest";
import {
  formatTradeStrengthCaption,
  formatTradeStrengthLabel,
  formatTradeStrengthPoints,
  resolveTradeStrengthBand,
  tradeStrengthToneClass,
} from "./tradeStrength";

describe("tradeStrength", () => {
  it("maps scores to strength bands and point labels", () => {
    expect(resolveTradeStrengthBand(0.9)).toBe("strong");
    expect(resolveTradeStrengthBand(0.7)).toBe("medium");
    expect(resolveTradeStrengthBand(0.4)).toBe("weak");
    expect(formatTradeStrengthLabel(0.9)).toBe("強");
    expect(formatTradeStrengthLabel(0.7)).toBe("中");
    expect(formatTradeStrengthLabel(0.4)).toBe("弱");
    expect(formatTradeStrengthPoints(0.956)).toBe("96点");
    expect(formatTradeStrengthCaption("買い", 0.956)).toBe("買い 強 96点");
    expect(tradeStrengthToneClass(0.9)).toBe("is-ok");
    expect(tradeStrengthToneClass(0.4)).toBe("is-warn");
  });
});
