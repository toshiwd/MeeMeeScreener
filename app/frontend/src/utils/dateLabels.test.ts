import { describe, expect, it } from "vitest";
import {
  formatCompactDateLabel,
  formatDateTimeLabel,
  formatIsoDateLabel,
  formatLedgerDateLabel,
  formatLongDateLabel,
} from "./dateLabels";

describe("dateLabels", () => {
  it("formats compact chart dates with weekday", () => {
    expect(formatCompactDateLabel(20260319)).toBe("26/03/19 (木)");
  });

  it("formats long detail dates with weekday", () => {
    expect(formatLongDateLabel(20260319)).toBe("2026/03/19 (木)");
    expect(formatIsoDateLabel("2026-03-19")).toBe("2026/03/19 (木)");
  });

  it("formats ledger dates with weekday", () => {
    expect(formatLedgerDateLabel("2026-03-18")).toBe("2026-03-18 (水)");
  });

  it("formats date time labels with weekday", () => {
    expect(formatDateTimeLabel("2026-03-19T00:05:06+09:00")).toBe("2026/03/19 (木) 00:05:06");
  });
});
