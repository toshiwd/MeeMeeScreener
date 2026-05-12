import { describe, expect, it } from "vitest";
import {
  buildDataFreshnessBadgeItems,
  normalizeMeeMeeDataFreshnessContract,
  shouldShowResearchOnlyWarning,
} from "./dataFreshnessContract";

describe("data freshness contract parser", () => {
  it("keeps absent contracts backward compatible", () => {
    const contract = normalizeMeeMeeDataFreshnessContract(undefined);

    expect(contract.contract_version).toBe("unknown");
    expect(contract.ranking.classification).toBe("missing");
    expect(contract.ranking.freshness_state).toBe("missing");
    expect(contract.charts.daily.status).toBe("missing");
  });

  it("normalizes confirmed ranking contracts", () => {
    const contract = normalizeMeeMeeDataFreshnessContract({
      contract_version: "meemee_data_freshness_v1",
      ranking: {
        snapshot_as_of: "2026-04-20",
        snapshot_id: "ranking:D:latest:up:trade:balanced:2026-04-20",
        freshness_state: "fresh",
        classification: "confirmed",
        status: "ready",
      },
    });

    expect(contract.ranking.snapshot_as_of).toBe("2026-04-20");
    expect(contract.ranking.classification).toBe("confirmed");
    expect(buildDataFreshnessBadgeItems(contract.ranking).map((item) => item.label)).toContain("最新");
  });

  it("labels provisional and mixed chart data as warning-class display states", () => {
    const contract = normalizeMeeMeeDataFreshnessContract({
      charts: {
        daily: {
          classification: "mixed",
          freshness_state: "stale",
          status: "ready",
          right_edge_date: "2026-04-16",
        },
        weekly: {
          classification: "provisional",
          freshness_state: "fresh",
          status: "ready",
        },
      },
    });

    const dailyBadges = buildDataFreshnessBadgeItems(contract.charts.daily);
    expect(dailyBadges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ label: "一部暫定", tone: "warn" }),
        expect.objectContaining({ label: "更新確認", tone: "warn" }),
      ])
    );
    expect(buildDataFreshnessBadgeItems(contract.charts.weekly)).toContainEqual(
      expect.objectContaining({ label: "暫定", tone: "warn" })
    );
  });

  it("keeps missing, empty, and error states explicit", () => {
    const contract = normalizeMeeMeeDataFreshnessContract({
      detail: {
        classification: "missing",
        status: "empty",
      },
      charts: {
        monthly: {
          classification: "missing",
          freshness_state: "error",
          status: "error",
        },
      },
    });

    expect(buildDataFreshnessBadgeItems(contract.detail)).toContainEqual(
      expect.objectContaining({ label: "データなし", tone: "muted" })
    );
    expect(buildDataFreshnessBadgeItems(contract.charts.monthly)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ label: "確認エラー", tone: "error" }),
        expect.objectContaining({ label: "確認エラー", tone: "error" }),
      ])
    );
  });

  it("marks research-only as an internal warning instead of a normal product label", () => {
    const contract = normalizeMeeMeeDataFreshnessContract({
      detail: {
        classification: "research-only",
        status: "ready",
      },
    });

    expect(shouldShowResearchOnlyWarning(contract.detail)).toBe(true);
    expect(buildDataFreshnessBadgeItems(contract.detail)).toContainEqual(
      expect.objectContaining({ label: "研究用", tone: "error" })
    );
  });
});
