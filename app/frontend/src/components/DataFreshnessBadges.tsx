import {
  buildDataFreshnessBadgeItems,
  getChartArea,
  normalizeMeeMeeDataFreshnessContract,
  shouldShowResearchOnlyWarning,
  type DataFreshnessTimeframe,
  type MeeMeeDataFreshnessContract,
} from "../dataFreshnessContract";

type Props = {
  contract?: MeeMeeDataFreshnessContract | null;
  scope: "ranking" | "detail" | "chart";
  timeframe?: DataFreshnessTimeframe;
  compact?: boolean;
};

export default function DataFreshnessBadges({
  contract,
  scope,
  timeframe = "daily",
  compact = false,
}: Props) {
  const normalized = normalizeMeeMeeDataFreshnessContract(contract);
  const area =
    scope === "ranking"
      ? normalized.ranking
      : scope === "detail"
        ? normalized.detail
        : getChartArea(normalized, timeframe);
  const items = buildDataFreshnessBadgeItems(area);
  if (shouldShowResearchOnlyWarning(area)) {
    return (
      <span className={`data-freshness-badges ${compact ? "is-compact" : ""}`}>
        <span className="data-freshness-badge is-error">internal source blocked</span>
      </span>
    );
  }
  return (
    <span className={`data-freshness-badges ${compact ? "is-compact" : ""}`}>
      {items.map((item) => (
        <span key={item.key} className={`data-freshness-badge is-${item.tone}`}>
          {item.label}
        </span>
      ))}
    </span>
  );
}
