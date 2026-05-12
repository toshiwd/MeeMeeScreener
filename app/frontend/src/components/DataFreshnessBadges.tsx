import {
  buildDataFreshnessBadgeItems,
  getChartArea,
  normalizeMeeMeeDataFreshnessContract,
  shouldShowResearchOnlyWarning,
  type DataFreshnessBadgeItem,
  type DataFreshnessTimeframe,
  type MeeMeeDataFreshnessContract,
} from "../dataFreshnessContract";

type Props = {
  contract?: MeeMeeDataFreshnessContract | null;
  scope: "ranking" | "detail" | "chart";
  timeframe?: DataFreshnessTimeframe;
  compact?: boolean;
};

const compactVisibleKeys = new Set(["classification", "status", "freshness"]);

const filterCompactItems = (items: DataFreshnessBadgeItem[]) =>
  items.filter((item) => {
    if (!compactVisibleKeys.has(item.key)) return false;
    if (item.tone === "ok") return false;
    return true;
  });

const dedupeBadgeItems = (items: DataFreshnessBadgeItem[]) => {
  const seen = new Set<string>();
  return items.filter((item) => {
    const key = `${item.tone}:${item.label}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
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
  const items = dedupeBadgeItems(
    compact ? filterCompactItems(buildDataFreshnessBadgeItems(area)) : buildDataFreshnessBadgeItems(area)
  );
  if (compact && items.length === 0) return null;
  if (shouldShowResearchOnlyWarning(area)) {
    return (
      <span className={`data-freshness-badges ${compact ? "is-compact" : ""}`}>
        <span className="data-freshness-badge is-error">通常表示対象外</span>
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
