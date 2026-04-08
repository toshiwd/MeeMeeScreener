type ResearchPatternBadgesProps = {
  researchPatternTag?: string | null;
  researchPriorBonus?: number | null;
  formatPct: (value?: number | null) => string;
};

export function ResearchPatternBadges({
  researchPatternTag,
  researchPriorBonus,
  formatPct,
}: ResearchPatternBadgesProps) {
  const hasPatternTag = researchPatternTag === "rebound_onset";
  const hasBonus = Number.isFinite(researchPriorBonus ?? NaN) && (researchPriorBonus ?? 0) > 0;
  if (!hasPatternTag && !hasBonus) {
    return null;
  }
  return (
    <span className="event-badges rank-header-event-badges">
      {hasPatternTag && <span className="event-badge event-earnings">反発初動候補</span>}
      {hasBonus && <span className="event-badge event-rights">補正 {formatPct(researchPriorBonus)}</span>}
    </span>
  );
}
