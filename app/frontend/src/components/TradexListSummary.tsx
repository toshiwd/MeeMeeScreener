import type { ReactNode } from "react";
import {
  formatTradexListSummaryConfidence,
  formatTradexListSummaryReadinessLabel,
  formatTradexListSummaryToneLabel,
  type TradexListSummaryItem,
} from "../routes/list/tradexSummary";

type TradexListSummaryProps = {
  summary: TradexListSummaryItem | null;
  loading?: boolean;
  className?: string;
};

const toneClassName: Record<"buy" | "neutral" | "sell", string> = {
  buy: "is-ok",
  neutral: "",
  sell: "is-warn",
};

const readinessClassName = (ready: boolean) => (ready ? "is-ok" : "is-warn");

const summaryBadge = (label: ReactNode, className = "") => (
  <span className={`rank-score-badge ${className}`.trim()}>{label}</span>
);

export default function TradexListSummary({ summary, loading = false, className }: TradexListSummaryProps) {
  if (!summary && !loading) return null;
  if (loading && !summary) {
    return (
      <div className={["tradex-list-summary", className].filter(Boolean).join(" ")}>
        <div className="rank-badges">
          {summaryBadge("TRADEX summary loading...", "rank-qualification is-warn")}
        </div>
      </div>
    );
  }
  if (!summary) return null;
  if (!summary.available) {
    return (
      <div className={["tradex-list-summary", className].filter(Boolean).join(" ")}>
        <div className="rank-badges">
          {summaryBadge(
            summary.reason ? `TRADEX: analysis unavailable (${summary.reason})` : "TRADEX: analysis unavailable",
            "rank-qualification is-warn"
          )}
        </div>
      </div>
    );
  }

  const toneLabel = formatTradexListSummaryToneLabel(summary.dominantTone);
  const toneBadgeClass = summary.dominantTone ? toneClassName[summary.dominantTone] : "";
  const readiness = summary.publishReadiness;
  const readinessLabel = readiness?.ready ? "publish readiness: ready" : formatTradexListSummaryReadinessLabel(summary);
  const readinessClass = readiness ? readinessClassName(readiness.ready) : "is-warn";

  return (
    <div className={["tradex-list-summary", className].filter(Boolean).join(" ")}>
      <div className="rank-badges">
        {summaryBadge(`TRADEX ${toneLabel}`, toneBadgeClass)}
        {summaryBadge(`confidence ${formatTradexListSummaryConfidence(summary.confidence)}`)}
        {summaryBadge(readinessLabel, `rank-qualification ${readinessClass}`)}
      </div>
      {summary.reasons.length > 0 && (
        <div className="signal-chips">
          {summary.reasons.slice(0, 2).map((reason) => (
            <span key={reason} className="signal-chip achieved">
              {reason}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
