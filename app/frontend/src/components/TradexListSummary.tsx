import type { ReactNode } from "react";
import {
  formatTradexListSummaryConfidence,
  formatTradexListSummaryReadinessLabel,
  formatTradexListSummaryToneLabel,
  formatTradexListSummaryUnavailableReason,
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
          {summaryBadge("分析を確認中...", "rank-qualification is-warn")}
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
            summary.reason
              ? `分析を確認できません (${formatTradexListSummaryUnavailableReason(summary.reason) ?? "詳細確認中"})`
              : "分析を確認できません",
            "rank-qualification is-warn"
          )}
        </div>
      </div>
    );
  }

  const toneLabel = formatTradexListSummaryToneLabel(summary.dominantTone);
  const toneBadgeClass = summary.dominantTone ? toneClassName[summary.dominantTone] : "";
  const readiness = summary.publishReadiness;
  const readinessLabel = formatTradexListSummaryReadinessLabel(summary);
  const readinessClass = readiness ? readinessClassName(readiness.ready) : "is-warn";
  const userFacingReasons = summary.reasons
    .filter((reason) => /[\u3040-\u30ff\u3400-\u9fff]/.test(reason) && !/[=_]/.test(reason))
    .slice(0, 2);

  return (
    <div className={["tradex-list-summary", className].filter(Boolean).join(" ")}>
      <div className="rank-badges">
        {summaryBadge(`検証 ${toneLabel}`, toneBadgeClass)}
        {summaryBadge(`信頼度 ${formatTradexListSummaryConfidence(summary.confidence)}`)}
        {summaryBadge(readinessLabel, `rank-qualification ${readinessClass}`)}
      </div>
      {userFacingReasons.length > 0 && (
        <div className="signal-chips">
          {userFacingReasons.map((reason) => (
            <span key={reason} className="signal-chip achieved">
              {reason}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
