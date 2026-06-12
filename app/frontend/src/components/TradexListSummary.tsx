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

const lifecycleLabel: Record<string, string> = {
  Probe: "打診",
  Watch: "監視",
  AddWatch: "追加監視",
  HoldReview: "保有確認",
  TakeProfit: "利確",
  Exit: "撤退",
  Avoid: "回避",
  RegimeMissing: "地合い欠損",
};

const lifecycleClassName = (state: string | null) => {
  if (state === "Probe" || state === "AddWatch" || state === "TakeProfit") return "is-ok";
  if (state === "Avoid" || state === "Exit" || state === "RegimeMissing") return "is-warn";
  return "is-neutral";
};

const readinessClassName = (ready: boolean) => (ready ? "is-ok" : "is-warn");

const formatPct = (value: number | null) =>
  Number.isFinite(value ?? NaN) ? `${(Number(value) * 100).toFixed(1)}%` : "--";

const summaryBadge = (label: ReactNode, className = "") => (
  <span className={`rank-score-badge ${className}`.trim()}>{label}</span>
);

export default function TradexListSummary({ summary, loading = false, className }: TradexListSummaryProps) {
  if (!summary && !loading) return null;
  if (loading && !summary) {
    return (
      <div className={["tradex-list-summary", className].filter(Boolean).join(" ")}>
        <div className="rank-badges">{summaryBadge("TRADEX確認中...", "rank-qualification is-neutral")}</div>
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

  const lifecycle = summary.shortLifecycle;
  const hasAnalysis = Boolean(summary.dominantTone || summary.confidence != null || summary.publishReadiness);
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
        {lifecycle
          ? summaryBadge(
              `TRADEX売り監視: ${lifecycleLabel[lifecycle.state ?? ""] ?? lifecycle.state ?? "--"}`,
              `rank-qualification ${lifecycleClassName(lifecycle.state)}`
            )
          : null}
        {lifecycle ? summaryBadge(`期待下げ ${formatPct(lifecycle.expectedDownsidePct)}`) : null}
        {lifecycle && lifecycle.riskRewardToSl8 != null ? summaryBadge(`RR ${lifecycle.riskRewardToSl8.toFixed(2)}`) : null}
        {lifecycle?.signalYmd ? summaryBadge(`signal ${lifecycle.signalYmd}`, "rank-qualification is-neutral") : null}
        {hasAnalysis ? summaryBadge(`検証 ${toneLabel}`, toneBadgeClass) : null}
        {hasAnalysis ? summaryBadge(`信頼度 ${formatTradexListSummaryConfidence(summary.confidence)}`) : null}
        {hasAnalysis ? summaryBadge(readinessLabel, `rank-qualification ${readinessClass}`) : null}
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
