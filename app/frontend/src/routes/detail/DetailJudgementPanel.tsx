// @ts-nocheck
import type {
  AnalysisEntryPolicy,
  AnalysisEntryPolicySide,
  AnalysisResearchPrior,
  EnvironmentSummary,
} from "./detailTypes";
import ScreenPanel from "../../components/ScreenPanel";

type AnalysisDecisionSummary = {
  tone: "up" | "down" | "neutral";
  sideLabel: string | null;
  patternLabel: string | null;
  confidence: number | null;
  buyProb: number | null;
  sellProb: number | null;
  neutralProb: number | null;
};

type AnalysisGuidance = {
  confidenceRank: string;
  action: string;
  watchpoint: string;
  buyWidth: number;
  sellWidth: number;
  neutralWidth: number;
  buySetupProb: number | null;
  sellSetupProb: number | null;
  buySetupWidth: number;
  sellSetupWidth: number;
  buySetupState: string;
  sellSetupState: string;
};

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatSignedPercentLabel = (value: number | null | undefined, digits?: number) => string;

type Props = {
  analysisDtLabel: string | null;
  analysisSummaryLoading: boolean;
  analysisMissingDataVisible: boolean;
  analysisDecision: AnalysisDecisionSummary;
  analysisGuidance: AnalysisGuidance;
  analysisEntryPolicy: AnalysisEntryPolicy | null;
  patternSummary: EnvironmentSummary;
  analysisResearchPrior: AnalysisResearchPrior | null;
  formatPercentLabel: FormatPercentLabel;
  formatNumber: FormatNumber;
  formatSignedPercentLabel: FormatSignedPercentLabel;
};

const formatSetupIntent = (side: "buy" | "sell", setupType?: string | null) => {
  if (!setupType) return side === "buy" ? "買い候補" : "売り候補";
  if (setupType === "target20_breakout" || setupType === "breakout20") return "20%ブレイク";
  if (setupType === "breakout" || setupType === "breakout_trend") {
    return side === "buy" ? "上昇継続" : "下降継続";
  }
  if (setupType === "breakdown") return "下落継続";
  if (setupType === "accumulation" || setupType === "accumulation_break") {
    return side === "buy" ? "底値拾い" : "戻り待ち";
  }
  if (setupType === "continuation") return side === "buy" ? "継続上昇" : "継続下降";
  if (setupType === "pressure") return "売り圧";
  if (setupType === "rebound") return side === "buy" ? "反発" : "反落";
  if (setupType === "turn") return side === "buy" ? "転換上" : "転換下";
  if (setupType === "watch" || setupType === "watchlist") {
    return side === "buy" ? "押し目待ち" : "戻り待ち";
  }
  if (setupType === "ml_fallback_down") return "下落補正";
  return setupType;
};

const formatHoldWindow = (policy: AnalysisEntryPolicySide | null) => {
  if (
    Number.isFinite(policy?.recommendedHoldMinDays ?? NaN) &&
    Number.isFinite(policy?.recommendedHoldMaxDays ?? NaN)
  ) {
    const minDays = Math.round(policy?.recommendedHoldMinDays ?? 0);
    const maxDays = Math.round(policy?.recommendedHoldMaxDays ?? 0);
    if (minDays > 0 && maxDays > 0 && minDays !== maxDays) return `${minDays}-${maxDays}日`;
  }
  if (Number.isFinite(policy?.recommendedHoldDays ?? NaN)) {
    return `${Math.round(policy?.recommendedHoldDays ?? 0)}日`;
  }
  return "--";
};

const formatInvalidationTrigger = (value?: string | null) => {
  if (!value) return "--";
  if (value === "box_break") return "箱割れ";
  if (value === "box_reclaim") return "箱戻し";
  if (value === "stop3") return "-3%";
  if (value === "stop5") return "-5%";
  if (value === "ma20") return "MA20割れ";
  return value;
};

const formatInvalidationAction = (value?: string | null) => {
  if (!value) return "--";
  if (value === "exit") return "手仕舞い";
  if (value === "hold") return "継続";
  if (value === "doten_opt") return "ドテン";
  if (value === "doten_remainder") return "残りドテン";
  return value;
};

const formatExitPlan = (policy: AnalysisEntryPolicySide | null) => {
  const trigger = formatInvalidationTrigger(policy?.invalidationTrigger);
  const action = formatInvalidationAction(policy?.invalidationRecommendedAction);
  if (trigger === "--" && action === "--") return "--";
  let summary = `${trigger}で${action}`;
  if (
    policy?.invalidationDotenRecommended &&
    Number.isFinite(policy?.invalidationOppositeHoldDays ?? NaN)
  ) {
    summary += ` / 反対側は${Math.round(policy?.invalidationOppositeHoldDays ?? 0)}日`;
  }
  return summary;
};

const resolveToneLabel = (tone: AnalysisDecisionSummary["tone"]) => {
  if (tone === "up") return "買い優先";
  if (tone === "down") return "売り優先";
  return "中立";
};

const resolveConfidenceRankLabel = (value: string) => {
  if (value === "high") return "高";
  if (value === "mid") return "中";
  if (value === "low") return "低";
  return value || "--";
};

const resolveActionLabel = (value: string) => {
  if (value === "buy_watch") return "買い継続監視";
  if (value === "buy_setup") return "買い仕込み候補";
  if (value === "sell_watch") return "売り継続監視";
  if (value === "sell_setup") return "売り仕込み候補";
  if (value === "neutral_watch") return "様子見";
  return value || "--";
};

const resolveSetupStateLabel = (value: string) => {
  if (value === "monitor") return "監視";
  if (value === "wait") return "待機";
  if (value === "setup") return "仕込み";
  if (value === "--") return "--";
  return value;
};

const resolveScenarioResultLabel = (value: string) => {
  if (value === "buy") return "買い";
  if (value === "sell") return "売り";
  if (value === "neutral") return "中立";
  return value;
};

const formatScenarioLabel = (tone: EnvironmentSummary["scenarios"][number]["tone"]) => {
  if (tone === "up") return "買い";
  if (tone === "down") return "売り";
  return "中立";
};

export function DetailJudgementPanel({
  analysisDtLabel,
  analysisSummaryLoading,
  analysisMissingDataVisible,
  analysisDecision,
  analysisGuidance,
  analysisEntryPolicy,
  patternSummary,
  analysisResearchPrior,
  formatPercentLabel,
  formatNumber,
  formatSignedPercentLabel,
}: Props) {
  const buyPolicy = analysisEntryPolicy?.up ?? null;
  const sellPolicy = analysisEntryPolicy?.down ?? null;
  const scenarioCount = patternSummary.scenarios.length;
  const sortedScenarios = patternSummary.scenarios.slice().sort((a, b) => b.score - a.score);
  const buyProb = analysisDecision.buyProb;
  const sellProb = analysisDecision.sellProb;
  const neutralProb = analysisDecision.neutralProb;
  const hasPriorityData = [buyProb, sellProb, neutralProb].some((value) => Number.isFinite(value ?? NaN));
  const judgementUnavailable = analysisMissingDataVisible || (!analysisSummaryLoading && !hasPriorityData && scenarioCount === 0);
  const judgementStateLabel = analysisSummaryLoading
    ? "確認中"
    : judgementUnavailable
      ? "判定不可"
      : analysisDecision.sideLabel ?? resolveToneLabel(analysisDecision.tone);
  const judgementBasisLabel = analysisSummaryLoading
    ? "ranking appearance を読み込み中"
    : judgementUnavailable
      ? "選択日にこの銘柄の ranking appearance がないため、売買優先度は出せません。"
      : analysisDecision.patternLabel ?? patternSummary.environmentLabel;
  const confidenceLabel = analysisSummaryLoading
    ? "確認中"
    : judgementUnavailable
      ? "--"
      : resolveConfidenceRankLabel(analysisGuidance.confidenceRank);
  const actionLabel = analysisSummaryLoading
    ? "確認中"
    : judgementUnavailable
      ? "見送り"
      : resolveActionLabel(analysisGuidance.action);
  const watchpointLabel = analysisSummaryLoading
    ? "ranking appearance を確認中"
    : judgementUnavailable
      ? "買い/売り判定に使えるランキング根拠なし"
      : analysisGuidance.watchpoint;
  const formatPriorityPercent = (value: number | null | undefined) =>
    analysisSummaryLoading ? "確認中" : judgementUnavailable ? "--" : formatPercentLabel(value);

  return (
    <ScreenPanel title="判定サマリー" className="detail-analysis-panel">
      <div className="detail-analysis-body">
        {analysisDtLabel && <div className="detail-analysis-meta">対象日 {analysisDtLabel}</div>}
        <div className="detail-analysis-meta">
          ※ これは選択日のランキング appearance を基準にした判定サマリーです。
        </div>
        {analysisMissingDataVisible && (
          <div className="detail-analysis-meta">その日の ranking appearance はまだ見つかっていません。</div>
        )}
        <div className="detail-analysis-section">
          <div className="detail-analysis-section-title">現在の優先度</div>
          <div className="detail-analysis-grid">
            <div className="detail-analysis-card">
              <div className="detail-analysis-label">判定</div>
              <div className="detail-analysis-value">
                {judgementStateLabel}
              </div>
              <div className="detail-analysis-meta">
                {judgementBasisLabel}
              </div>
            </div>
            <div className="detail-analysis-card">
              <div className="detail-analysis-label">確信度</div>
              <div className="detail-analysis-value">
                {confidenceLabel}
              </div>
              <div className="detail-analysis-meta">ranking appearance / risk_mode {analysisEntryPolicy?.riskMode ?? "--"}</div>
            </div>
            <div className="detail-analysis-card">
              <div className="detail-analysis-label">今やること</div>
              <div className="detail-analysis-value">
                {actionLabel}
              </div>
              <div className="detail-analysis-meta">
                {watchpointLabel}
              </div>
            </div>
            <div className="detail-analysis-card">
              <div className="detail-analysis-label">判定要素</div>
              <div className="detail-analysis-value">{scenarioCount}本</div>
              <div className="detail-analysis-meta">
                {judgementUnavailable ? "ランキング根拠なし" : patternSummary.environmentLabel}
              </div>
            </div>
          </div>
        </div>
        <div className="detail-analysis-section">
          <div className="detail-analysis-section-title">買い / 中立 / 売りの優先度</div>
          <div className="detail-analysis-prob-meter-list">
            <div className="detail-analysis-prob-meter-row tone-up">
              <div className="detail-analysis-prob-meter-label">
                買い {formatPriorityPercent(buyProb)}
              </div>
              <div className="detail-analysis-prob-meter-track">
                <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buyWidth}%` }} />
              </div>
            </div>
            <div className="detail-analysis-prob-meter-row tone-neutral">
              <div className="detail-analysis-prob-meter-label">
                中立 {formatPriorityPercent(neutralProb)}
              </div>
              <div className="detail-analysis-prob-meter-track">
                <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.neutralWidth}%` }} />
              </div>
            </div>
            <div className="detail-analysis-prob-meter-row tone-down">
              <div className="detail-analysis-prob-meter-label">
                売り {formatPriorityPercent(sellProb)}
              </div>
              <div className="detail-analysis-prob-meter-track">
                <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellWidth}%` }} />
              </div>
            </div>
          </div>
        </div>
        <div className="detail-analysis-section">
          <div className="detail-analysis-section-title">継続保有 / 仕込み</div>
          {judgementUnavailable ? (
            <div className="detail-analysis-empty">買い・売り候補なし。選択日のランキング掲載が確認できるまで見送りです。</div>
          ) : (
            <div className="detail-analysis-call-reason-list">
              <div className="detail-analysis-call-reason">
                <div>買い</div>
                <div className="detail-analysis-meta">
                  {formatSetupIntent("buy", buyPolicy?.setupType)} / {formatHoldWindow(buyPolicy)} / {formatExitPlan(buyPolicy)}
                </div>
                {buyPolicy?.recommendedHoldReason && (
                  <div className="detail-analysis-meta">補足 {buyPolicy.recommendedHoldReason}</div>
                )}
              </div>
              <div className="detail-analysis-call-reason">
                <div>売り</div>
                <div className="detail-analysis-meta">
                  {formatSetupIntent("sell", sellPolicy?.setupType)} / {formatHoldWindow(sellPolicy)} / {formatExitPlan(sellPolicy)}
                </div>
                {sellPolicy?.recommendedHoldReason && (
                  <div className="detail-analysis-meta">補足 {sellPolicy.recommendedHoldReason}</div>
                )}
              </div>
              <div className="detail-analysis-call-reason">
                <div>中立</div>
                <div className="detail-analysis-meta">
                  {analysisSummaryLoading ? "確認中" : `様子見 / ${analysisGuidance.watchpoint}`}
                </div>
              </div>
            </div>
          )}
        </div>
        <div className="detail-analysis-section">
          <div className="detail-analysis-section-title">判定要素</div>
          {sortedScenarios.length > 0 ? (
            <div className="detail-analysis-call-reason-list">
              {sortedScenarios.slice(0, 3).map((item) => (
                <div key={item.key} className="detail-analysis-call-reason">
                  <div>
                    {formatScenarioLabel(item.tone)} / {resolveScenarioResultLabel(item.label)}
                  </div>
                  <div className="detail-analysis-meta">score {formatPercentLabel(item.score)}</div>
                  {item.reasons.length > 0 && <div className="detail-analysis-meta">{item.reasons.slice(0, 2).join(" / ")}</div>}
                </div>
              ))}
            </div>
          ) : (
            <div className="detail-analysis-empty">判定要素データなし</div>
          )}
        </div>
        {analysisResearchPrior && (
          <div className="detail-analysis-section">
            <div className="detail-analysis-section-title">保有取引実績</div>
            <div className="detail-analysis-call-reason-list">
              <div className="detail-analysis-call-reason">
                <div>買い側</div>
                <div className="detail-analysis-meta">
                  {analysisResearchPrior.up?.rank != null ? `rank ${analysisResearchPrior.up.rank}` : "--"}
                  {analysisResearchPrior.up?.fitScore != null ? ` / 適合度 ${formatPercentLabel(analysisResearchPrior.up.fitScore, 0)}` : ""}
                </div>
                {analysisResearchPrior.up?.adoptionReasons?.length ? (
                  <div className="detail-analysis-meta">{analysisResearchPrior.up.adoptionReasons.slice(0, 2).join(" / ")}</div>
                ) : null}
              </div>
              <div className="detail-analysis-call-reason">
                <div>売り側</div>
                <div className="detail-analysis-meta">
                  {analysisResearchPrior.down?.rank != null ? `rank ${analysisResearchPrior.down.rank}` : "--"}
                  {analysisResearchPrior.down?.fitScore != null ? ` / 適合度 ${formatPercentLabel(analysisResearchPrior.down.fitScore, 0)}` : ""}
                </div>
                {analysisResearchPrior.down?.adoptionReasons?.length ? (
                  <div className="detail-analysis-meta">{analysisResearchPrior.down.adoptionReasons.slice(0, 2).join(" / ")}</div>
                ) : null}
              </div>
            </div>
          </div>
        )}
        <div className="detail-analysis-meta">
          現在の優先度: {judgementStateLabel}
          {!judgementUnavailable && analysisDecision.confidence != null ? ` / 確信度 ${formatPercentLabel(analysisDecision.confidence)}` : ""}
          {!judgementUnavailable && analysisDecision.patternLabel ? ` / 参照 ${analysisDecision.patternLabel}` : ""}
        </div>
        <div className="detail-analysis-meta">
          ランキングの売買判定は残っています。ここでは ranking appearance、表示スコア、厳選通過状態から買い・売り・中立の優先度を表示しています。
        </div>
      </div>
    </ScreenPanel>
  );
}

export default DetailJudgementPanel;
