import { Link } from "react-router-dom";
import TradexTrackingSummaryCard from "../components/TradexTrackingSummaryCard";
import { tradexCandidateStatusLabel, tradexFreshnessLabel } from "../labels";
import type {
  TradexForecastSurfaceProjection,
  TradexForecastSurfaceProjectionRow,
  TradexLiveStrategyJudgement
} from "../contracts";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import { useTradexBootstrap } from "../useTradexBootstrap";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(digits);
};

function CandidateCard({
  candidateId,
  logicKey,
  title,
  status,
  sampleCount,
  expectancyDelta
}: {
  candidateId: string;
  logicKey: string;
  title: string;
  status: string;
  sampleCount: number | null;
  expectancyDelta: number | null;
}) {
  return (
    <article className="tradex-candidate-card">
      <div className="tradex-candidate-card-title">{title}</div>
      <div className="tradex-candidate-card-meta">
        <span>{candidateId}</span>
        <span>{logicKey}</span>
      </div>
      <div className="tradex-candidate-card-status">
        <span className="tradex-pill">{tradexCandidateStatusLabel(status)}</span>
        <span className="tradex-pill is-muted">件数 {sampleCount == null ? "--" : sampleCount.toLocaleString("ja-JP")}</span>
        <span className="tradex-pill is-muted">期待差 {expectancyDelta == null ? "--" : formatNumber(expectancyDelta, 4)}</span>
      </div>
      <div className="tradex-candidate-card-actions">
        <Link
          to={`/compare?candidateId=${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.compareCandidateId, candidateId)}
        >
          比較へ
        </Link>
        <Link
          to={`/adopt?candidateId=${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.adoptCandidateId, candidateId)}
        >
          採用へ
        </Link>
        <Link
          to={`/detail/${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.detailCandidateId, candidateId)}
        >
          詳細へ
        </Link>
      </div>
    </article>
  );
}

function renderProjectionRow(row: TradexForecastSurfaceProjectionRow) {
  return (
    <div key={`${row.side}:${row.code}:${row.action_state}`} className="tradex-projection-row">
      <strong>{row.code}</strong>
      <span className="tradex-pill is-muted">{row.side}</span>
      <span className="tradex-pill">{row.action_state}</span>
      <span className="tradex-pill is-muted">p {formatNumber(row.direction_prob, 2)}</span>
      <span className="tradex-pill is-muted">up {formatNumber(row.expected_upside, 2)}</span>
      <span className="tradex-pill is-muted">down {formatNumber(row.expected_downside, 2)}</span>
    </div>
  );
}

function ForecastSurfacePreview({ projection }: { projection: TradexForecastSurfaceProjection | null }) {
  if (!projection) return null;

  const summary = projection.summary;
  const coverage = typeof summary.coverage_ratio === "number" ? `${(summary.coverage_ratio * 100).toFixed(1)}%` : "--";
  const alerts = summary.alerts.length > 0 ? summary.alerts.join(" / ") : "none";
  const topLong = projection.long_rank.slice(0, 4);
  const topShort = projection.short_rank.slice(0, 4);
  const promotions = projection.watchlist_promotions.slice(0, 4);

  return (
    <section className="tradex-panel">
      <div className="tradex-panel-head">
        <div>
          <div className="tradex-panel-title">Forecast surface preview</div>
          <div className="tradex-panel-caption">Current TRADEX ranking surface, surfaced read-only for release review.</div>
        </div>
      </div>
      <div className="tradex-flow-grid">
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Surface summary</div>
          <div className="tradex-flow-text">
            <div>as_of: {summary.as_of_date ?? "--"}</div>
            <div>model: {summary.model_version ?? "--"}</div>
            <div>coverage: {coverage}</div>
            <div>
              rows: {summary.actual_row_count ?? 0} / {summary.expected_row_count ?? 0}
            </div>
            <div>alerts: {alerts}</div>
          </div>
        </article>
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Top long</div>
          <div className="tradex-flow-text">{topLong.length > 0 ? topLong.map(renderProjectionRow) : <div>--</div>}</div>
        </article>
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Top short</div>
          <div className="tradex-flow-text">{topShort.length > 0 ? topShort.map(renderProjectionRow) : <div>--</div>}</div>
        </article>
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Watchlist promotions</div>
          <div className="tradex-flow-text">{promotions.length > 0 ? promotions.map(renderProjectionRow) : <div>--</div>}</div>
        </article>
      </div>
    </section>
  );
}

function LiveStrategyJudgementPreview({ judgement }: { judgement: TradexLiveStrategyJudgement | null }) {
  if (!judgement) return null;

  const isBuy = judgement.human_readable_judgement === "buy";
  const stateLabel = judgement.human_readable_judgement ? judgement.human_readable_judgement.toUpperCase() : "UNKNOWN";
  const actionLabel = judgement.machine_action_state ? judgement.machine_action_state.toUpperCase() : "UNKNOWN";
  const scoreLabel = typeof judgement.buy_score === "number" ? judgement.buy_score.toFixed(2) : "--";
  const targetLabel = [judgement.target.code, judgement.target.as_of_date].filter(Boolean).join(" / ") || "--";
  const reasonLabel = judgement.reason_codes.length > 0 ? judgement.reason_codes.join(" / ") : "none";

  return (
    <section className="tradex-panel">
      <div className="tradex-panel-head">
        <div>
          <div className="tradex-panel-title">Live judgement</div>
          <div className="tradex-panel-caption">Latest strategy judgement surfaced from the app backend.</div>
        </div>
        <div className="tradex-panel-actions">
          <span className={`tradex-pill ${isBuy ? "is-ok" : "is-muted"}`}>{stateLabel}</span>
          <span className="tradex-pill is-muted">{actionLabel}</span>
        </div>
      </div>
      <div className="tradex-flow-grid">
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">{isBuy ? "買い判定" : "現在は買いではない"}</div>
          <div className="tradex-flow-text">
            <div>target: {targetLabel}</div>
            <div>score: {scoreLabel}</div>
            <div>adapter: {judgement.primary_adapter_id ?? "--"}</div>
            <div>authoritative: {judgement.authoritative_decision ?? "--"}</div>
          </div>
        </article>
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Scores</div>
          <div className="tradex-flow-text">
            <div>environment: {formatNumber(judgement.environment_score, 2)}</div>
            <div>trend: {formatNumber(judgement.trend_score, 2)}</div>
            <div>trigger: {formatNumber(judgement.trigger_score, 2)}</div>
            <div>risk: {formatNumber(judgement.risk_score, 2)}</div>
          </div>
        </article>
        <article className="tradex-flow-card">
          <div className="tradex-flow-title">Reasons</div>
          <div className="tradex-flow-text">
            <div>codes: {reasonLabel}</div>
            <div>experiment: {judgement.experiment_id ?? "--"}</div>
            <div>generated_at: {judgement.generated_at ?? "--"}</div>
            <div>signal: {judgement.is_buy_signal ? "buy" : "watch"}</div>
          </div>
        </article>
      </div>
    </section>
  );
}

export default function TradexHomePage() {
  const { data, loading, error } = useTradexBootstrap();
  const candidates = data?.candidates ?? [];
  const topCandidates = [...candidates]
    .sort(
      (a, b) =>
        Number(b.readiness_pass) - Number(a.readiness_pass) ||
        (b.expectancy_delta ?? Number.NEGATIVE_INFINITY) - (a.expectancy_delta ?? Number.NEGATIVE_INFINITY) ||
        (b.sample_count ?? 0) - (a.sample_count ?? 0)
    )
    .slice(0, 6);
  const focusCandidateId = readTradexLocal<string>(tradexStorageKeys.homeFocus, "");
  const summary = data?.summary;
  const liveStrategyJudgement = data?.live_strategy_judgement ?? null;
  const forecastSurfaceProjection = data?.forecast_surface_projection?.projection ?? null;

  return (
    <div className="tradex-page tradex-home-page">
      <section className="tradex-hero">
        <div>
          <div className="tradex-page-kicker">検証ホーム</div>
          <h1 className="tradex-page-title">候補の確認と採用判断をまとめて追う</h1>
          <p className="tradex-page-lead">
            比較、採用、候補詳細、そして判定追跡 summary を同じホームで確認します。重い追跡 UI は MeeMeeA 側へ寄せ、TRADEX は read-only に留めます。
          </p>
        </div>
        <div className="tradex-hero-aside">
          <div className="tradex-hero-chip">基準日 {summary?.as_of_date ?? (loading ? "読み込み中" : "--")}</div>
          <div className="tradex-hero-chip">鮮度 {tradexFreshnessLabel(summary?.freshness_state)}</div>
          <div className="tradex-hero-chip">要確認 {summary?.attention_count?.toLocaleString("ja-JP") ?? "0"}</div>
          <div className="tradex-hero-chip">
            authoritative {summary?.authoritative_state ?? "unknown"}
            {summary?.authoritative_decision ? ` / ${summary.authoritative_decision}` : ""}
          </div>
          <div className="tradex-hero-chip">
            live {liveStrategyJudgement?.human_readable_judgement ?? "unknown"}
            {liveStrategyJudgement?.human_readable_judgement === "buy" ? " / BUY" : ""}
          </div>
        </div>
      </section>

      {error ? <div className="tradex-inline-error">{error}</div> : null}
      <LiveStrategyJudgementPreview judgement={liveStrategyJudgement} />

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">まず見る候補</div>
            <div className="tradex-panel-caption">比較と採用に繋げやすい候補を上位から並べます。</div>
          </div>
          <div className="tradex-panel-actions">
            <Link className="tradex-secondary-action" to="/verify">
              検証へ
            </Link>
            <Link className="tradex-secondary-action" to="/compare">
              比較へ
            </Link>
            <Link className="tradex-secondary-action" to="/adopt">
              採用へ
            </Link>
          </div>
        </div>

        {topCandidates.length > 0 ? (
          <div className="tradex-candidate-grid">
            {topCandidates.map((candidate) => (
              <CandidateCard
                key={candidate.candidate_id}
                candidateId={candidate.candidate_id}
                logicKey={candidate.logic_key}
                title={candidate.name}
                status={candidate.status}
                sampleCount={candidate.sample_count}
                expectancyDelta={candidate.expectancy_delta}
              />
            ))}
          </div>
        ) : (
          <div className="tradex-empty-state">
            <strong>候補がまだありません。</strong>
            <p>検証データを読み込み、候補の比較と採用に進める状態にしてください。</p>
            <div className="tradex-empty-actions">
              <Link to="/verify">検証へ</Link>
              <Link to="/legacy/tags">旧タグ検証へ</Link>
            </div>
          </div>
        )}
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">進め方</div>
            <div className="tradex-panel-caption">確認から採用までの流れを短く保ちます。</div>
          </div>
        </div>
        <div className="tradex-flow-grid">
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">1</div>
            <div className="tradex-flow-title">検証</div>
            <div className="tradex-flow-text">候補の基礎統計と注意点をまず確認します。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">2</div>
            <div className="tradex-flow-title">比較</div>
            <div className="tradex-flow-text">既存候補との差を並べて、残す理由を見極めます。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">3</div>
            <div className="tradex-flow-title">採用</div>
            <div className="tradex-flow-text">採用判断を記録し、運用へ渡します。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">4</div>
            <div className="tradex-flow-title">追跡</div>
            <div className="tradex-flow-text">予後監視は MeeMeeA の判定追跡で確認します。</div>
          </article>
        </div>
      </section>

      <TradexTrackingSummaryCard />
      <ForecastSurfacePreview projection={forecastSurfaceProjection} />

      {focusCandidateId ? <div className="tradex-inline-note">前回注目していた候補: {focusCandidateId}</div> : null}
    </div>
  );
}
