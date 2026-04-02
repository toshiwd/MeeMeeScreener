import { Link } from "react-router-dom";
import TradexTrackingSummaryCard from "../components/TradexTrackingSummaryCard";
import { tradexCandidateStatusLabel, tradexFreshnessLabel } from "../labels";
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

export default function TradexHomePage() {
  const { data, loading, error } = useTradexBootstrap();
  const candidates = data?.candidates ?? [];
  const topCandidates = [...candidates]
    .sort((a, b) => (Number(b.readiness_pass) - Number(a.readiness_pass)) || (b.sample_count ?? 0) - (a.sample_count ?? 0))
    .slice(0, 6);
  const focusCandidateId = readTradexLocal<string>(tradexStorageKeys.homeFocus, "");
  const summary = data?.summary;

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
        </div>
      </section>

      {error ? <div className="tradex-inline-error">{error}</div> : null}

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

      {focusCandidateId ? <div className="tradex-inline-note">前回注目していた候補: {focusCandidateId}</div> : null}
    </div>
  );
}
