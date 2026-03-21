import { Link } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";

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
        <span className="tradex-pill">{status}</span>
        <span className="tradex-pill is-muted">件数 {sampleCount == null ? "--" : sampleCount.toLocaleString("ja-JP")}</span>
        <span className="tradex-pill is-muted">期待値差 {expectancyDelta == null ? "--" : formatNumber(expectancyDelta, 4)}</span>
      </div>
      <div className="tradex-candidate-card-actions">
        <Link
          to={`/compare?candidateId=${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.compareCandidateId, candidateId)}
        >
          候補比較へ
        </Link>
        <Link
          to={`/adopt?candidateId=${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.adoptCandidateId, candidateId)}
        >
          反映判定へ
        </Link>
        <Link
          to={`/detail/${encodeURIComponent(candidateId)}`}
          onClick={() => writeTradexLocal(tradexStorageKeys.detailCandidateId, candidateId)}
        >
          候補詳細
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

  return (
    <div className="tradex-page tradex-home-page">
      <section className="tradex-hero">
        <div>
          <div className="tradex-page-kicker">現在の研究</div>
          <h1 className="tradex-page-title">今、どの候補を見て、どれを採用するか</h1>
          <p className="tradex-page-lead">
            TRADEX は研究の進捗、候補の比較、反映の判断、過去検証の確認をまとめて扱う内部コンソールです。
            まずは候補比較と反映判定を見て、必要なら詳細と検証へ降りていきます。
          </p>
        </div>
        <div className="tradex-hero-actions">
          <Link className="tradex-primary-action" to="/compare">
            候補比較を開く
          </Link>
          <Link className="tradex-secondary-action" to="/verify">
            検証を見る
          </Link>
        </div>
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">まず見る導線</div>
            <div className="tradex-panel-caption">判断の前に必要な画面だけを先に並べています。</div>
          </div>
        </div>
        <div className="tradex-action-grid">
          <article className="tradex-action-card">
            <div className="tradex-action-card-title">候補比較</div>
            <div className="tradex-action-card-desc">候補同士と現行版との差分を、総合点・最大損失・件数で並べて見ます。</div>
            <Link to="/compare">開く</Link>
          </article>
          <article className="tradex-action-card">
            <div className="tradex-action-card-title">反映判定</div>
            <div className="tradex-action-card-desc">比較確認を前提に、採用するか保留にするかを判断します。</div>
            <Link to="/adopt">開く</Link>
          </article>
          <article className="tradex-action-card">
            <div className="tradex-action-card-title">検証</div>
            <div className="tradex-action-card-desc">実行待ち / 実行中 / 完了 / 異常 の進捗を確認します。</div>
            <Link to="/verify">開く</Link>
          </article>
        </div>
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">注目候補</div>
            <div className="tradex-panel-caption">比較に進みやすい候補を上から並べています。</div>
          </div>
          <span className="tradex-pill is-muted">{loading ? "読み込み中" : `${candidates.length}件`}</span>
        </div>
        {error ? <div className="tradex-inline-error">{error}</div> : null}
        {focusCandidateId ? <div className="tradex-inline-note">最後に見た候補: {focusCandidateId}</div> : null}
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
      </section>
    </div>
  );
}
