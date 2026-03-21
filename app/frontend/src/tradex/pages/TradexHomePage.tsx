import { Link } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import { tradexCandidateStatusLabel, tradexFreshnessLabel } from "../labels";

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
        <span className="tradex-pill is-muted">期待値差分 {expectancyDelta == null ? "--" : formatNumber(expectancyDelta, 4)}</span>
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
          候補詳細へ
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
          <h1 className="tradex-page-title">研究の進捗と採用判断をまとめて確認する</h1>
          <p className="tradex-page-lead">
            候補比較、反映判定、候補詳細をひとつにつなぎ、今どれを保留し、どれを採用判断に進めるかを素早く見ます。
          </p>
        </div>
        <div className="tradex-hero-aside">
          <div className="tradex-hero-chip">基準日 {summary?.as_of_date ?? (loading ? "読み込み中" : "--")}</div>
          <div className="tradex-hero-chip">鮮度 {tradexFreshnessLabel(summary?.freshness_state)}</div>
          <div className="tradex-hero-chip">注目件数 {summary?.attention_count?.toLocaleString("ja-JP") ?? "0"}</div>
        </div>
      </section>

      {error ? <div className="tradex-inline-error">{error}</div> : null}

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">今すぐ見る候補</div>
            <div className="tradex-panel-caption">採用候補を先に続ようにし、その後に比較と反映判定へ進みます。</div>
          </div>
          <div className="tradex-panel-actions">
            <Link className="tradex-secondary-action" to="/verify">検証へ</Link>
            <Link className="tradex-secondary-action" to="/compare">候補比較へ</Link>
            <Link className="tradex-secondary-action" to="/adopt">反映判定へ</Link>
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
            <p>検証データが届くまで、検証と比較の導線だけ先に使えるようにしています。</p>
            <div className="tradex-empty-actions">
              <Link to="/verify">検証へ</Link>
              <Link to="/legacy/tags">旧検証画面へ</Link>
            </div>
          </div>
        )}
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">進め方</div>
            <div className="tradex-panel-caption">候補比較を起点に、採用判断と候補詳細を順番に追います。</div>
          </div>
        </div>
        <div className="tradex-flow-grid">
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">1</div>
            <div className="tradex-flow-title">検証</div>
            <div className="tradex-flow-text">研究候補の状態、進捗、異常を確認します。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">2</div>
            <div className="tradex-flow-title">候補比較</div>
            <div className="tradex-flow-text">現行版との差分を見て、候補同士を比べます。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">3</div>
            <div className="tradex-flow-title">反映判定</div>
            <div className="tradex-flow-text">比較確認を経て、保留か採用申請かを決めます。</div>
          </article>
          <article className="tradex-flow-card">
            <div className="tradex-flow-step">4</div>
            <div className="tradex-flow-title">候補詳細</div>
            <div className="tradex-flow-text">個別候補の内訳、差分、検証結果を掛け込みます。</div>
          </article>
        </div>
      </section>

      {focusCandidateId ? <div className="tradex-inline-note">前回注目した候補: {focusCandidateId}</div> : null}
    </div>
  );
}
