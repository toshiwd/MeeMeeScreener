import { Link, useParams } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { buildComparisonDraft, findTradexCandidate } from "../data";
import { readTradexLocal, tradexStorageKeys, writeTradexLocal } from "../storage";
import { tradexCandidateStatusLabel, tradexValidationStatusLabel } from "../labels";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
};

const pickSymbol = (candidate: ReturnType<typeof findTradexCandidate>) => {
  if (!candidate) return null;
  const sources = [candidate.published_logic_manifest, candidate.published_ranking_snapshot, candidate.validation_summary];
  for (const source of sources) {
    if (!source) continue;
    const code =
      typeof source.code === "string"
        ? source.code
        : typeof source.symbol === "string"
          ? source.symbol
          : typeof source.ticker === "string"
            ? source.ticker
            : null;
    if (code) return code;
  }
  return null;
};

export default function TradexCandidateDetailPage() {
  const { candidateId } = useParams();
  const { data } = useTradexBootstrap();
  const candidates = data?.candidates ?? [];
  const candidate = findTradexCandidate(candidates, candidateId ?? null);
  const symbol = pickSymbol(candidate);
  const comparison = candidate
    ? buildComparisonDraft(data?.baseline ?? { logic_id: null, version: null, published_at: null, publish_id: null }, candidate)
    : null;
  const lastVisited = readTradexLocal<string>(tradexStorageKeys.detailCandidateId, "");
  const directionLabel =
    comparison?.metric_deltas.expected_value_delta == null
      ? "--"
      : comparison.metric_deltas.expected_value_delta > 0
        ? "上昇"
        : comparison.metric_deltas.expected_value_delta < 0
          ? "下落"
          : "中立";
  const actionLabel =
    comparison?.metric_deltas.expected_value_delta == null
      ? "--"
      : comparison.metric_deltas.expected_value_delta >= 0
        ? "採用候補"
        : "保留候補";

  if (!candidate) {
    return (
      <div className="tradex-page tradex-detail-page">
        <section className="tradex-panel">
          <div className="tradex-panel-head">
            <div>
              <div className="tradex-panel-title">候補詳細</div>
              <div className="tradex-panel-caption">候補が見つかりませんでした。候補一覧から選び直してください。</div>
            </div>
            <Link className="tradex-secondary-action" to="/compare">
              候補比較へ
            </Link>
          </div>
          {lastVisited ? <div className="tradex-inline-note">最後に開いた候補: {lastVisited}</div> : null}
        </section>
      </div>
    );
  }

  writeTradexLocal(tradexStorageKeys.detailCandidateId, candidate.candidate_id);

  return (
    <div className="tradex-page tradex-detail-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">候補詳細</div>
            <div className="tradex-panel-caption">候補の内訳、検証結果、差分をまとめて確認できます。</div>
          </div>
          <div className="tradex-panel-actions">
            <Link className="tradex-secondary-action" to={`/compare?candidateId=${encodeURIComponent(candidate.candidate_id)}`}>
              比較へ
            </Link>
            <Link className="tradex-secondary-action" to={`/adopt?candidateId=${encodeURIComponent(candidate.candidate_id)}`}>
              反映判定へ
            </Link>
          </div>
        </div>

        <div className="tradex-detail-hero">
          <article className="tradex-detail-card">
            <div className="tradex-detail-label">候補名</div>
            <div className="tradex-detail-value">{candidate.name}</div>
            <div className="tradex-detail-sub">{candidate.candidate_id}</div>
          </article>
          <article className="tradex-detail-card">
            <div className="tradex-detail-label">現行基準</div>
            <div className="tradex-detail-value">{comparison?.baseline_publish_id ?? "--"}</div>
            <div className="tradex-detail-sub">{candidate.logic_key}</div>
          </article>
          <article className="tradex-detail-card">
            <div className="tradex-detail-label">状態</div>
            <div className="tradex-detail-value">{tradexCandidateStatusLabel(candidate.status)}</div>
            <div className="tradex-detail-sub">{tradexValidationStatusLabel(candidate.validation_state)}</div>
          </article>
        </div>

        <div className="tradex-inline-grid">
          <div>
            <span>logic_id</span>
            <strong>{candidate.logic_id ?? "--"}</strong>
          </div>
          <div>
            <span>logic_version</span>
            <strong>{candidate.logic_version ?? "--"}</strong>
          </div>
          <div>
            <span>logic_family</span>
            <strong>{candidate.logic_family ?? "--"}</strong>
          </div>
          <div>
            <span>symbol</span>
            <strong>{symbol ?? "--"}</strong>
          </div>
        </div>

        <div className="tradex-metric-row">
          <div className="tradex-metric-pill">
            <span>件数</span>
            <strong>{candidate.sample_count == null ? "--" : candidate.sample_count.toLocaleString("ja-JP")}</strong>
          </div>
          <div className="tradex-metric-pill">
            <span>期待値差分</span>
            <strong>{formatNumber(candidate.expectancy_delta, 4)}</strong>
          </div>
          <div className="tradex-metric-pill">
            <span>検証</span>
            <strong>{candidate.readiness_pass ? "採用可" : "保留"}</strong>
          </div>
          <div className="tradex-metric-pill">
            <span>スナップショット</span>
            <strong>{candidate.has_snapshot ? "あり" : "なし"}</strong>
          </div>
        </div>

        {comparison ? (
          <div className="tradex-inline-grid">
            <div>
              <span>comparison_snapshot_id</span>
              <strong>{comparison.comparison_snapshot_id}</strong>
            </div>
            <div>
              <span>baseline_publish_id</span>
              <strong>{comparison.baseline_publish_id ?? "--"}</strong>
            </div>
            <div>
              <span>方向</span>
              <strong>{directionLabel}</strong>
            </div>
            <div>
              <span>推奨</span>
              <strong>{actionLabel}</strong>
            </div>
          </div>
        ) : null}

        <details className="tradex-json-panel">
          <summary>validation_summary / manifest / ranking snapshot</summary>
          <pre>{JSON.stringify({ validation_summary: candidate.validation_summary, published_logic_manifest: candidate.published_logic_manifest, published_ranking_snapshot: candidate.published_ranking_snapshot }, null, 2)}</pre>
        </details>

        <div className="tradex-inline-note">
          {symbol ? (
            <a href={`/detail/${encodeURIComponent(symbol)}`}>MeeMee の銘柄詳細へ移動</a>
          ) : (
            "MeeMee の銘柄詳細へ移動できるコードが見つかりません。"
          )}
        </div>
      </section>
    </div>
  );
}
