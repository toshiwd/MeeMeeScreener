import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api";
import TopNav from "../components/TopNav";
import type { TradexForecastSurfaceProjection, TradexForecastSurfaceProjectionEnvelope, TradexForecastSurfaceProjectionRow } from "../tradex/contracts";
import { TRADEX_RESEARCH_ENDPOINTS, tradexResearchRoute } from "../tradex/researchRoutes";

type ForecastReview = {
  readiness_pass: boolean;
  gate_reason: string | null;
  gate_failures: string[];
  calibration_gap_long: number | null;
  calibration_gap_short: number | null;
  ready_streak: number;
  recent_ready_count_20: number;
};
type ForecastReviewEnvelope = { review: ForecastReview | null; degraded?: boolean; reason?: string | null };

export const formatForecastPercent = (value: number | null | undefined, signed = false) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const percent = value * 100;
  return `${signed && percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
};

export const forecastReadinessLabel = (review: Pick<ForecastReview, "readiness_pass"> | null) => {
  if (!review) return "評価データなし";
  return review.readiness_pass ? "判断利用可" : "表示のみ・判断未採用";
};

function ForecastCard({ row }: { row: TradexForecastSurfaceProjectionRow }) {
  const direction = row.side === "short" ? "下落" : "上昇";
  return (
    <Link className="forecast-card" to={`/detail/${encodeURIComponent(row.code)}`}>
      <div className="forecast-card-head"><strong>{row.code}</strong><span className={`forecast-side ${row.side === "short" ? "is-down" : "is-up"}`}>{direction}</span></div>
      <div className="forecast-primary">方向確率 {formatForecastPercent(row.direction_prob)}</div>
      <dl className="forecast-metrics">
        <div><dt>20日期待</dt><dd>{formatForecastPercent(row.expected_ret_20, true)}</dd></div>
        <div><dt>上方向余地</dt><dd>{formatForecastPercent(row.expected_upside)}</dd></div>
        <div><dt>下方向リスク</dt><dd>{formatForecastPercent(row.expected_downside)}</dd></div>
      </dl>
      <div className="forecast-card-foot"><span>{row.action_state === "enter" ? "注目" : row.action_state === "wait" ? "待機" : "見送り"}</span><span>無効化価格 {row.invalidation_price == null ? "--" : row.invalidation_price.toLocaleString("ja-JP")}</span></div>
    </Link>
  );
}

function ForecastSection({ title, rows }: { title: string; rows: TradexForecastSurfaceProjectionRow[] }) {
  return <section className="forecast-section"><h2>{title}</h2>{rows.length > 0 ? <div className="forecast-grid">{rows.map((row) => <ForecastCard key={`${row.side}:${row.code}:${row.action_state}`} row={row} />)}</div> : <div className="forecast-empty">該当データはありません。</div>}</section>;
}

export default function ForecastView() {
  const [projection, setProjection] = useState<TradexForecastSurfaceProjection | null>(null);
  const [review, setReview] = useState<ForecastReview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    Promise.all([
      api.get<TradexForecastSurfaceProjectionEnvelope>(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.forecastSurfaceProjection), { params: { limit_per_side: 20 } }),
      api.get<ForecastReviewEnvelope>(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.forecastSurfaceReview)),
    ]).then(([projectionResponse, reviewResponse]) => {
      if (!active) return;
      setProjection(projectionResponse.data.projection ?? null);
      setReview(reviewResponse.data.review ?? null);
      if (!projectionResponse.data.projection) setError(projectionResponse.data.reason ?? "日々の予測データがまだありません。");
    }).catch((reason: unknown) => {
      if (active) setError(reason instanceof Error ? reason.message : "日々の予測を取得できませんでした。");
    }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, []);

  const riskRows = useMemo(() => projection?.high_risk_avoid.slice(0, 12) ?? [], [projection]);
  const summary = projection?.summary ?? null;
  return (
    <div className="app-shell forecast-view">
      <header className="unified-list-header forecast-header"><div className="list-header-row"><div className="header-row-top"><div className="header-row-left"><TopNav /></div></div></div></header>
      <main className="forecast-main">
        <div className="forecast-title-row"><div><p className="forecast-kicker">ランキングとは独立</p><h1>日々の動き予測</h1><p>銘柄ごとの上昇・下落方向と想定値幅を確認する表示専用サーフェスです。</p></div><div className={`forecast-readiness ${review?.readiness_pass ? "is-ready" : "is-review"}`}><strong>{forecastReadinessLabel(review)}</strong><span>基準日 {summary?.as_of_date ?? "--"}</span></div></div>
        {loading ? <div className="forecast-status">予測データを読み込み中...</div> : null}
        {error ? <div className="forecast-status is-error">{error}</div> : null}
        {projection ? <>
          <section className="forecast-quality"><div><span>対象銘柄</span><strong>{summary?.universe_code_count?.toLocaleString("ja-JP") ?? "--"}</strong></div><div><span>データ充足率</span><strong>{formatForecastPercent(summary?.coverage_ratio)}</strong></div><div><span>直近20回合格</span><strong>{review ? `${review.recent_ready_count_20}/20` : "--"}</strong></div><div><span>上昇校正誤差</span><strong>{formatForecastPercent(review?.calibration_gap_long)}</strong></div><div><span>下落校正誤差</span><strong>{formatForecastPercent(review?.calibration_gap_short)}</strong></div></section>
          {!review?.readiness_pass ? <div className="forecast-warning">予測値は確認用です。現在のランキング、売買候補、自動判断には反映していません。{review?.gate_reason ? ` 未通過理由: ${review.gate_reason}` : ""}</div> : null}
          <ForecastSection title="上昇方向" rows={projection.long_rank} />
          <ForecastSection title="下落方向" rows={projection.short_rank} />
          <ForecastSection title="反対方向リスクが大きい銘柄" rows={riskRows} />
        </> : null}
      </main>
    </div>
  );
}
