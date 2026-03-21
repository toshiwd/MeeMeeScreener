import { Link } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";

const formatNumber = (value: number | null | undefined, digits = 3) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(digits);
};

export default function TradexVerifyPage() {
  const { data, loading, error } = useTradexBootstrap();
  const summary = data?.summary;
  const candidateRows = data?.candidates ?? [];
  const currentReplay = data?.raw.replay_progress as Record<string, unknown> | null;
  const actionQueue = data?.raw.action_queue as Record<string, unknown> | null;

  return (
    <div className="tradex-page tradex-verify-page">
      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">検証</div>
            <div className="tradex-panel-caption">進捗と状態を先に見て、異常があればその場で切り分けます。</div>
          </div>
          <span className="tradex-pill">{loading ? "読み込み中" : summary?.replay_status ?? "--"}</span>
        </div>
        {error ? <div className="tradex-inline-error">{error}</div> : null}
        <div className="tradex-status-grid">
          <article className="tradex-status-card">
            <div className="tradex-status-label">基準日</div>
            <div className="tradex-status-value">{summary?.as_of_date ?? "--"}</div>
          </article>
          <article className="tradex-status-card">
            <div className="tradex-status-label">鮮度</div>
            <div className="tradex-status-value">{summary?.freshness_state ?? "--"}</div>
          </article>
          <article className="tradex-status-card">
            <div className="tradex-status-label">注目件数</div>
            <div className="tradex-status-value">{summary?.attention_count?.toLocaleString("ja-JP") ?? "0"}</div>
          </article>
          <article className="tradex-status-card">
            <div className="tradex-status-label">候補件数</div>
            <div className="tradex-status-value">{summary?.candidate_count?.toLocaleString("ja-JP") ?? "0"}</div>
          </article>
        </div>
      </section>

      <section className="tradex-panel tradex-grid-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">候補一覧</div>
            <div className="tradex-panel-caption">採用候補の状態をコンパクトに並べます。</div>
          </div>
          <Link className="tradex-secondary-action" to="/compare">
            候補比較へ
          </Link>
        </div>
        <div className="tradex-table-wrap">
          <table className="tradex-table">
            <thead>
              <tr>
                <th>候補</th>
                <th>状態</th>
                <th>件数</th>
                <th>期待値差</th>
                <th>詳細</th>
              </tr>
            </thead>
            <tbody>
              {candidateRows.slice(0, 10).map((candidate) => (
                <tr key={candidate.candidate_id}>
                  <td>
                    <strong>{candidate.name}</strong>
                    <div className="tradex-table-sub">{candidate.candidate_id}</div>
                  </td>
                  <td>{candidate.status}</td>
                  <td>{candidate.sample_count == null ? "--" : candidate.sample_count.toLocaleString("ja-JP")}</td>
                  <td>{candidate.expectancy_delta == null ? "--" : formatNumber(candidate.expectancy_delta, 4)}</td>
                  <td>
                    <Link to={`/detail/${encodeURIComponent(candidate.candidate_id)}`}>開く</Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="tradex-panel">
        <div className="tradex-panel-head">
          <div>
            <div className="tradex-panel-title">実行状況</div>
            <div className="tradex-panel-caption">replay-progress と action-queue の要点だけを見せます。</div>
          </div>
        </div>
        <div className="tradex-split-grid">
          <article className="tradex-insight-card">
            <div className="tradex-insight-label">replay-progress</div>
            <div className="tradex-insight-value">
              {currentReplay ? `${String(currentReplay.status ?? "--")} / ${String(currentReplay.current_phase ?? "--")}` : "--"}
            </div>
            <div className="tradex-insight-sub">
              {currentReplay ? `${String(currentReplay.last_completed_as_of_date ?? "--")} → ${String(currentReplay.end_as_of_date ?? "--")}` : "未取得"}
            </div>
          </article>
          <article className="tradex-insight-card">
            <div className="tradex-insight-label">action-queue</div>
            <div className="tradex-insight-value">{Array.isArray(actionQueue?.actions) ? actionQueue.actions.length : 0} 件</div>
            <div className="tradex-insight-sub">注目候補に並べて確認したいアクション数です。</div>
          </article>
        </div>
      </section>
    </div>
  );
}
