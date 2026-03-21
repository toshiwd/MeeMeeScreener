import { Link } from "react-router-dom";
import { useTradexBootstrap } from "../useTradexBootstrap";
import { tradexCandidateStatusLabel, tradexFreshnessLabel, tradexReplayLabel } from "../labels";

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
            <div className="tradex-panel-caption">研究候補の進捗と異常を見て、次に比較する対象を選びます。</div>
          </div>
          <span className="tradex-pill">{loading ? "読み込み中" : tradexReplayLabel(summary?.replay_status)}</span>
        </div>
        {error ? <div className="tradex-inline-error">{error}</div> : null}
        <div className="tradex-status-grid">
          <article className="tradex-status-card">
            <div className="tradex-status-label">基準日</div>
            <div className="tradex-status-value">{summary?.as_of_date ?? "--"}</div>
          </article>
          <article className="tradex-status-card">
            <div className="tradex-status-label">鮮度</div>
            <div className="tradex-status-value">{tradexFreshnessLabel(summary?.freshness_state)}</div>
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
            <div className="tradex-panel-title">研究候補一覧</div>
            <div className="tradex-panel-caption">実行待機 / 実行中 / 完了 / 異常 の状態をまとめて確認します。</div>
          </div>
          <Link className="tradex-secondary-action" to="/compare">候補比較へ</Link>
        </div>
        <div className="tradex-table-wrap">
          <table className="tradex-table">
            <thead>
              <tr>
                <th>候補</th>
                <th>状態</th>
                <th>件数</th>
                <th>期待値差分</th>
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
                  <td>{tradexCandidateStatusLabel(candidate.status)}</td>
                  <td>{candidate.sample_count == null ? "--" : candidate.sample_count.toLocaleString("ja-JP")}</td>
                  <td>{candidate.expectancy_delta == null ? "--" : formatNumber(candidate.expectancy_delta, 4)}</td>
                  <td>
                    <Link to={`/detail/${encodeURIComponent(candidate.candidate_id)}`}>
                      候補詳細
                    </Link>
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
            <div className="tradex-panel-caption">再生進捗とアクションキューを、必要な範囲だけ確認します。</div>
          </div>
        </div>

        <div className="tradex-info-grid">
          <article className="tradex-info-card">
            <div className="tradex-info-label">現在の再生</div>
            <div className="tradex-info-value">{currentReplay ? String((currentReplay as Record<string, unknown>).status ?? "--") : "--"}</div>
            <div className="tradex-info-note">{currentReplay ? `進捗 ${String((currentReplay as Record<string, unknown>).progress_pct ?? "--")}` : "再生情報はありません"}</div>
          </article>
          <article className="tradex-info-card">
            <div className="tradex-info-label">アクションキュー</div>
            <div className="tradex-info-value">{actionQueue && Array.isArray(actionQueue.actions) ? actionQueue.actions.length.toLocaleString("ja-JP") : "0"}</div>
            <div className="tradex-info-note">採用前に確認する件数の目安です。</div>
          </article>
        </div>

        <details className="tradex-json-panel">
          <summary>再生進捗とアクションキューの詳細</summary>
          <pre>{JSON.stringify({ replay_progress: currentReplay, action_queue: actionQueue }, null, 2)}</pre>
        </details>
      </section>
    </div>
  );
}
