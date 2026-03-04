import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import TopNav from "../components/TopNav";

type ToredexSimulationSummaryPoint = {
  season_id: string | null;
  net_cum_return_pct: number | null;
  final_jpy: number | null;
  gain_jpy: number | null;
};

type ToredexSimulationSummary = {
  count: number;
  avg: ToredexSimulationSummaryPoint;
  median: ToredexSimulationSummaryPoint;
  best: ToredexSimulationSummaryPoint;
  worst: ToredexSimulationSummaryPoint;
};

type ToredexSimulationItem = {
  season_id: string;
  start_date: string | null;
  end_date: string | null;
  metric_days: number;
  trades: number;
  net_cum_return_pct: number;
  max_drawdown_pct: number | null;
  final_jpy: number;
  gain_jpy: number;
};

type ToredexSimulationResponse = {
  principal_jpy: number;
  filters: Record<string, unknown>;
  summary: ToredexSimulationSummary;
  items: ToredexSimulationItem[];
};

const formatJpy = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", { style: "currency", currency: "JPY", maximumFractionDigits: 0 });
};

const formatSignedJpy = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatJpy(value)}`;
};

const formatPct = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
};

export default function ToredexSimulationView() {
  const { ready } = useBackendReadyState();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ToredexSimulationResponse | null>(null);

  useEffect(() => {
    if (!ready) return;
    let active = true;
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await api.get<ToredexSimulationResponse>("/toredex/simulation/validate", {
          params: { limit: 30 }
        });
        if (!active) return;
        setData(res.data);
      } catch (err) {
        if (!active) return;
        const message = err instanceof Error ? err.message : "シミュレーション取得に失敗しました";
        setError(message);
      } finally {
        if (active) setLoading(false);
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [ready]);

  const cards = useMemo(() => {
    if (!data) return [];
    return [
      { key: "best", label: "最良", point: data.summary.best },
      { key: "median", label: "中央値", point: data.summary.median },
      { key: "avg", label: "平均", point: data.summary.avg },
      { key: "worst", label: "最悪", point: data.summary.worst }
    ];
  }, [data]);

  return (
    <div className="app-shell toredex-sim-view">
      <div className="dynamic-header">
        <div className="dynamic-header-row header-row-top">
          <TopNav />
        </div>
        <div className="dynamic-header-row header-row-bottom">
          <div className="header-title-group">
            <div className="header-nav-title">
              <span className="header-brand">TOREDEX 資産シミュ</span>
            </div>
            <span className="updates-label">元手 1,000万円固定 / validate + pass</span>
          </div>
        </div>
      </div>

      <main className="toredex-sim-main">
        <section className="toredex-sim-panel">
          {!ready && <div className="toredex-sim-status">バックエンド準備中...</div>}
          {ready && loading && <div className="toredex-sim-status">読み込み中...</div>}
          {error && <div className="toredex-sim-error">{error}</div>}

          {data && (
            <>
              <div className="toredex-sim-meta">
                対象件数: {data.summary.count.toLocaleString("ja-JP")} / 元手: {formatJpy(data.principal_jpy)}
              </div>

              <div className="toredex-sim-summary-grid">
                {cards.map((card) => (
                  <article key={card.key} className="toredex-sim-card">
                    <div className="toredex-sim-card-label">{card.label}</div>
                    <div className="toredex-sim-card-value">{formatJpy(card.point.final_jpy)}</div>
                    <div className="toredex-sim-card-sub">
                      <span>{formatPct(card.point.net_cum_return_pct)}</span>
                      <span>{formatSignedJpy(card.point.gain_jpy)}</span>
                    </div>
                    {card.point.season_id && <div className="toredex-sim-card-note">{card.point.season_id}</div>}
                  </article>
                ))}
              </div>

              <div className="toredex-sim-table-wrap">
                <table className="toredex-sim-table">
                  <thead>
                    <tr>
                      <th>season_id</th>
                      <th>期間</th>
                      <th>日数</th>
                      <th>取引数</th>
                      <th>収益率</th>
                      <th>最終資産</th>
                      <th>損益</th>
                      <th>最大DD</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.items.map((item) => (
                      <tr key={item.season_id}>
                        <td>{item.season_id}</td>
                        <td>
                          {(item.start_date ?? "--")} - {(item.end_date ?? "--")}
                        </td>
                        <td>{item.metric_days.toLocaleString("ja-JP")}</td>
                        <td>{item.trades.toLocaleString("ja-JP")}</td>
                        <td>{formatPct(item.net_cum_return_pct)}</td>
                        <td>{formatJpy(item.final_jpy)}</td>
                        <td>{formatSignedJpy(item.gain_jpy)}</td>
                        <td>{formatPct(item.max_drawdown_pct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </section>
      </main>
    </div>
  );
}
