import { useEffect, useState } from "react";
import { api } from "../../api";

type TrackingSummary = {
  active_count: number;
  completed_count: number;
  archive_count: number;
  active_average_directional_return: number | null;
  completed_win_rate: number | null;
  duplicate_signal_rate: number | null;
};

type TrackingCampaign = {
  campaign_id: string;
  code: string;
  name: string | null;
  remaining_bars: number;
  favorable_return_close_basis: number | null;
};

const formatPercent = (value: number | null | undefined) => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const percent = value * 100;
  if (Math.abs(percent) < 0.05) return "0.0%";
  return `${percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
};

export default function TradexTrackingSummaryCard() {
  const [summary, setSummary] = useState<TrackingSummary | null>(null);
  const [campaigns, setCampaigns] = useState<TrackingCampaign[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      try {
        const [summaryResponse, campaignResponse] = await Promise.all([
          api.get("/signal-tracking/summary", { params: { side: "buy", logic_version: "latest" } }),
          api.get("/signal-tracking/campaigns", { params: { status: "active", side: "buy", logic_version: "latest", limit: 5 } })
        ]);
        if (canceled) return;
        setSummary((summaryResponse.data ?? null) as TrackingSummary | null);
        setCampaigns(Array.isArray(campaignResponse.data?.items) ? (campaignResponse.data.items as TrackingCampaign[]) : []);
        setError(null);
      } catch (loadError) {
        if (!canceled) {
          console.error("[tradex] tracking summary load failed", loadError);
          setError("判定追跡 summary の取得に失敗しました。");
        }
      }
    };
    void load();
    return () => {
      canceled = true;
    };
  }, []);

  return (
    <section className="tradex-panel">
      <div className="tradex-panel-head">
        <div>
          <div className="tradex-panel-title">判定追跡 summary</div>
          <div className="tradex-panel-caption">TRADEX では read-only で監視し、詳細は MeeMeeA の判定追跡へ寄せます。</div>
        </div>
        <a className="tradex-secondary-action" href="/ranking/tracking?view=signal&side=buy&logic_version=latest">
          MeeMeeAで開く
        </a>
      </div>
      {error ? <div className="tradex-inline-error">{error}</div> : null}
      <div className="tradex-inline-grid">
        <div>
          <span>Active件数</span>
          <strong>{summary?.active_count ?? "--"}</strong>
        </div>
        <div>
          <span>Completed満了勝率</span>
          <strong>{formatPercent(summary?.completed_win_rate)}</strong>
        </div>
        <div>
          <span>重複判定率</span>
          <strong>{formatPercent(summary?.duplicate_signal_rate)}</strong>
        </div>
      </div>
      <div className="tradex-subtitle">Active 上位</div>
      <div className="tradex-table-wrap">
        <table className="tradex-table">
          <thead>
            <tr>
              <th>コード</th>
              <th>銘柄名</th>
              <th>残営業日</th>
              <th>Close基準</th>
            </tr>
          </thead>
          <tbody>
            {campaigns.map((campaign) => (
              <tr key={campaign.campaign_id}>
                <td>{campaign.code}</td>
                <td>{campaign.name ?? "--"}</td>
                <td>{campaign.remaining_bars}</td>
                <td>{formatPercent(campaign.favorable_return_close_basis)}</td>
              </tr>
            ))}
            {!campaigns.length ? (
              <tr>
                <td colSpan={4}>表示対象がありません。</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
}
