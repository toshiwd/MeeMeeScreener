import { useState } from "react";
import TopNav from "../components/TopNav";
import SectorHeatmap from "../features/market/SectorHeatmap";
import SectorBubbleChart from "../features/market/SectorBubbleChart";

export default function MarketView() {
  const [tab, setTab] = useState<"heatmap" | "bubble">("heatmap");

  return (
    <div className="app-shell market-view">
      <div className="dynamic-header">
        <div className="dynamic-header-row header-row-top">
          <div className="header-row-left">
            <TopNav />
          </div>
        </div>
        <div className="dynamic-header-row header-row-bottom">
          <div className="header-title-group">
            <div className="header-nav-title">
              <span className="header-brand">業種別ヒートマップ / バブルチャート</span>
            </div>
            <span className="updates-label">投資効率 (騰落率) / 熱量 (売買代金)</span>
          </div>
          <div className="view-mode-tabs" style={{ display: "flex", gap: "8px" }}>
            <button
              onClick={() => setTab("heatmap")}
              style={{
                background: tab === "heatmap" ? "var(--theme-accent)" : "transparent",
                color: tab === "heatmap" ? "#fff" : "var(--theme-text-secondary)",
                border: "1px solid",
                borderColor: tab === "heatmap" ? "var(--theme-accent)" : "var(--theme-border)",
                padding: "6px 12px",
                borderRadius: "999px",
                fontSize: "12px",
                cursor: "pointer",
                fontWeight: 600
              }}
            >
              ヒートマップ
            </button>
            <button
              onClick={() => setTab("bubble")}
              style={{
                background: tab === "bubble" ? "var(--theme-accent)" : "transparent",
                color: tab === "bubble" ? "#fff" : "var(--theme-text-secondary)",
                border: "1px solid",
                borderColor: tab === "bubble" ? "var(--theme-accent)" : "var(--theme-border)",
                padding: "6px 12px",
                borderRadius: "999px",
                fontSize: "12px",
                cursor: "pointer",
                fontWeight: 600
              }}
            >
              バブルチャート
            </button>
          </div>
        </div>
      </div>
      <main className="market-main">
        {tab === "heatmap" ? <SectorHeatmap /> : <SectorBubbleChart />}
      </main>
    </div>
  );
}
