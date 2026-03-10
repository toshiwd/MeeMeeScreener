import TopNav from "../components/TopNav";
import SectorHeatmap from "../features/market/SectorHeatmap";

export default function MarketView() {
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
              <span className="header-brand">業種別ヒートマップ</span>
            </div>
            <span className="updates-label">投資効率 (騰落率) / 熱量 (売買代金)</span>
          </div>
        </div>
      </div>
      <main className="market-main">
        <SectorHeatmap />
      </main>
    </div>
  );
}
