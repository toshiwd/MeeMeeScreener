import { Link, NavLink, Outlet } from "react-router-dom";
import { useTradexBootstrap } from "./useTradexBootstrap";
import { readTradexLocal, tradexStorageKeys } from "./storage";
import { tradexFreshnessLabel, tradexReplayLabel } from "./labels";

const navItems = [
  { to: "/verify", label: "検証" },
  { to: "/compare", label: "候補比較" },
  { to: "/adopt", label: "反映判定" }
] as const;

function SummaryMetric({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "ok" | "warn" }) {
  return (
    <div className={`tradex-summary-metric ${tone === "ok" ? "is-ok" : tone === "warn" ? "is-warn" : ""}`}>
      <span className="tradex-summary-metric-label">{label}</span>
      <strong className="tradex-summary-metric-value">{value}</strong>
    </div>
  );
}

export default function TradexShell() {
  const { loading, error, data } = useTradexBootstrap();
  const summary = data?.summary;
  const runId = readTradexLocal<string>(tradexStorageKeys.runId, "");
  const detailCode = readTradexLocal<string>(tradexStorageKeys.detailCode, "");
  const detailHref = runId ? `/detail/${encodeURIComponent(runId)}${detailCode ? `?code=${encodeURIComponent(detailCode)}` : ""}` : null;

  return (
    <div className="tradex-shell">
      <header className="tradex-shell-header">
        <div className="tradex-brand-row">
          <Link to="/" className="tradex-brand-link">
            <div className="tradex-brand-title">TRADEX</div>
            <div className="tradex-brand-subtitle">研究候補を比較して、反映可否を判断する専用画面</div>
          </Link>
          <div className="tradex-brand-hint">MeeMee と分離した TRADEX の入口です</div>
        </div>

        <div className="tradex-summary-strip" aria-label="TRADEX summary">
          <SummaryMetric label="基準日" value={summary?.as_of_date ?? (loading ? "読み込み中" : "--")} />
          <SummaryMetric label="鮮度" value={tradexFreshnessLabel(summary?.freshness_state)} tone={summary?.freshness_state ? "ok" : "neutral"} />
          <SummaryMetric label="実行状況" value={tradexReplayLabel(summary?.replay_status)} tone={summary?.replay_status?.includes("error") ? "warn" : "ok"} />
          <SummaryMetric label="注目件数" value={typeof summary?.attention_count === "number" ? summary.attention_count.toLocaleString("ja-JP") : "0"} />
        </div>

        <nav className="tradex-primary-nav" aria-label="TRADEX primary navigation">
          {navItems.map((item) => (
            <NavLink key={item.to} to={item.to} className={({ isActive }) => `tradex-nav-item${isActive ? " active" : ""}`}>
              {item.label}
            </NavLink>
          ))}
          {detailHref ? (
            <NavLink to={detailHref} className={({ isActive }) => `tradex-nav-item${isActive ? " active" : ""}`}>
              候補詳細
            </NavLink>
          ) : (
            <span className="tradex-nav-item is-disabled">候補詳細</span>
          )}
        </nav>

        <details className="tradex-legacy-links">
          <summary>移行中の旧画面</summary>
          <div className="tradex-legacy-link-row">
            <NavLink to="/legacy/tags">検証（旧）</NavLink>
            <NavLink to="/legacy/publish">反映（旧）</NavLink>
            <NavLink to="/legacy/sim">検証シミュレーション（旧）</NavLink>
          </div>
        </details>

        {error ? <div className="tradex-shell-alert is-error">{error}</div> : null}
      </header>

      <main className="tradex-shell-main">
        <Outlet />
      </main>
    </div>
  );
}
