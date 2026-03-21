import { Link, NavLink, Outlet } from "react-router-dom";
import { useTradexBootstrap } from "./useTradexBootstrap";
import { readTradexLocal, tradexStorageKeys } from "./storage";

const navItems = [
  { to: "/verify", label: "検証" },
  { to: "/compare", label: "候補比較" },
  { to: "/adopt", label: "反映判定" }
] as const;

function SummaryCard({ label, value, tone = "neutral" }: { label: string; value: string; tone?: "neutral" | "ok" | "warn" }) {
  return (
    <article className={`tradex-summary-card ${tone === "ok" ? "is-ok" : tone === "warn" ? "is-warn" : ""}`}>
      <div className="tradex-summary-card-label">{label}</div>
      <div className="tradex-summary-card-value">{value}</div>
    </article>
  );
}

function prettyValue(value: string | null | undefined, fallback = "--") {
  const text = typeof value === "string" ? value.trim() : "";
  return text || fallback;
}

export default function TradexShell() {
  const { loading, error, data } = useTradexBootstrap();
  const summary = data?.summary;
  const candidates = data?.candidates ?? [];
  const detailCandidateId = readTradexLocal<string>(tradexStorageKeys.detailCandidateId, "");
  const fallbackDetailCandidateId = candidates[0]?.candidate_id ?? "";
  const detailTarget = detailCandidateId || fallbackDetailCandidateId;
  const detailHref = detailTarget ? `/detail/${encodeURIComponent(detailTarget)}` : null;

  return (
    <div className="tradex-shell">
      <header className="tradex-shell-header">
        <div className="tradex-brand-row">
          <Link to="/" className="tradex-brand-link">
            <div className="tradex-brand-title">TRADEX</div>
            <div className="tradex-brand-subtitle">研究・候補比較・反映判定のための内部コンソール</div>
          </Link>
          <div className="tradex-brand-hint">比較を先に見てから、反映の判断へ進む。</div>
        </div>

        <div className="tradex-summary-strip" aria-label="TRADEX summary">
          <SummaryCard label="基準日" value={summary?.as_of_date ?? (loading ? "取得中" : "--")} />
          <SummaryCard label="鮮度" value={prettyValue(summary?.freshness_state, loading ? "取得中" : "--")} tone={summary?.freshness_state ? "ok" : "neutral"} />
          <SummaryCard label="実行状況" value={prettyValue(summary?.replay_status, loading ? "取得中" : "--")} tone={summary?.replay_status?.includes("異常") ? "warn" : "ok"} />
          <SummaryCard label="注目件数" value={typeof summary?.attention_count === "number" ? summary.attention_count.toLocaleString("ja-JP") : "0"} />
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
          <summary>移行中の互換画面</summary>
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
