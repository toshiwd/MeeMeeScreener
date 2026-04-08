import { NavLink } from "react-router-dom";
import {
  clearPerfDiagnostics,
  exportPerfDiagnostics,
  isPerfDiagnosticsEnabled,
  openPerfDiagnosticsDir,
  recordPerfEvent,
} from "../perfDiagnostics";
import { preloadRoute } from "../routePreload";

const navItems = [
  { to: "/", label: "一覧", end: true },
  { to: "/ranking", label: "ランキング" },
  { to: "/market", label: "市場" },
  { to: "/positions", label: "建玉" },
  { to: "/favorites", label: "お気に入り" },
  { to: "/candidates", label: "候補" },
];

export default function TopNav() {
  const diagnosticsEnabled = isPerfDiagnosticsEnabled();

  return (
    <>
      <div className="app-brand">
        <div className="app-brand-title">MeeMee</div>
        <div className="app-brand-sub">Screener</div>
      </div>
      <nav className="list-tabs">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            onMouseEnter={() => {
              void preloadRoute(item.to);
            }}
            onFocus={() => {
              void preloadRoute(item.to);
            }}
            onPointerDown={() => {
              void preloadRoute(item.to);
              recordPerfEvent("topnav_pointerdown", {
                route: item.to,
                label: item.label,
              });
            }}
            onClick={() => {
              recordPerfEvent("topnav_click", {
                route: item.to,
                label: item.label,
              });
            }}
            className={({ isActive }) =>
              isActive
                ? `list-tab${item.end ? " list-home" : ""} active`
                : `list-tab${item.end ? " list-home" : ""}`
            }
          >
            {item.label}
          </NavLink>
        ))}
      </nav>
      {diagnosticsEnabled ? (
        <div className="perf-diagnostics-tools">
          <button
            type="button"
            className="perf-diagnostics-button"
            onClick={() => {
              window.localStorage.removeItem("meemeePerfDiagnosticsEnabled");
              window.location.reload();
            }}
          >
            診断OFF
          </button>
          <button
            type="button"
            className="perf-diagnostics-button"
            onClick={() => {
              void exportPerfDiagnostics();
            }}
          >
            診断保存
          </button>
          <button
            type="button"
            className="perf-diagnostics-button"
            onClick={() => {
              void openPerfDiagnosticsDir();
            }}
          >
            保存先
          </button>
          <button
            type="button"
            className="perf-diagnostics-button"
            onClick={() => {
              void clearPerfDiagnostics();
            }}
          >
            消去
          </button>
        </div>
      ) : (
        <div className="perf-diagnostics-tools">
          <button
            type="button"
            className="perf-diagnostics-button"
            onClick={() => {
              window.localStorage.setItem("meemeePerfDiagnosticsEnabled", "1");
              window.location.reload();
            }}
          >
            診断ON
          </button>
        </div>
      )}
    </>
  );
}
