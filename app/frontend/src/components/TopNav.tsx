import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";
import { recordPerfEvent } from "../perfDiagnostics";
import { preloadRoute } from "../routePreload";

const navItems = [
  { to: "/forecast", label: "日々予測" },
  { to: "/", label: "一覧", end: true },
  { to: "/ranking", label: "ランキング" },
  { to: "/market", label: "市場" },
  { to: "/positions", label: "建玉" },
  { to: "/favorites", label: "お気に入り" },
  { to: "/candidates", label: "候補" },
];

type TopNavProps = {
  actions?: ReactNode;
};

export default function TopNav({ actions }: TopNavProps) {
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
      {actions ? <div className="topnav-actions">{actions}</div> : null}
    </>
  );
}
