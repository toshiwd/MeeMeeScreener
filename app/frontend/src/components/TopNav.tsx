import { NavLink } from "react-router-dom";

const navItems = [
  { to: "/", label: "一覧", end: true },
  { to: "/ranking", label: "ランキング" },
  { to: "/market", label: "市場" },
  { to: "/positions", label: "建玉" },
  { to: "/favorites", label: "お気に入り" },
  { to: "/candidates", label: "候補" }
];

export default function TopNav() {
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
    </>
  );
}
