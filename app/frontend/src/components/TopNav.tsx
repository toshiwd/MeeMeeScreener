import { NavLink } from "react-router-dom";

export default function TopNav() {
  return (
    <nav className="top-nav">
      <NavLink
        to="/"
        end
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        スクリーナー
      </NavLink>
      <NavLink
        to="/ranking"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        ランキング
      </NavLink>
      <NavLink
        to="/favorites"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        お気に入り
      </NavLink>
      <NavLink
        to="/candidates"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        候補
      </NavLink>
      <NavLink
        to="/positions"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        保有
      </NavLink>
    </nav>
  );
}
