import { NavLink } from "react-router-dom";
export default function TopNav() {
  return (
    <nav className="list-tabs">
      <NavLink
        to="/"
        end
        className={({ isActive }) =>
          isActive ? "list-tab list-home active" : "list-tab list-home"
        }
      >
        一覧に戻る
      </NavLink>
      <NavLink
        to="/ranking"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        ランキング
      </NavLink>
      <NavLink
        to="/market"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        市場概況
      </NavLink>
      <NavLink
        to="/toredex-sim"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        資産シミュ
      </NavLink>
      <NavLink
        to="/favorites"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        お気に入り
      </NavLink>
      <NavLink
        to="/candidates"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        候補
      </NavLink>
      <NavLink
        to="/positions"
        className={({ isActive }) => (isActive ? "list-tab active" : "list-tab")}
      >
        保有
      </NavLink>
    </nav>
  );
}
